//! The lease cache: one table name to one vended lease, with a refresh
//! deadline.
//!
//! The daemon owns the cross-process copy; a reader owns a local copy so a
//! `bytemass` over one table does not ask the socket once per file. Both use
//! the same [`Cached`] rules: refresh five minutes before expiry, or half the
//! remaining lifetime for a lease shorter than ten minutes; a catalog that
//! vends nothing is remembered briefly so the negative answer is not asked
//! again for every file.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex as AsyncMutex;

use pqbench::v1::Credentials;
use pqbench::{Dialect, Workspace};

use crate::CliError;

use super::client;
use super::path;
use super::protocol::{Request, Response};
use super::rate::UnityRate;

/// How long before expiry a lease is refreshed.
const REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);

/// When the catalog reports no expiry, assume the usual STS lifetime.
const ASSUMED_LIFETIME: Duration = Duration::from_secs(60 * 60);

/// How long a "the catalog vends nothing" answer is remembered.
const AMBIENT_TTL: Duration = Duration::from_secs(5 * 60);

/// How long a failed vending call is remembered.
const FAILED_TTL: Duration = Duration::from_secs(30);

/// The synthetic `local` catalog is a path, not a catalog: it never vends.
fn local(table: &str) -> bool {
    table.starts_with("catalogs/local/")
}

/// A lease as the daemon reports it.
#[derive(Debug, Clone)]
pub(crate) struct Vended {
    pub env: BTreeMap<String, String>,
    /// When the lease expires, milliseconds since the Unix epoch.
    pub expire_time_ms: Option<u64>,
}

/// A cache in one process: the daemon's, or a reader's.
#[derive(Clone)]
pub(crate) struct LeaseCache {
    vendor: Vendor,
    entries: Arc<Mutex<BTreeMap<String, Cached>>>,
    /// One async lock per table, so a burst of files from the same table
    /// makes one vending call, not one per in-flight file.
    inflight: Arc<Mutex<BTreeMap<String, Arc<AsyncMutex<()>>>>>,
    /// Whether a failed vend is remembered. The daemon does not remember one:
    /// a walk retry must reach the catalog again. A reader does, so a table's
    /// files do not each retry the same failure.
    cache_failures: bool,
    /// The daemon's Unity pacer; readers do not pace their own vends.
    rate: Option<Arc<UnityRate>>,
}

#[derive(Clone)]
enum Vendor {
    /// Vend directly from the catalog.
    Direct(Workspace),
    /// Ask the daemon; fall back to the catalog if it cannot be reached.
    Daemon { path: PathBuf, fallback: Workspace },
}

#[derive(Clone)]
enum Cached {
    Vended {
        env: BTreeMap<String, String>,
        refresh_time: Instant,
        expire_time: Instant,
    },
    Ambient {
        expire_time: Instant,
    },
    Failed {
        message: String,
        expire_time: Instant,
    },
}

impl LeaseCache {
    /// The daemon's cache: always vends directly, and paces Unity vends at
    /// the catalog's per-endpoint ceiling.
    pub(crate) fn direct(workspace: Workspace) -> Self {
        let rate =
            (workspace.config().dialect == Dialect::Unity).then(|| Arc::new(UnityRate::new()));
        Self::new(Vendor::Direct(workspace), false, rate)
    }

    /// A reader's cache: use the daemon when its socket exists, else the
    /// catalog.
    pub(crate) fn client(workspace: Workspace) -> Self {
        // An explicit socket can serve a reader that knows no endpoint; only
        // an unconfigured reader has nothing to ask.
        if workspace.config().endpoint.is_empty() && std::env::var_os("PQB_DAEMON_SOCK").is_none() {
            return Self::new(Vendor::Direct(workspace), true, None);
        }
        let socket = path::socket_path(workspace.config());
        let vendor = if socket.exists() {
            Vendor::Daemon {
                path: socket,
                fallback: workspace,
            }
        } else {
            Vendor::Direct(workspace)
        };
        Self::new(vendor, true, None)
    }

    fn new(vendor: Vendor, cache_failures: bool, rate: Option<Arc<UnityRate>>) -> Self {
        Self {
            vendor,
            entries: Arc::new(Mutex::new(BTreeMap::new())),
            inflight: Arc::new(Mutex::new(BTreeMap::new())),
            cache_failures,
            rate,
        }
    }

    /// The `AWS_*` options to read `table` with; empty means ambient.
    pub(crate) async fn resolve_env(
        &self,
        table: &str,
    ) -> Result<BTreeMap<String, String>, CliError> {
        Ok(self
            .resolve(table)
            .await?
            .map(|vended| vended.env)
            .unwrap_or_default())
    }

    /// The daemon's answer for `table`: the lease, or `None` for ambient.
    pub(crate) async fn resolve_vended(&self, table: &str) -> Result<Option<Vended>, CliError> {
        self.resolve(table).await
    }

    async fn resolve(&self, table: &str) -> Result<Option<Vended>, CliError> {
        if local(table) {
            return Ok(None);
        }
        if let Some(cached) = self.lookup(table) {
            return cached.result();
        }
        // Serialize vends per table: a `list columns` over one table can have
        // many files in flight, and they all need the same lease.
        let lock = self.lock(table);
        let _guard = lock.lock().await;
        if let Some(cached) = self.lookup(table) {
            return cached.result();
        }
        if let Some(rate) = &self.rate {
            rate.wait().await;
        }
        match self.vendor.vend(table).await {
            Ok(vended) => {
                let cached = Cached::new(vended, Instant::now());
                self.put(table, cached.clone());
                cached.result()
            }
            Err(error) => {
                if self.cache_failures {
                    self.put(table, Cached::failed(&error, Instant::now()));
                }
                Err(error)
            }
        }
    }

    /// The per-table vend lock. The entry stays; one small mutex per table
    /// walked is cheaper than removing it under racing waiters.
    fn lock(&self, table: &str) -> Arc<AsyncMutex<()>> {
        self.inflight
            .lock()
            .expect("the lease in-flight lock is not poisoned")
            .entry(table.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    fn lookup(&self, table: &str) -> Option<Cached> {
        let now = Instant::now();
        self.entries
            .lock()
            .expect("the lease cache lock is not poisoned")
            .get(table)
            .filter(|cached| cached.fresh(now))
            .cloned()
    }

    fn put(&self, table: &str, cached: Cached) {
        self.entries
            .lock()
            .expect("the lease cache lock is not poisoned")
            .insert(table.to_string(), cached);
    }
}

impl Vendor {
    async fn vend(&self, table: &str) -> Result<Option<Vended>, CliError> {
        match self {
            Vendor::Direct(workspace) => direct(workspace, table).await,
            Vendor::Daemon { path, fallback } => {
                match client::request(path, &Request::table(table)).await {
                    Ok(response) => parse_response(response),
                    Err(client::Error::Connect(_) | client::Error::Protocol(_)) => {
                        direct(fallback, table).await
                    }
                }
            }
        }
    }
}

async fn direct(workspace: &Workspace, table: &str) -> Result<Option<Vended>, CliError> {
    if workspace.config().endpoint.is_empty() {
        return Ok(None);
    }
    match pqbench::credentials::get::get(workspace, table).await {
        Ok(Some(credentials)) => Ok(Some(vended(credentials))),
        Ok(None) => Ok(None),
        // A principal without the vending privilege still may read under its
        // ambient credentials; let the storage read decide. A transport or
        // server failure stays an error and is cached briefly below.
        Err(pqbench::credentials::get::Error::Http(pqbench::http::Error::Status {
            status: 400 | 403 | 404 | 501,
            ..
        })) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn parse_response(response: Response) -> Result<Option<Vended>, CliError> {
    if let Some(error) = response.error {
        return Err(error.into());
    }
    if response.none || response.env.is_none() {
        return Ok(None);
    }
    Ok(Some(Vended {
        env: response.env.unwrap_or_default(),
        expire_time_ms: response.expire_time_ms,
    }))
}

fn vended(credentials: Credentials) -> Vended {
    Vended {
        env: credentials.to_env(),
        expire_time_ms: credentials.expire_time,
    }
}

impl Cached {
    fn failed(error: &CliError, now: Instant) -> Self {
        Cached::Failed {
            message: error.to_string(),
            expire_time: now + FAILED_TTL,
        }
    }

    fn new(vended: Option<Vended>, now: Instant) -> Self {
        let Some(vended) = vended else {
            return Cached::Ambient {
                expire_time: now + AMBIENT_TTL,
            };
        };
        let lifetime = match vended.expire_time_ms {
            Some(expire_time_ms) => remaining(expire_time_ms).unwrap_or(Duration::ZERO),
            None => ASSUMED_LIFETIME,
        };
        let expire_time = now + lifetime;
        let margin = REFRESH_MARGIN.min(lifetime / 2);
        Cached::Vended {
            env: vended.env,
            refresh_time: expire_time.checked_sub(margin).unwrap_or(now),
            expire_time,
        }
    }

    fn fresh(&self, now: Instant) -> bool {
        match self {
            Cached::Vended { refresh_time, .. } => now < *refresh_time,
            Cached::Ambient { expire_time } | Cached::Failed { expire_time, .. } => {
                now < *expire_time
            }
        }
    }

    fn result(&self) -> Result<Option<Vended>, CliError> {
        match self {
            Cached::Vended {
                env, expire_time, ..
            } => Ok(Some(Vended {
                env: env.clone(),
                expire_time_ms: Some(epoch_ms(*expire_time, Instant::now())),
            })),
            Cached::Ambient { .. } => Ok(None),
            Cached::Failed { message, .. } => Err(message.clone().into()),
        }
    }
}

/// How long until an epoch-millisecond instant; `None` when it is already past.
fn remaining(epoch_ms: u64) -> Option<Duration> {
    let expiry = UNIX_EPOCH + Duration::from_millis(epoch_ms);
    expiry.duration_since(SystemTime::now()).ok()
}

/// An `Instant` as epoch milliseconds.
fn epoch_ms(instant: Instant, now: Instant) -> u64 {
    let remaining = instant.saturating_duration_since(now);
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64);
    now_ms + remaining.as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease(expires_in: Duration) -> Vended {
        Vended {
            env: BTreeMap::from([("AWS_ACCESS_KEY_ID".to_string(), "key".to_string())]),
            expire_time_ms: Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    + expires_in.as_millis() as u64,
            ),
        }
    }

    #[test]
    fn a_hour_lease_refreshes_five_minutes_before_expiry() {
        let now = Instant::now();
        let cached = Cached::new(Some(lease(Duration::from_secs(3600))), now);
        assert!(cached.fresh(now + Duration::from_secs(54 * 60)));
        assert!(!cached.fresh(now + Duration::from_secs(56 * 60)));
    }

    #[test]
    fn a_short_lease_refreshes_at_half_life() {
        let now = Instant::now();
        let cached = Cached::new(Some(lease(Duration::from_secs(480))), now);
        assert!(cached.fresh(now + Duration::from_secs(3 * 60)));
        assert!(!cached.fresh(now + Duration::from_secs(5 * 60)));
    }

    #[test]
    fn no_expiry_uses_the_conservative_hour() {
        let now = Instant::now();
        let cached = Cached::new(
            Some(Vended {
                env: BTreeMap::new(),
                expire_time_ms: None,
            }),
            now,
        );
        assert!(cached.fresh(now + Duration::from_secs(54 * 60)));
        assert!(!cached.fresh(now + Duration::from_secs(56 * 60)));
    }

    #[test]
    fn an_expired_lease_is_not_fresh() {
        let now = Instant::now();
        let cached = Cached::new(Some(lease(Duration::ZERO)), now);
        assert!(!cached.fresh(now + Duration::from_millis(1)));
    }

    #[test]
    fn a_failed_vend_is_remembered_briefly() {
        let now = Instant::now();
        let error: CliError = "boom".into();
        let cached = Cached::failed(&error, now);
        assert!(cached.fresh(now + Duration::from_secs(1)));
        assert!(!cached.fresh(now + FAILED_TTL + Duration::from_secs(1)));
    }

    #[test]
    fn a_response_without_an_env_is_ambient() {
        assert!(parse_response(Response::default())
            .expect("an empty response is not a failure")
            .is_none());
    }

    #[test]
    fn the_local_catalog_is_never_vended() {
        assert!(local("catalogs/local/schemas/local/tables/t"));
        assert!(!local("catalogs/pqbench_ext/schemas/s/tables/t"));
    }

    #[test]
    fn an_ambient_answer_is_remembered() {
        let now = Instant::now();
        let cached = Cached::new(None, now);
        assert!(cached.fresh(now + Duration::from_secs(4 * 60)));
        assert!(!cached.fresh(now + Duration::from_secs(6 * 60)));
        assert!(cached.result().expect("ambient is not a failure").is_none());
    }
}
