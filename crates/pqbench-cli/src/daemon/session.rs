//! A daemon session: the lease cache, and the catalog transport the daemon
//! owns.
//!
//! The daemon owns the cross-process lease copy; a reader owns a local copy so
//! a `bytemass` over one table does not ask the socket once per file. Both use
//! the same [`Cached`] rules: refresh five minutes before expiry, or half the
//! remaining lifetime for a lease shorter than ten minutes; a catalog that
//! vends nothing is remembered briefly so the negative answer is not asked
//! again for every file.
//!
//! The daemon also owns the catalog transport: [`Daemon::http_workspace`]
//! attaches it to the reader's workspace as the HTTP GET hook, so every catalog
//! command runs the library's own URLs, pages, and JSON decoding while the
//! daemon sends the request under its identity and paces it at the catalog's
//! ceiling. A reader with no daemon reads its endpoint directly. A reader with
//! no endpoint (local files) has no catalog to ask and never needs one.

use std::collections::BTreeMap;
use std::fmt;
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

/// A daemon session in one process: the daemon's, or a reader's.
#[derive(Clone)]
pub(crate) struct Daemon {
    vendor: Vendor,
    entries: Arc<Mutex<BTreeMap<String, Cached>>>,
    /// One async lock per table, so a burst of files from the same table
    /// makes one vending call, not one per in-flight file.
    inflight: Arc<Mutex<BTreeMap<String, Arc<AsyncMutex<()>>>>>,
    /// Whether a failed vend is remembered. The daemon does not remember one:
    /// a walk retry must reach the catalog again. A reader does, so a table's
    /// files do not each retry the same failure.
    cache_failures: bool,
    /// The daemon's Unity pacer; readers do not pace their own requests.
    rate: Option<Arc<UnityRate>>,
}

#[derive(Clone)]
enum Vendor {
    /// Vend directly from the catalog: the daemon itself, or a reader with
    /// no endpoint, which has no catalog to call.
    Direct(Workspace),
    /// Ask the daemon at `path`.
    Daemon { path: PathBuf, workspace: Workspace },
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

impl Daemon {
    /// The daemon's session: always vends directly, and paces Unity requests
    /// at the catalog's per-endpoint ceiling.
    pub(crate) fn direct(workspace: Workspace) -> Self {
        let rate =
            (workspace.config().dialect == Dialect::Unity).then(|| Arc::new(UnityRate::new()));
        Self::new(Vendor::Direct(workspace), false, rate)
    }

    /// A reader's session: every lease goes to the daemon, and so does every
    /// catalog request ([`Daemon::http_workspace`]).
    pub(crate) fn client(workspace: Workspace) -> Self {
        // An explicit socket can serve a reader that knows no endpoint; only
        // an unconfigured reader has nothing to ask.
        if workspace.config().endpoint.is_empty() && std::env::var_os("PQB_DAEMON_SOCK").is_none() {
            return Self::new(Vendor::Direct(workspace), true, None);
        }
        let path = path::socket_path(workspace.config());
        Self::new(Vendor::Daemon { path, workspace }, true, None)
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

    /// The workspace this session speaks for.
    pub(crate) fn workspace(&self) -> &Workspace {
        match &self.vendor {
            Vendor::Direct(workspace) => workspace,
            Vendor::Daemon { workspace, .. } => workspace,
        }
    }

    /// This session's workspace with the daemon as its HTTP GET: every catalog
    /// request a command makes is sent by the daemon at `path`, which paces it
    /// at the catalog's ceiling and uses its own bearer. A session with no
    /// daemon reads its own endpoint directly.
    pub(crate) fn http_workspace(&self) -> Workspace {
        let workspace = self.workspace().clone();
        let path = match &self.vendor {
            Vendor::Direct(_) => return workspace,
            Vendor::Daemon { path, .. } => path.clone(),
        };
        workspace.with_http_get(move |url: String| {
            let path = path.clone();
            async move { daemon_get(&path, &url).await }
        })
    }

    /// Wait for the daemon's Unity slot; an Iceberg session and a reader have
    /// none.
    pub(crate) async fn pace(&self) {
        if let Some(rate) = &self.rate {
            rate.wait().await;
        }
    }

    /// The `AWS_*` options to read `table` with; empty means ambient.
    pub(crate) async fn resolve_env(
        &self,
        table: &str,
    ) -> Result<BTreeMap<String, String>, CliError> {
        Ok(self
            .resolve_vended(table)
            .await?
            .map(|vended| vended.env)
            .unwrap_or_default())
    }

    /// The lease to read `table` with; `None` means the ambient `AWS_*`.
    pub(crate) async fn resolve_vended(&self, table: &str) -> Result<Option<Vended>, CliError> {
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
        self.pace().await;
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
            Vendor::Daemon { path, .. } => parse_response(ask(path, &Request::lease(table)).await?),
        }
    }
}

/// One request to the daemon at `path`; no daemon is an error that says how
/// to start one.
async fn ask(path: &std::path::Path, request: &Request) -> Result<Response, CliError> {
    client::request(path, request)
        .await
        .map_err(|error| connect_error(path, error))
}

fn connect_error(path: &std::path::Path, error: client::Error) -> CliError {
    match error {
        client::Error::Connect(_) => format!(
            "no daemon is listening at {}; run `pqbench daemon start`",
            path.display()
        )
        .into(),
        client::Error::Protocol(_) => error.to_string().into(),
    }
}

/// One GET sent by the daemon at `path`: its answer is the endpoint's, status
/// and body unchanged.
async fn daemon_get(
    path: &std::path::Path,
    url: &str,
) -> Result<pqbench::third_party::reqwest::Response, pqbench::third_party::reqwest::Error> {
    let answer = client::request(path, &Request::http_get(url))
        .await
        .map_err(|error| transport_error(connect_error(path, error).to_string()))?;
    let answer = match answer.error {
        Some(message) => return Err(transport_error(message)),
        None => answer,
    };
    let Some(status) = answer.status else {
        return Err(transport_error(
            "the daemon answered a catalog GET without a status".into(),
        ));
    };
    Ok(pqbench::third_party::reqwest::Response {
        status,
        headers: Vec::new(),
        bytes: answer.body.unwrap_or_default().into_bytes(),
    })
}

/// A transport error whose message already carries its cause: the daemon's
/// answer is one line, so the chain is the message.
fn transport_error(message: String) -> pqbench::third_party::reqwest::Error {
    pqbench::third_party::reqwest::Error::Transport(Box::new(Message(message)))
}

/// The message a proxied GET reports; it names the daemon and its answer.
#[derive(Debug)]
struct Message(String);

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Message {}

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
    let response = answer(response)?;
    if response.none {
        return Ok(None);
    }
    let Some(env) = response.env else {
        return Ok(None);
    };
    Ok(Some(Vended {
        env,
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
            refresh_time: expire_time - margin,
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
    let left = instant.saturating_duration_since(now);
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, millis);
    now_ms.saturating_add(millis(left))
}

/// A duration as whole milliseconds, clamped rather than wrapped.
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The answer, unless it reports the request's failure.
fn answer(response: Response) -> Result<Response, CliError> {
    match response.error {
        Some(error) => Err(error.into()),
        None => Ok(response),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease(expires_in: Duration) -> Vended {
        Vended {
            env: BTreeMap::from([("AWS_ACCESS_KEY_ID".to_string(), "key".to_string())]),
            expire_time_ms: Some(
                millis(SystemTime::now().duration_since(UNIX_EPOCH).unwrap()) + millis(expires_in),
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
