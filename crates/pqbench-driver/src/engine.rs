//! The engine: the map of partition versions, its whole state, and the
//! transitions that move them.
//!
//! The engine holds no file and runs no command. A walk's listing makes
//! versions ([`Engine::found`]) and deletes the ones it no longer finds;
//! [`Engine::start`] hands out the next runs, each a [`Transition`] on one
//! version, and [`Engine::finish`] takes how a run ended; [`Engine::purge`]
//! drops what a walk deleted. The driver runs the commands and keeps the
//! files (`drive`, `store`); every call here is a short update of the map.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) use pqbench::v1::partition_version::State;
use pqbench::v1::{Partition, PartitionPrice, PartitionVersion};

use super::protocol::DriverStatus;
use super::store;

/// Immediate retries before a version backs off.
const MAX_FAILURES: u64 = 2;

/// Initial backoff after [`MAX_FAILURES`] failures, doubling up to [`RETRY_MAX_MS`].
const RETRY_BASE_MS: u64 = 60_000;
const RETRY_MAX_MS: u64 = 60 * 60_000;

/// What a transition's command reads on standard input.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Input {
    /// The version's `list partitions` record.
    Partition,
    /// The partition's sample file, a TableFile record.
    SampleFile,
}

/// One transition: the command it runs on a version and the states it moves the
/// version through.
pub(crate) struct Transition {
    pub source: State,
    pub running: State,
    pub done: State,
    pub command: &'static [&'static str],
    pub input: Input,
    /// The result file the run leaves in the version's directory.
    pub output: &'static str,
    /// Whether the command writes the result to standard output; otherwise it
    /// is given `--output` and writes the file itself.
    pub stdout: bool,
    /// Whether the command reads the downloaded sample with `--local`.
    pub local: bool,
}

const PRICE: Transition = Transition {
    source: State::Pending,
    running: State::Pricing,
    done: State::Priced,
    command: &["get", "partition-price"],
    input: Input::Partition,
    output: store::PRICE,
    stdout: true,
    local: false,
};

/// The transitions, in pipeline order: the sample file is copied, the
/// commands that read its data read the copy, and the verifier measures the
/// suggestions the listing found while the copy is still there.
const TRANSITIONS: &[Transition] = &[
    PRICE,
    Transition {
        source: State::Priced,
        running: State::Downloading,
        done: State::Downloaded,
        command: &["download"],
        input: Input::SampleFile,
        output: store::SAMPLE,
        stdout: false,
        local: false,
    },
    Transition {
        source: State::Downloaded,
        running: State::Bytemassing,
        done: State::Bytemassed,
        command: &["bytemass"],
        input: Input::SampleFile,
        output: store::BYTEMASS,
        stdout: true,
        local: true,
    },
    Transition {
        source: State::Bytemassed,
        running: State::Linting,
        done: State::Listed,
        command: &["list", "suggestions"],
        input: Input::SampleFile,
        output: store::LIST_SUGGESTIONS,
        stdout: true,
        local: true,
    },
    VERIFY,
];

/// The transitions on a host with little network: no file is copied;
/// bytemass reads only the sample file's footer, in place, there is no lint,
/// and the verifier lands only what the partition's metadata alone supports.
const LOW_NETWORK_TRANSITIONS: &[Transition] = &[
    PRICE,
    Transition {
        source: State::Priced,
        running: State::Bytemassing,
        done: State::Bytemassed,
        command: &["bytemass"],
        input: Input::SampleFile,
        output: store::BYTEMASS,
        stdout: true,
        local: false,
    },
    Transition {
        source: State::Bytemassed,
        running: State::Verifying,
        done: State::UpToDate,
        command: &["get", "suggestion"],
        input: Input::Partition,
        output: store::SUGGESTIONS,
        stdout: true,
        local: true,
    },
];

/// The last transition either way: the potential suggestions on disk are
/// measured, and the partition rules that need no file are added.
const VERIFY: Transition = Transition {
    source: State::Listed,
    running: State::Verifying,
    done: State::UpToDate,
    command: &["get", "suggestion"],
    input: Input::Partition,
    output: store::SUGGESTIONS,
    stdout: true,
    local: true,
};

/// A version: its partition name and etag.
pub(crate) type Key = (String, String);

/// One run the engine handed out: a transition on one version.
pub(crate) struct Run {
    pub key: Key,
    pub transition: &'static Transition,
}

pub(crate) struct Engine {
    /// [`LOW_NETWORK_TRANSITIONS`] on a host with little network, else
    /// [`TRANSITIONS`].
    transitions: &'static [Transition],
    /// The versions by partition and etag; a row's `name` is left empty, the
    /// key holds it, and [`named`] puts it back.
    rows: BTreeMap<Key, PartitionVersion>,
    /// What the driver is doing: the walk, and the commands in flight.
    activity: Activity,
    /// Whether the rows changed since the last checkpoint.
    changed: bool,
}

/// What the driver is doing, for `status`: it is not the map, so it never
/// marks the map changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Activity {
    /// The worker commands running now: a version's command, or a walk.
    pub running_commands: u64,
    /// When the last walk started, milliseconds since the Unix epoch.
    pub last_walk_time: Option<u64>,
    /// Why the last walk failed; absent when it succeeded.
    pub last_walk_error: Option<String>,
}

impl Engine {
    /// The engine over a checkpoint's versions. The commands a version was
    /// running died with the old driver, so it goes back to the state before
    /// its transition.
    pub(crate) fn new(versions: Vec<PartitionVersion>, low_network: bool) -> Result<Self, String> {
        let transitions = match low_network {
            true => LOW_NETWORK_TRANSITIONS,
            false => TRANSITIONS,
        };
        let mut rows = BTreeMap::new();
        for mut row in versions {
            // A checkpoint from before the walk removed superseded versions
            // may name deleted ones; the sweep takes their results.
            if row.state == State::Deleted {
                continue;
            }
            let key = row
                .name
                .rsplit_once("/versions/")
                .and_then(|(name, etag)| key(name, etag))
                .ok_or_else(|| format!("bad checkpoint row: {} names no version", row.name))?;
            if let Some(transition) = running_transition(transitions, row.state) {
                row.state = transition.source;
            }
            row.name = String::new();
            rows.insert(key, row);
        }
        Ok(Engine {
            transitions,
            rows,
            activity: Activity::default(),
            changed: true,
        })
    }

    /// Every version, in key order.
    pub(crate) fn versions(&self) -> impl Iterator<Item = PartitionVersion> + '_ {
        self.rows.iter().map(|(key, row)| named(key, row))
    }

    /// The versions under `parent`, in key order: a catalog, schema, table or
    /// partition name, or every version when it is empty.
    pub(crate) fn records(&self, parent: &str) -> Result<Vec<PartitionVersion>, String> {
        Ok(self
            .keys(parent)?
            .into_iter()
            .map(|key| named(&key, &self.rows[&key]))
            .collect())
    }

    /// The keys of the versions under `parent`, in key order.
    pub(crate) fn keys(&self, parent: &str) -> Result<Vec<Key>, String> {
        let prefix = parent_prefix(parent)?;
        let under = |name: &str| {
            prefix.is_empty()
                || name == prefix
                || name
                    .strip_prefix(&prefix)
                    .is_some_and(|rest| rest.starts_with('/'))
        };
        Ok(self
            .rows
            .range((prefix.clone(), String::new())..)
            .take_while(|((name, _), _)| name.starts_with(&prefix))
            .filter(|((name, _), _)| under(name))
            .map(|(key, _)| key.clone())
            .collect())
    }

    /// The map as a whole: the versions by state, and what the driver is
    /// doing.
    pub(crate) fn status(&self) -> DriverStatus {
        let mut state_counts: BTreeMap<String, u64> = State::ALL
            .into_iter()
            .map(|state| (state.to_string(), 0))
            .collect();
        for row in self.rows.values() {
            *state_counts.entry(row.state.to_string()).or_default() += 1;
        }
        DriverStatus {
            state_counts,
            total_versions: self.rows.len() as u64,
            running_commands: self.activity.running_commands,
            failing_versions: self
                .rows
                .values()
                .filter(|row| row.failure_count > 0)
                .count() as u64,
            last_walk_time: self.activity.last_walk_time,
            last_walk_error: self.activity.last_walk_error.clone(),
        }
    }

    /// What the driver is doing now. A status update is not a map change, so
    /// it never asks for a checkpoint.
    pub(crate) fn set_activity(&mut self, activity: Activity) {
        self.activity = activity;
    }

    /// One version by name: a version, or a partition for its one version.
    pub(crate) fn version(&self, name: &str) -> Result<(Key, PartitionVersion), String> {
        if name.is_empty() {
            return Err("a version or partition name is required".into());
        }
        let found = match name.split_once("/versions/") {
            Some((partition, etag)) => self
                .rows
                .get_key_value(&(partition.to_string(), etag.to_string()))
                .ok_or_else(|| format!("no such version: {name}"))?,
            None => self
                .rows
                .range((name.to_string(), String::new())..)
                .take_while(|((other, _), _)| other == name)
                .max_by_key(|(_, row)| row.create_time)
                .ok_or_else(|| format!("no such partition: {name}"))?,
        };
        Ok((found.0.clone(), named(found.0, found.1)))
    }

    /// A version's state, if the map has it.
    pub(crate) fn state(&self, key: &Key) -> Option<State> {
        self.rows.get(key).map(|row| row.state)
    }

    /// When the newest walk found anything: the time a walk was last due
    /// from.
    pub(crate) fn last_list_time(&self) -> Option<u64> {
        self.rows.values().filter_map(|row| row.list_time).max()
    }

    /// A walk found a version, with `listing` its Partition record: a version
    /// the map lacks is a new `PENDING` row, with the partition's history from
    /// its newest version, its sizes and newest file's time from the listing;
    /// the versions of the partition it supersedes leave the map, and their
    /// keys are returned. Every found version is stamped with the walk's time.
    pub(crate) fn found(&mut self, key: &Key, listing: &Partition, now: u64) -> Vec<Key> {
        self.changed = true;
        if let Some(row) = self.rows.get_mut(key) {
            row.list_time = Some(now);
            return Vec::new();
        }
        let partition: Vec<(Key, u64, u64, u64)> = self
            .rows
            .range((key.0.clone(), String::new())..)
            .take_while(|((name, _), _)| *name == key.0)
            .map(|(key, row)| {
                (
                    key.clone(),
                    row.first_listed_time,
                    row.etag_change_count,
                    row.create_time,
                )
            })
            .collect();
        let newest = partition
            .iter()
            .max_by_key(|(_, _, _, create_time)| *create_time);
        let (first_listed_time, etag_change_count) =
            newest.map_or((now, 0), |newest| (newest.1, newest.2 + 1));
        let superseded: Vec<Key> = partition.into_iter().map(|(key, ..)| key).collect();
        for key in &superseded {
            self.rows.remove(key);
        }
        let row = PartitionVersion {
            state: State::Pending,
            create_time: now,
            list_time: Some(now),
            first_listed_time,
            etag_change_count,
            file_count: listing.file_count,
            size_bytes: listing.size_bytes,
            removed_size_bytes: listing.removed_size_bytes,
            last_modification_time: listing
                .last_file
                .as_ref()
                .and_then(|file| file.modification_time),
            ..PartitionVersion::default()
        };
        self.rows.insert(key.clone(), row);
        superseded
    }

    /// After a table's listing: its versions the listing did not name leave
    /// the map, and their keys are returned.
    pub(crate) fn missing(&mut self, table: &str, found: &BTreeSet<Key>) -> Vec<Key> {
        let prefix = format!("{}/partitions/", table.trim_end_matches('/'));
        let gone: Vec<Key> = self
            .rows
            .range((prefix.clone(), String::new())..)
            .take_while(|((name, _), _)| name.starts_with(&prefix))
            .filter(|(key, _)| !found.contains(*key))
            .map(|(key, _)| key.clone())
            .collect();
        self.remove(&gone);
        gone
    }

    /// After a walk that read every table: the versions of the tables it did
    /// not list leave the map, and their keys are returned.
    pub(crate) fn retain(&mut self, tables: &BTreeSet<String>) -> Vec<Key> {
        let gone: Vec<Key> = self
            .rows
            .keys()
            .filter(|(name, _)| !tables.contains(parent_table(name)))
            .cloned()
            .collect();
        self.remove(&gone);
        gone
    }

    /// Hand out up to `limit` runs, each version now in its transition's
    /// running state: the later transitions first, so a version in progress
    /// finishes before a new one starts, and within a transition the newest
    /// data first, then the biggest partition. A version in backoff waits
    /// until `retry_time`.
    pub(crate) fn start(&mut self, limit: usize) -> Vec<Run> {
        self.start_timed(limit, now_ms())
    }

    pub(crate) fn start_timed(&mut self, limit: usize, now: u64) -> Vec<Run> {
        let mut runs = Vec::new();
        let transitions = self.transitions;
        for transition in transitions.iter().rev() {
            let free = limit.saturating_sub(runs.len());
            if free == 0 {
                break;
            }
            let mut ready: Vec<&Key> = self
                .rows
                .iter()
                .filter(|(_, row)| {
                    row.state == transition.source
                        && row.retry_time.is_none_or(|until| now >= until)
                })
                .map(|(key, _)| key)
                .collect();
            // Only the runs that fit matter, so select them without ordering
            // every ready version; a large map is the normal case after a walk.
            if ready.len() > free {
                ready.select_nth_unstable_by(free, |left, right| self.priority(left, right));
                ready.truncate(free);
            }
            ready.sort_by(|left, right| self.priority(left, right));
            let chosen: Vec<Key> = ready.into_iter().cloned().collect();
            for key in chosen {
                let row = self.rows.get_mut(&key).expect("a chosen key is in the map");
                row.state = transition.running;
                runs.push(Run { key, transition });
            }
        }
        self.changed |= !runs.is_empty();
        runs
    }

    /// The order a transition's ready versions are scheduled in: the newest
    /// data first, then the largest partition, then the name.
    fn priority(&self, left: &Key, right: &Key) -> Ordering {
        let freshness = |key: &Key| {
            let row = &self.rows[key];
            row.last_modification_time
                .or(row.list_time)
                .unwrap_or_default()
        };
        freshness(right)
            .cmp(&freshness(left))
            .then_with(|| self.rows[right].size_bytes.cmp(&self.rows[left].size_bytes))
            .then_with(|| left.cmp(right))
    }

    /// A run ended: `Ok` with its price when it priced the version, `Ok` with
    /// none for any other transition, else its error. A success moves the
    /// version on; a price whose partition has no live file, or whose files
    /// need a restore, ends it, since there is nothing to read. A failure
    /// puts the version back with the error counted. Returns how it ended,
    /// `None` when the run no longer has a version.
    pub(crate) fn finish(
        &mut self,
        key: &Key,
        outcome: Result<Option<PartitionPrice>, String>,
    ) -> Option<Conclusion> {
        self.finish_timed(key, outcome, now_ms())
    }

    pub(crate) fn finish_timed(
        &mut self,
        key: &Key,
        outcome: Result<Option<PartitionPrice>, String>,
        now: u64,
    ) -> Option<Conclusion> {
        let row = self.rows.get_mut(key)?;
        let transition = running_transition(self.transitions, row.state)?;
        self.changed = true;
        match outcome {
            Ok(price) => {
                row.state = transition.done;
                row.failure_count = 0;
                row.error = None;
                row.retry_time = None;
                if let Some(price) = price {
                    let costs = price.storage_costs.iter().map(|cost| cost.cost_per_month);
                    row.lowest_cost_per_month = costs.clone().reduce(f64::min);
                    row.highest_cost_per_month = costs.reduce(f64::max);
                    if row.file_count == 0 || archived(&price) {
                        row.state = State::UpToDate;
                    }
                }
            }
            Err(error) => {
                row.state = transition.source;
                row.failure_count += 1;
                row.error = Some(error);
                if row.failure_count >= MAX_FAILURES {
                    let shift = (row.failure_count - MAX_FAILURES).min(6) as u32;
                    let delay = RETRY_BASE_MS
                        .saturating_mul(1_u64 << shift)
                        .min(RETRY_MAX_MS);
                    let delay = jitter_ms(delay, key, row.failure_count).min(RETRY_MAX_MS);
                    row.retry_time = Some(now.saturating_add(delay));
                    if needs_sample(row.state) {
                        row.state = State::Priced;
                    }
                }
            }
        }
        Some(Conclusion {
            state: row.state,
            sample: engine_needs_sample(row),
        })
    }

    /// Whether the rows changed since the last checkpoint.
    pub(crate) fn changed(&self) -> bool {
        self.changed
    }

    /// The rows are on disk.
    pub(crate) fn checkpointed(&mut self) {
        self.changed = false;
    }

    /// Take versions out of the map. A run on one finishes into nothing.
    fn remove(&mut self, keys: &[Key]) {
        for key in keys {
            self.rows.remove(key);
        }
        self.changed |= !keys.is_empty();
    }

    /// Put every version a run was moving back to the state before its
    /// transition: aborting the run killed its command, so a checkpoint taken
    /// now restarts them there.
    pub(crate) fn reset_running(&mut self) {
        let transitions = self.transitions;
        for row in self.rows.values_mut() {
            let Some(transition) = running_transition(transitions, row.state) else {
                continue;
            };
            row.state = transition.source;
            self.changed = true;
        }
    }
}

/// How a finished run left its version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Conclusion {
    pub state: State,
    /// Whether the version still reads its copy of the sample file: a
    /// transition it waits for does, and one that stopped for good does not.
    pub sample: bool,
}

fn engine_needs_sample(row: &PartitionVersion) -> bool {
    row.retry_time.is_none() && needs_sample(row.state)
}

/// Whether a version in `state` reads its copy of the sample file: from the
/// download to the verification that measures the listing against it.
pub(crate) fn needs_sample(state: State) -> bool {
    matches!(
        state,
        State::Downloading
            | State::Downloaded
            | State::Bytemassing
            | State::Bytemassed
            | State::Linting
            | State::Listed
            | State::Verifying
    )
}

/// The transition of `transitions` a version in `state` is running, if any.
fn running_transition(
    transitions: &'static [Transition],
    state: State,
) -> Option<&'static Transition> {
    transitions
        .iter()
        .find(|transition| transition.running == state)
}

/// The key of a partition name of eight plain segments and a hex etag: a
/// key that is also a path under the store's results, and cannot leave it.
pub(crate) fn key(name: &str, etag: &str) -> Option<Key> {
    let segments: Vec<&str> = name.split('/').collect();
    let valid = segments.len() == 8
        && segments[6] == "partitions"
        && segments
            .iter()
            .all(|segment| !segment.is_empty() && *segment != "." && *segment != "..")
        && !etag.is_empty()
        && etag.chars().all(|character| character.is_ascii_hexdigit());
    valid.then(|| (name.to_string(), etag.to_string()))
}

/// The table a partition name is under.
pub(crate) fn parent_table(name: &str) -> &str {
    name.split_once("/partitions/")
        .map_or(name, |(table, _)| table)
}

/// The canonical prefix a `parent` names: its non-empty segments, which must
/// spell catalog, schema, table and partition names in that order. The empty
/// parent is every version.
fn parent_prefix(parent: &str) -> Result<String, String> {
    let wanted: Vec<&str> = parent
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    let collections = ["catalogs", "schemas", "tables", "partitions"];
    let valid = wanted.len().is_multiple_of(2)
        && wanted.len() <= 2 * collections.len()
        && wanted
            .iter()
            .step_by(2)
            .zip(collections)
            .all(|(have, want)| *have == want);
    if !valid {
        return Err(format!(
            "{parent} is not a catalog, schema, table or partition name"
        ));
    }
    Ok(wanted.join("/"))
}

/// Whether any of a price's files needs a restore to be read. The fact comes
/// from the HEADs `get partition-price` made, not from a candidate price tier:
/// an Intelligent-Tiering price lists every access tier as a candidate cost.
fn archived(price: &PartitionPrice) -> bool {
    price.restore_required == Some(true)
}

/// A row as the [`PartitionVersion`] it is, its name put back.
fn named(key: &Key, row: &PartitionVersion) -> PartitionVersion {
    PartitionVersion {
        name: format!("{}/versions/{}", key.0, key.1),
        ..row.clone()
    }
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

/// A deterministic spread of at most ±25% on a retry delay: the hash of the
/// version and its failure count. Two versions that failed in the same tick
/// therefore retry at different times, so an outage that fails a whole walk
/// does not come back as a synchronized retry storm (Tail at Scale).
fn jitter_ms(delay: u64, key: &Key, failures: u64) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for byte in key
        .0
        .as_bytes()
        .iter()
        .chain(key.1.as_bytes())
        .chain(failures.to_le_bytes().iter())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    let spread = delay / 4;
    if spread == 0 {
        return delay;
    }
    delay - spread + hash % (2 * spread + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pqbench::v1::TableFile;

    const PARTITION: &str = "catalogs/c/schemas/s/tables/t/partitions/day%3D1";

    /// An engine that found one version of [`PARTITION`] with `file_count`
    /// files.
    fn found(file_count: u64) -> (Engine, Key) {
        let mut engine = Engine::new(Vec::new(), false).unwrap();
        let key = key(PARTITION, "aa").unwrap();
        let listing = Partition {
            file_count,
            ..Partition::default()
        };
        engine.found(&key, &listing, 1);
        (engine, key)
    }

    /// Price one version with a result naming `class` and `tier` (and whether
    /// its files need a restore), and return the state it lands in.
    fn priced(file_count: u64, class: &str, tier: Option<&str>) -> State {
        priced_requiring_restore(file_count, class, tier, None)
    }

    fn priced_requiring_restore(
        file_count: u64,
        class: &str,
        tier: Option<&str>,
        restore_required: Option<bool>,
    ) -> State {
        let (mut engine, key) = found(file_count);
        assert_eq!(engine.start(1).len(), 1);
        let mut price: PartitionPrice = serde_json::from_value(serde_json::json!({
            "name": format!("{PARTITION}/price"),
            "partition_etag": "aa",
            "storage_classes": [class],
            "storage_costs": [{"storage_class": class, "access_tier": tier,
                "dollars_per_gibibyte_per_month": 0.023, "cost_per_month": 1.0}],
        }))
        .unwrap();
        price.restore_required = restore_required;
        let conclusion = engine.finish(&key, Ok(Some(price))).unwrap();
        assert_eq!(engine.rows[&key].highest_cost_per_month, Some(1.0));
        conclusion.state
    }

    /// The price `get partition-price` writes for an Intelligent-Tiering
    /// partition: all five access tiers are candidate costs, and none of them
    /// is a fact about where the object is.
    fn intelligent_tiering_price(restore_required: Option<bool>) -> PartitionPrice {
        let costs: Vec<serde_json::Value> = [
            ("FREQUENT_ACCESS", 0.023),
            ("INFREQUENT_ACCESS", 0.0125),
            ("ARCHIVE_INSTANT_ACCESS", 0.004),
            ("ARCHIVE_ACCESS", 0.0036),
            ("DEEP_ARCHIVE_ACCESS", 0.00099),
        ]
        .into_iter()
        .map(|(tier, price)| {
            serde_json::json!({"storage_class": "INTELLIGENT_TIERING", "access_tier": tier,
                "dollars_per_gibibyte_per_month": price, "cost_per_month": 1.0})
        })
        .collect();
        let mut price: PartitionPrice = serde_json::from_value(serde_json::json!({
            "name": format!("{PARTITION}/price"),
            "partition_etag": "aa",
            "storage_classes": ["INTELLIGENT_TIERING"],
            "storage_costs": costs,
        }))
        .unwrap();
        price.restore_required = restore_required;
        price
    }

    /// A price ends the version when there is nothing to read: no live file,
    /// or a HEAD that says the files need a restore.
    #[test]
    fn a_price_ends_the_version_when_there_is_nothing_to_read() {
        assert_eq!(priced(1, "STANDARD_IA", None), State::Priced);
        assert_eq!(priced(1, "GLACIER_IR", None), State::Priced);
        assert_eq!(
            priced_requiring_restore(1, "GLACIER", None, Some(true)),
            State::UpToDate
        );
        assert_eq!(
            priced_requiring_restore(1, "DEEP_ARCHIVE", None, Some(true)),
            State::UpToDate
        );
        assert_eq!(
            priced_requiring_restore(1, "GLACIER", None, Some(false)),
            State::Priced
        );
        assert_eq!(priced(0, "STANDARD", None), State::UpToDate);
    }

    /// An Intelligent-Tiering partition stays priced: its five candidate costs
    /// are not evidence of an archive tier, so a false positive would leave
    /// every such partition unmeasured.
    #[test]
    fn intelligent_tiering_candidate_costs_are_not_archived() {
        let (mut engine, key) = found(1);
        assert_eq!(engine.start(1).len(), 1);
        let price = intelligent_tiering_price(Some(false));
        let conclusion = engine.finish(&key, Ok(Some(price))).unwrap();
        assert_eq!(conclusion.state, State::Priced);

        // An older price without the field is not an archive verdict either.
        let (mut engine, key) = found(1);
        assert_eq!(engine.start(1).len(), 1);
        let price = intelligent_tiering_price(None);
        let conclusion = engine.finish(&key, Ok(Some(price))).unwrap();
        assert_eq!(conclusion.state, State::Priced);
    }

    /// A transition takes the newest data first, then the biggest partition.
    #[test]
    fn a_transition_schedules_recent_and_biggest_first() {
        let mut engine = Engine::new(Vec::new(), false).unwrap();
        let now = 10_000;
        let mut found = |day: &str, modified: u64, size: u64| {
            let name = format!("catalogs/c/schemas/s/tables/t/partitions/day%3D{day}");
            let key = key(&name, "aa").unwrap();
            let listing = Partition {
                file_count: 1,
                size_bytes: size,
                last_file: Some(TableFile {
                    modification_time: Some(modified),
                    ..TableFile::default()
                }),
                ..Partition::default()
            };
            engine.found(&key, &listing, now);
            key
        };
        let old_big = found("1", 1_000, 500);
        let new_small = found("2", 9_000, 10);
        let new_big = found("3", 9_000, 900);
        let middle = found("4", 5_000, 100);
        let scheduled: Vec<Key> = engine.start(4).into_iter().map(|run| run.key).collect();
        assert_eq!(scheduled, vec![new_big, new_small, middle, old_big]);
    }

    /// A failed run puts the version back with its error, and after two it
    /// waits for its (jittered) backoff window before retrying.
    #[test]
    fn a_version_that_failed_twice_waits() {
        let (mut engine, key) = found(1);
        let t0 = 1_000_000;
        for attempt in 0..2 {
            assert_eq!(engine.start_timed(1, t0).len(), 1);
            let conclusion = engine.finish_timed(&key, Err(format!("boom {attempt}")), t0);
            assert_eq!(conclusion.map(|c| c.state), Some(State::Pending));
        }
        assert_eq!(engine.rows[&key].failure_count, 2);
        assert_eq!(engine.rows[&key].error.as_deref(), Some("boom 1"));
        let retry_time = engine.rows[&key].retry_time.expect("the version waits");
        assert!(
            retry_time >= t0 + RETRY_BASE_MS * 3 / 4 && retry_time <= t0 + RETRY_BASE_MS * 5 / 4,
            "the backoff was {retry_time} ms"
        );
        assert!(engine.start_timed(1, retry_time - 1).is_empty());
        assert_eq!(engine.start_timed(1, retry_time).len(), 1);
    }

    /// Two versions that fail in the same tick back off to different times, so
    /// one outage does not bring every retry back at once; the same version and
    /// failure count always gets the same delay.
    #[test]
    fn a_failure_that_spreads_retries_is_deterministic() {
        let left: Key = key(PARTITION, "aa").unwrap();
        let right: Key = key(PARTITION, "bb").unwrap();
        let delays: Vec<u64> = [&left, &right]
            .into_iter()
            .map(|key| jitter_ms(RETRY_BASE_MS, key, MAX_FAILURES))
            .collect();
        assert_ne!(delays[0], delays[1], "both versions retry together");
        assert_eq!(delays[0], jitter_ms(RETRY_BASE_MS, &left, MAX_FAILURES));
        for delay in delays {
            assert!((RETRY_BASE_MS * 3 / 4..=RETRY_BASE_MS * 5 / 4).contains(&delay));
        }
    }

    /// A restart puts a running version back to the state before its
    /// transition.
    #[test]
    fn a_restart_puts_running_versions_back() {
        let (mut engine, key) = found(1);
        engine.start(1);
        let versions: Vec<_> = engine.versions().collect();
        assert_eq!(versions[0].state, State::Pricing);
        let engine = Engine::new(versions, false).unwrap();
        assert_eq!(engine.rows[&key].state, State::Pending);
    }

    /// `status` counts every state the map can hold, the versions that are
    /// failing, and what the driver is doing.
    #[test]
    fn a_status_counts_the_map_by_state() {
        let (mut engine, key) = found(1);
        engine.set_activity(Activity {
            running_commands: 2,
            last_walk_time: Some(42),
            last_walk_error: Some("boom".into()),
        });
        let status = engine.status();
        assert_eq!(status.total_versions, 1);
        assert_eq!(status.state_counts.len(), State::ALL.len());
        assert_eq!(status.state_counts["PENDING"], 1);
        assert_eq!(status.state_counts["UP_TO_DATE"], 0);
        assert_eq!(status.running_commands, 2);
        assert_eq!(status.last_walk_time, Some(42));
        assert_eq!(status.last_walk_error.as_deref(), Some("boom"));
        assert_eq!(status.failing_versions, 0);

        engine.start(1);
        engine.finish_timed(&key, Err("boom".into()), 1);
        assert_eq!(engine.status().failing_versions, 1);
    }

    /// A parent selects the keys under it at any level, and a name that is not
    /// one is refused.
    #[test]
    fn a_parent_selects_the_keys_under_it() {
        let (engine, key) = found(1);
        for parent in [
            "",
            "catalogs/c",
            "catalogs/c/schemas/s",
            "catalogs/c/schemas/s/tables/t",
            PARTITION,
        ] {
            assert_eq!(engine.keys(parent).unwrap(), vec![key.clone()], "{parent}");
        }
        assert!(engine.keys("catalogs/other").unwrap().is_empty());
        assert!(engine.keys("catalogs/c/tables/t").is_err());
    }

    /// A stop puts the versions a run was moving back to the state before its
    /// transition, and asks for a checkpoint of that map.
    #[test]
    fn a_stop_resets_the_versions_a_run_was_moving() {
        let (mut engine, key) = found(1);
        engine.checkpointed();
        assert!(!engine.changed());
        engine.start(1);
        assert_eq!(engine.rows[&key].state, State::Pricing);
        engine.checkpointed();
        engine.reset_running();
        assert_eq!(engine.rows[&key].state, State::Pending);
        assert!(engine.changed());
    }

    /// A name that would be a path outside the store is no key.
    #[test]
    fn a_name_that_leaves_the_store_is_no_key() {
        assert!(key("catalogs/c/schemas/s/tables/t/partitions/..", "aa").is_none());
        assert!(key(PARTITION, "../x").is_none());
        assert!(key(PARTITION, "aa").is_some());
    }

    /// A deterministic pseudo-random sequence: enough variety to reach every
    /// transition, and the same run on every machine when a test fails.
    struct Lcg(u64);

    impl Lcg {
        fn new(seed: u64) -> Self {
            Self(seed)
        }

        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0 >> 33
        }

        fn pick(&mut self, bound: u64) -> u64 {
            self.next() % bound
        }
    }

    fn running(state: State) -> bool {
        TRANSITIONS
            .iter()
            .any(|transition| transition.running == state)
    }

    /// Whether `state` is a state a version can reach from `previous` in one
    /// step: no transition may skip a state or go backwards except through the
    /// failure/reset edge back to its source.
    fn follows_an_edge(previous: State, state: State) -> bool {
        if previous == state {
            return true;
        }
        TRANSITIONS.iter().any(|transition| {
            (previous == transition.source && state == transition.running)
                || (previous == transition.running
                    && (state == transition.done
                        || state == transition.source
                        || state == State::Priced
                        || state == State::UpToDate))
        })
    }

    /// IronFleet-style state-machine safety: a seeded sequence of every
    /// transition the engine has, with its invariants checked after every
    /// step.
    ///
    /// The invariants are that the map holds exactly the versions the walk
    /// found (none lost, none duplicated), every state change follows an edge
    /// of the transition graph, `reset_running` leaves no version running, and
    /// every backoff stays inside its bounds.
    #[test]
    fn the_map_keeps_its_invariants_under_a_seeded_transition_sequence() {
        let tables = [
            "catalogs/c/schemas/s/tables/t",
            "catalogs/c/schemas/s/tables/u",
        ];
        let etags = ["aa", "bb", "cc"];
        let mut sequence = Lcg::new(0x5eed);
        let mut engine = Engine::new(Vec::new(), false).unwrap();
        // The versions the walk found: the reference the map is checked against.
        let mut expected: BTreeMap<Key, ()> = BTreeMap::new();
        let mut previous: BTreeMap<Key, State> = BTreeMap::new();
        let mut now = 1_000_000_u64;
        // How often each transition ran, so a sequence that silently skipped
        // one could not pass by checking nothing.
        let mut ran = [0_u64; 6];

        for step in 0..600 {
            now += 1_000;
            let op = sequence.pick(6) as usize;
            ran[op] += 1;
            match op {
                // A walk found a version: the newest etag supersedes the old.
                0 => {
                    let table = tables[sequence.pick(tables.len() as u64) as usize];
                    let day = sequence.pick(3);
                    let name = format!("{table}/partitions/day%3D{day}");
                    let etag = etags[sequence.pick(etags.len() as u64) as usize];
                    let key = key(&name, etag).unwrap();
                    let listing = Partition {
                        file_count: sequence.pick(2),
                        ..Partition::default()
                    };
                    let superseded = engine.found(&key, &listing, now);
                    for key in superseded {
                        expected.remove(&key);
                    }
                    expected.insert(key, ());
                }
                // The driver hands out runs.
                1 => {
                    let limit = 1 + sequence.pick(4) as usize;
                    engine.start_timed(limit, now);
                }
                // A run ends, either way.
                2 => {
                    let running: Vec<Key> = engine
                        .rows
                        .iter()
                        .filter(|(_, row)| running(row.state))
                        .map(|(key, _)| key.clone())
                        .collect();
                    if running.is_empty() {
                        continue;
                    }
                    let key = running[sequence.pick(running.len() as u64) as usize].clone();
                    let price = (engine.rows[&key].state == State::Pricing).then(|| {
                        serde_json::from_value(serde_json::json!({
                            "name": format!("{}/versions/{}", key.0, key.1),
                            "partition_etag": key.1,
                            "storage_classes": ["STANDARD"],
                            "storage_costs": [{"storage_class": "STANDARD",
                                "dollars_per_gibibyte_per_month": 0.023,
                                "cost_per_month": 1.0}],
                        }))
                        .unwrap()
                    });
                    let outcome = if sequence.pick(3) == 0 {
                        Err(format!("boom {step}"))
                    } else {
                        Ok(price)
                    };
                    let failed = outcome.is_err();
                    engine.finish_timed(&key, outcome, now);
                    if failed && engine.rows[&key].failure_count >= MAX_FAILURES {
                        let wait = engine.rows[&key]
                            .retry_time
                            .expect("a backed-off version waits");
                        let delay = wait - now;
                        assert!(
                            (RETRY_BASE_MS * 3 / 4..=RETRY_MAX_MS).contains(&delay),
                            "step {step}: a backoff of {delay} ms is outside its bounds"
                        );
                    }
                }
                // A table's listing no longer names some of its versions.
                3 => {
                    let table = tables[sequence.pick(tables.len() as u64) as usize];
                    let under: Vec<Key> = engine
                        .rows
                        .keys()
                        .filter(|(name, _)| name.starts_with(&format!("{table}/partitions/")))
                        .cloned()
                        .collect();
                    let found: BTreeSet<Key> = under
                        .into_iter()
                        .filter(|_| sequence.pick(2) == 1)
                        .collect();
                    let purged = engine.missing(table, &found);
                    for key in purged {
                        expected.remove(&key);
                    }
                }
                // A walk lists only some tables: the rest are purged.
                4 => {
                    let listed: BTreeSet<String> = tables
                        .iter()
                        .filter(|_| sequence.pick(2) == 1)
                        .map(|table| table.to_string())
                        .collect();
                    for key in engine.retain(&listed) {
                        expected.remove(&key);
                    }
                }
                // A stop, then a restart from the checkpoint.
                _ => {
                    engine.reset_running();
                    for row in engine.rows.values() {
                        assert!(
                            !running(row.state),
                            "step {step}: a reset left a version in {}",
                            row.state
                        );
                    }
                    previous = engine
                        .rows
                        .iter()
                        .map(|(key, row)| (key.clone(), row.state))
                        .collect();
                    let versions: Vec<_> = engine.versions().collect();
                    engine = Engine::new(versions, false).unwrap();
                }
            }

            // No version lost or duplicated: the map is exactly what the walk
            // found, once each.
            let keys: BTreeSet<&Key> = engine.rows.keys().collect();
            let wanted: BTreeSet<&Key> = expected.keys().collect();
            assert_eq!(
                keys, wanted,
                "step {step}: the map lost or gained a version"
            );
            let names: BTreeSet<String> = engine.versions().map(|row| row.name).collect();
            assert_eq!(
                names.len(),
                engine.rows.len(),
                "step {step}: a version is duplicated"
            );

            // Every state change follows an edge of the transition graph.
            for (key, row) in &engine.rows {
                if let Some(state) = previous.get(key) {
                    assert!(
                        follows_an_edge(*state, row.state),
                        "step {step}: {} went from {} to {}",
                        key.0,
                        state,
                        row.state
                    );
                }
                let backed_off = row.failure_count >= MAX_FAILURES;
                assert_eq!(
                    backed_off,
                    row.retry_time.is_some(),
                    "step {step}: a failure count and its backoff disagree"
                );
            }
            previous = engine
                .rows
                .iter()
                .map(|(key, row)| (key.clone(), row.state))
                .collect();
        }

        for (op, count) in ran.into_iter().enumerate() {
            assert!(count > 0, "the sequence never ran transition {op}");
        }
    }
}
