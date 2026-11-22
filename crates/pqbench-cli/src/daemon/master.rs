//! The master: the map of partition versions, its whole state, and the
//! transitions that move them.
//!
//! The master holds no file and runs no command. A walk's listing makes
//! versions ([`Master::found`]) and deletes the ones it no longer finds;
//! [`Master::start`] hands out the next runs, each a [`Transition`] on one
//! version, and [`Master::finish`] takes how a run ended; [`Master::purge`]
//! drops what a walk deleted. The driver runs the commands and keeps the
//! files (`drive`, `store`); every call here is a short update of the map.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) use pqbench::v1::partition_version::State;
use pqbench::v1::{PartitionPrice, PartitionVersion};
use serde_json::Value;

/// A version that failed this many times in one transition is left where it is.
const MAX_FAILURES: u64 = 2;

/// Classes whose objects cannot be read without a restore, and the
/// Intelligent-Tiering access tiers that cannot.
const ARCHIVE_CLASSES: &[&str] = &["GLACIER", "DEEP_ARCHIVE"];
const ARCHIVE_TIERS: &[&str] = &["ARCHIVE_ACCESS", "DEEP_ARCHIVE_ACCESS"];

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
    output: "price.ndjson",
    stdout: true,
    local: false,
};

/// The transitions, in pipeline order: the sample file is copied, and the
/// commands that read its data read the copy.
const TRANSITIONS: &[Transition] = &[
    PRICE,
    Transition {
        source: State::Priced,
        running: State::Downloading,
        done: State::Downloaded,
        command: &["download"],
        input: Input::SampleFile,
        output: "sample.parquet",
        stdout: false,
        local: false,
    },
    Transition {
        source: State::Downloaded,
        running: State::Bytemassing,
        done: State::Bytemassed,
        command: &["bytemass"],
        input: Input::SampleFile,
        output: "bytemass.ndjson",
        stdout: true,
        local: true,
    },
    Transition {
        source: State::Bytemassed,
        running: State::Linting,
        done: State::UpToDate,
        command: &["list", "suggestions"],
        input: Input::SampleFile,
        output: "list-suggestions.ndjson",
        stdout: true,
        local: true,
    },
];

/// The transitions on a host with little network: no file is copied;
/// bytemass reads only the sample file's footer, in place, and is the last.
const LOW_NETWORK_TRANSITIONS: &[Transition] = &[
    PRICE,
    Transition {
        source: State::Priced,
        running: State::Bytemassing,
        done: State::UpToDate,
        command: &["bytemass"],
        input: Input::SampleFile,
        output: "bytemass.ndjson",
        stdout: true,
        local: false,
    },
];

/// A version: its partition name and etag.
pub(crate) type Key = (String, String);

/// One run the master handed out: a transition on one version.
pub(crate) struct Run {
    pub key: Key,
    pub transition: &'static Transition,
}

pub(crate) struct Master {
    /// [`LOW_NETWORK_TRANSITIONS`] on a host with little network, else
    /// [`TRANSITIONS`].
    transitions: &'static [Transition],
    /// The versions by partition and etag; a row's `name` is left empty, the
    /// key holds it, and [`named`] puts it back.
    rows: BTreeMap<Key, PartitionVersion>,
    /// Whether the rows changed since the last checkpoint.
    changed: bool,
}

impl Master {
    /// The master over a checkpoint's versions. The commands a version was
    /// running died with the old daemon, so it goes back to the state before
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
        Ok(Master {
            transitions,
            rows,
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
        let wanted: Vec<&str> = parent
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect();
        let collections = ["catalogs", "schemas", "tables", "partitions"];
        if !wanted.len().is_multiple_of(2)
            || wanted.len() > 2 * collections.len()
            || wanted
                .iter()
                .step_by(2)
                .zip(collections)
                .any(|(have, want)| *have != want)
        {
            return Err(format!(
                "{parent} is not a catalog, schema, table or partition name"
            ));
        }
        let prefix = wanted.join("/");
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
            .map(|(key, row)| named(key, row))
            .collect())
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
    pub(crate) fn found(&mut self, key: &Key, listing: &Value, now: u64) -> Vec<Key> {
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
            file_count: listing["file_count"].as_u64().unwrap_or(0),
            size_bytes: listing["size_bytes"].as_u64().unwrap_or(0),
            removed_size_bytes: listing["removed_size_bytes"].as_u64(),
            last_modification_time: listing["last_file"]["modification_time"].as_u64(),
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
    /// finishes before a new one starts. A version that failed its
    /// transition twice waits.
    pub(crate) fn start(&mut self, limit: usize) -> Vec<Run> {
        let mut runs = Vec::new();
        let transitions = self.transitions;
        for transition in transitions.iter().rev() {
            for (key, row) in &mut self.rows {
                if runs.len() >= limit {
                    break;
                }
                if row.state == transition.source && row.failure_count < MAX_FAILURES {
                    row.state = transition.running;
                    runs.push(Run {
                        key: key.clone(),
                        transition,
                    });
                }
            }
        }
        self.changed |= !runs.is_empty();
        runs
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
        let row = self.rows.get_mut(key)?;
        let transition = running_transition(self.transitions, row.state)?;
        self.changed = true;
        match outcome {
            Ok(price) => {
                row.state = transition.done;
                row.failure_count = 0;
                row.error = None;
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
            }
        }
        Some(Conclusion {
            state: row.state,
            sample: master_needs_sample(row),
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
}

/// How a finished run left its version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Conclusion {
    pub state: State,
    /// Whether the version still reads its copy of the sample file: a
    /// transition it waits for does, and one that stopped for good does not.
    pub sample: bool,
}

fn master_needs_sample(row: &PartitionVersion) -> bool {
    row.failure_count < MAX_FAILURES && needs_sample(row.state)
}

/// Whether a version in `state` reads its copy of the sample file: from the
/// download to the transitions that read the data.
pub(crate) fn needs_sample(state: State) -> bool {
    matches!(
        state,
        State::Downloading
            | State::Downloaded
            | State::Bytemassing
            | State::Bytemassed
            | State::Linting
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

/// Whether any of a price's files needs a restore to be read: an archive
/// class or access tier. Files with no class (not S3 objects) do not.
fn archived(price: &PartitionPrice) -> bool {
    let archive = |class: &str| ARCHIVE_CLASSES.contains(&class);
    price.storage_classes.iter().any(|class| archive(class))
        || price.storage_costs.iter().any(|cost| {
            archive(&cost.storage_class)
                || cost
                    .access_tier
                    .as_deref()
                    .is_some_and(|tier| ARCHIVE_TIERS.contains(&tier))
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    const PARTITION: &str = "catalogs/c/schemas/s/tables/t/partitions/day%3D1";

    /// A master that found one version of [`PARTITION`] with `file_count`
    /// files.
    fn found(file_count: u64) -> (Master, Key) {
        let mut master = Master::new(Vec::new(), false).unwrap();
        let key = key(PARTITION, "aa").unwrap();
        master.found(&key, &serde_json::json!({"file_count": file_count}), 1);
        (master, key)
    }

    /// Price one version with a result naming `class` and `tier`, and return
    /// the state it lands in.
    fn priced(file_count: u64, class: &str, tier: Option<&str>) -> State {
        let (mut master, key) = found(file_count);
        assert_eq!(master.start(1).len(), 1);
        let price: PartitionPrice = serde_json::from_value(serde_json::json!({
            "name": format!("{PARTITION}/price"),
            "partition_etag": "aa",
            "storage_classes": [class],
            "storage_costs": [{"storage_class": class, "access_tier": tier,
                "dollars_per_gibibyte_per_month": 0.023, "cost_per_month": 1.0}],
        }))
        .unwrap();
        let conclusion = master.finish(&key, Ok(Some(price))).unwrap();
        assert_eq!(master.rows[&key].highest_cost_per_month, Some(1.0));
        conclusion.state
    }

    /// A price ends the version when there is nothing to read: no live file,
    /// or a class or access tier that needs a restore.
    #[test]
    fn a_price_ends_the_version_when_there_is_nothing_to_read() {
        assert_eq!(priced(1, "STANDARD_IA", None), State::Priced);
        assert_eq!(priced(1, "GLACIER_IR", None), State::Priced);
        assert_eq!(priced(1, "GLACIER", None), State::UpToDate);
        assert_eq!(priced(1, "DEEP_ARCHIVE", None), State::UpToDate);
        assert_eq!(
            priced(1, "INTELLIGENT_TIERING", Some("FREQUENT_ACCESS")),
            State::Priced
        );
        assert_eq!(
            priced(1, "INTELLIGENT_TIERING", Some("ARCHIVE_ACCESS")),
            State::UpToDate
        );
        assert_eq!(priced(0, "STANDARD", None), State::UpToDate);
    }

    /// A failed run puts the version back with its error, and after two it
    /// is not handed out again.
    #[test]
    fn a_version_that_failed_twice_waits() {
        let (mut master, key) = found(1);
        for attempt in 0..2 {
            assert_eq!(master.start(1).len(), 1);
            let conclusion = master.finish(&key, Err(format!("boom {attempt}")));
            assert_eq!(conclusion.map(|c| c.state), Some(State::Pending));
        }
        assert_eq!(master.rows[&key].failure_count, 2);
        assert_eq!(master.rows[&key].error.as_deref(), Some("boom 1"));
        assert!(master.start(1).is_empty());
    }

    /// A restart puts a running version back to the state before its
    /// transition.
    #[test]
    fn a_restart_puts_running_versions_back() {
        let (mut master, key) = found(1);
        master.start(1);
        let versions: Vec<_> = master.versions().collect();
        assert_eq!(versions[0].state, State::Pricing);
        let master = Master::new(versions, false).unwrap();
        assert_eq!(master.rows[&key].state, State::Pending);
    }

    /// A name that would be a path outside the store is no key.
    #[test]
    fn a_name_that_leaves_the_store_is_no_key() {
        assert!(key("catalogs/c/schemas/s/tables/t/partitions/..", "aa").is_none());
        assert!(key(PARTITION, "../x").is_none());
        assert!(key(PARTITION, "aa").is_some());
    }
}
