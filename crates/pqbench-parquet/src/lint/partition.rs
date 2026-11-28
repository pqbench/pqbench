//! `tombstones`, `small-files`, and `delete-files`: the storage a partition's
//! metadata alone shows it does not need, without reading a file.
//!
//! All three rules decide from the totals `list partitions` already folded, so
//! they cost no read and hold for every partition of the walk, including the
//! ones whose files a host with little network never copies. The bytes a
//! tombstone holds are exact (`removed_size_bytes` is the log's own sum), so the
//! `tombstones` record is verified as it is found; `small-files` and
//! `delete-files` have no byte saving of their own and state a verdict instead.

use crate::v1;

/// The rule id a tombstone suggestion carries.
pub const TOMBSTONES_RULE: &str = "tombstones";

/// The rule id a small-files suggestion carries.
pub const SMALL_FILES_RULE: &str = "small-files";

/// The rule id a delete-files suggestion carries.
pub const DELETE_FILES_RULE: &str = "delete-files";

/// Removed bytes worth a vacuum on their own.
const TOMBSTONES_MIN_BYTES: u64 = 1024 * 1024;

/// Removed bytes worth a vacuum against the active bytes: at least one tenth.
const TOMBSTONES_ACTIVE_DENOMINATOR: u64 = 10;

/// A partition with at least this many files is small-filed when its files are
/// small: a handful of files is a partition, not a problem.
const SMALL_FILES_MIN_COUNT: u64 = 16;

/// The average active file size below which a partition's files are small.
const SMALL_FILES_AVERAGE_BYTES: u64 = 16 * 1024 * 1024;

/// Delete files worth a rewrite on their own: a scan pays for each one, however
/// small it is.
const DELETE_FILES_MIN_COUNT: u64 = 4;

/// Delete bytes worth a rewrite on their own.
const DELETE_FILES_MIN_BYTES: u64 = 1024 * 1024;

/// Delete bytes worth a rewrite against the data they narrow: at least one
/// twentieth, since a delete file is read on every scan it applies to.
const DELETE_FILES_ACTIVE_DENOMINATOR: u64 = 20;

/// The settings that delete a partition's tombstones, one per engine.
///
/// Delta keeps a removed file's tombstone for its deleted-file retention, so a
/// vacuum deletes the data file at once and the tombstone on the log's own
/// schedule. Iceberg has no tombstones: an expired snapshot's files go with
/// the snapshot, and the data files no snapshot names any more are orphans.
fn vacuum_remedies() -> Vec<v1::Remedy> {
    vec![
        v1::Remedy::new("delta", "VACUUM <table> RETAIN 168 HOURS"),
        v1::Remedy::new(
            "iceberg",
            "ALTER TABLE <table> EXECUTE expire_snapshots(retention_threshold => '7d'); \
             ALTER TABLE <table> EXECUTE remove_orphan_files(retention_threshold => '7d')",
        ),
    ]
}

/// The settings that compact a partition's small files, one per engine.
fn compaction_remedies() -> Vec<v1::Remedy> {
    vec![
        v1::Remedy::new("delta", "OPTIMIZE <table>"),
        v1::Remedy::new("iceberg", "ALTER TABLE <table> EXECUTE rewrite_data_files"),
        v1::Remedy::new(
            "spark",
            "df.coalesce(n) or df.repartition(n) before write, so one task writes one file",
        ),
    ]
}

/// The settings that merge a partition's delete files into its data files, one
/// per engine.
///
/// A merge-on-read table pays for its deletes twice: the delete files are
/// stored, and every scan reads them alongside the data. Rewriting the data
/// files applies them and leaves the position-delete files empty.
fn delete_remedies() -> Vec<v1::Remedy> {
    vec![
        v1::Remedy::new(
            "iceberg",
            "ALTER TABLE <table> EXECUTE rewrite_data_files; \
             ALTER TABLE <table> EXECUTE rewrite_position_delete_files",
        ),
        v1::Remedy::new(
            "spark",
            "CALL system.rewrite_data_files(table => '<table>'); \
             CALL system.rewrite_position_delete_files(table => '<table>')",
        ),
    ]
}

/// The suggestions a partition's metadata alone supports, one per rule that
/// triggers. Every rule is engine-agnostic: the suggestion names the settings
/// of every engine that can apply it.
#[must_use]
pub fn partition_suggestions(partition: &v1::Partition) -> Vec<v1::Suggestion> {
    let rules = [tombstones, small_files, delete_files];
    rules.iter().filter_map(|rule| rule(partition)).collect()
}

/// The bytes a vacuum recovers: the removed files the log still counts, when
/// they are worth a vacuum on their own or against the active bytes.
fn tombstones(partition: &v1::Partition) -> Option<v1::Suggestion> {
    let removed_bytes = partition.removed_size_bytes.filter(|bytes| *bytes > 0)?;
    let active_bytes = partition.size_bytes;
    let large = removed_bytes >= TOMBSTONES_MIN_BYTES;
    let share = active_bytes > 0
        && removed_bytes.saturating_mul(TOMBSTONES_ACTIVE_DENOMINATOR) >= active_bytes;
    if !large && !share {
        return None;
    }
    let remedies = vacuum_remedies();
    let remedy = super::setting(&remedies, "delta");
    let mut suggestion = candidate(
        partition,
        TOMBSTONES_RULE,
        format!(
            "{} removed files totalling {} are still stored, {}; a vacuum deletes them",
            partition.removed_file_count.unwrap_or(0),
            human_bytes(removed_bytes),
            kept_text(removed_bytes, active_bytes),
        ),
        remedy,
        remedies,
    );
    // The removed bytes are the log's own sum, so the saving needs no rewrite
    // to confirm it.
    suggestion.verified = Some(true);
    suggestion.savings_bytes = Some(removed_bytes);
    Some(suggestion)
}

/// The verdict on a partition whose files are small: enough files, each far
/// below the size a compacted file holds.
fn small_files(partition: &v1::Partition) -> Option<v1::Suggestion> {
    if partition.file_count < SMALL_FILES_MIN_COUNT {
        return None;
    }
    let average_bytes = partition.size_bytes / partition.file_count;
    if average_bytes >= SMALL_FILES_AVERAGE_BYTES {
        return None;
    }
    let remedies = compaction_remedies();
    let remedy = super::setting(&remedies, "delta");
    Some(candidate(
        partition,
        SMALL_FILES_RULE,
        format!(
            "{} files averaging {} each, far below the {} a compacted file holds",
            partition.file_count,
            human_bytes(average_bytes),
            human_bytes(SMALL_FILES_AVERAGE_BYTES),
        ),
        remedy,
        remedies,
    ))
}

/// The verdict on a partition whose merge-on-read delete files are worth
/// rewriting: enough of them, large enough, or large against the data they
/// narrow.
///
/// One delete file is a normal write, not a problem; a scan reads it anyway, so
/// it becomes one when there are several, when the file is large, or when it is
/// not small next to the data it deletes from.
fn delete_files(partition: &v1::Partition) -> Option<v1::Suggestion> {
    let delete_file_count = partition.delete_file_count.filter(|count| *count > 0)?;
    let delete_size_bytes = partition.delete_size_bytes.unwrap_or_default();
    let active_bytes = partition.size_bytes;
    let many = delete_file_count >= DELETE_FILES_MIN_COUNT;
    let large = delete_size_bytes >= DELETE_FILES_MIN_BYTES;
    let share = active_bytes > 0
        && delete_size_bytes.saturating_mul(DELETE_FILES_ACTIVE_DENOMINATOR) >= active_bytes;
    if !many && !large && !share {
        return None;
    }
    let remedies = delete_remedies();
    let remedy = super::setting(&remedies, "iceberg");
    Some(candidate(
        partition,
        DELETE_FILES_RULE,
        format!(
            "{delete_file_count} delete files totalling {} are merge-on-read deletes, read on \
             every scan and stored until a rewrite folds them into the data",
            human_bytes(delete_size_bytes),
        ),
        remedy,
        remedies,
    ))
}

/// A partition rule's record: the partition as its name, and the table above
/// it when the name is a partition name.
fn candidate(
    partition: &v1::Partition,
    rule: &'static str,
    message: String,
    remedy: String,
    remedies: Vec<v1::Remedy>,
) -> v1::Suggestion {
    let table = partition
        .name
        .rsplit_once("/partitions/")
        .map(|(table, _)| table.to_string());
    v1::Suggestion {
        name: v1::name::partition_suggestion(&partition.name, rule),
        rule: rule.to_string(),
        table,
        partition: Some(partition.name.clone()),
        message,
        remedy,
        remedies,
        ..v1::Suggestion::default()
    }
}

/// How the removed bytes compare with the partition's active bytes, in words.
fn kept_text(removed_bytes: u64, active_bytes: u64) -> String {
    if active_bytes == 0 {
        return "with no active file left to keep".to_string();
    }
    let share = 100.0 * removed_bytes as f64 / active_bytes as f64;
    format!(
        "{}% of its {} of active bytes",
        super::percent(share),
        human_bytes(active_bytes)
    )
}

/// A byte count in the largest unit that keeps it above one.
fn human_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let value = bytes as f64;
    if value >= GIB {
        format!("{:.1} GiB", value / GIB)
    } else if value >= MIB {
        format!("{:.1} MiB", value / MIB)
    } else if value >= KIB {
        format!("{:.1} KiB", value / KIB)
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PARTITION: &str = "catalogs/c/schemas/s/tables/t/partitions/day%3D1";

    /// A partition with these totals and no removed bytes the log reports.
    fn partition(file_count: u64, size_bytes: u64) -> v1::Partition {
        v1::Partition {
            name: PARTITION.to_string(),
            file_count,
            size_bytes,
            ..v1::Partition::default()
        }
    }

    /// A partition whose log reports `removed_file_count` tombstones totalling
    /// `removed_size_bytes`.
    fn tombstoned(
        partition: v1::Partition,
        removed_file_count: u64,
        removed_size_bytes: u64,
    ) -> v1::Partition {
        v1::Partition {
            removed_file_count: Some(removed_file_count),
            removed_size_bytes: Some(removed_size_bytes),
            ..partition
        }
    }

    fn rule<'a>(suggestions: &'a [v1::Suggestion], rule: &str) -> Option<&'a v1::Suggestion> {
        suggestions
            .iter()
            .find(|suggestion| suggestion.rule == rule)
    }

    /// A log's tombstones are a measured saving: large enough on their own, or,
    /// below that, at least a tenth of the active bytes.
    #[test]
    fn tombstones_trigger_on_the_bytes_they_hold() {
        let removed_bytes = 2 * 1024 * 1024;
        let partition = tombstoned(partition(3, 100 * 1024 * 1024), 4, removed_bytes);
        let suggestions = partition_suggestions(&partition);
        let suggestion = rule(&suggestions, TOMBSTONES_RULE).expect("a tombstone suggestion");
        assert_eq!(suggestion.verified, Some(true));
        assert_eq!(suggestion.savings_bytes, Some(removed_bytes));
        assert_eq!(suggestion.partition.as_deref(), Some(PARTITION));
        assert_eq!(
            suggestion.table.as_deref(),
            Some("catalogs/c/schemas/s/tables/t")
        );
        assert_eq!(
            suggestion.name,
            format!("{PARTITION}/suggestions/{TOMBSTONES_RULE}")
        );
        assert!(suggestion.column.is_empty());
        assert!(
            suggestion.message.contains("2.0 MiB"),
            "{}",
            suggestion.message
        );
        for engine in ["delta", "iceberg"] {
            assert!(
                suggestion
                    .remedies
                    .iter()
                    .any(|remedy| remedy.engine == engine),
                "{engine} has no remedy"
            );
        }
        let json = serde_json::to_string(suggestion).expect("a suggestion serializes");
        let read: v1::Suggestion = serde_json::from_str(&json).expect("it parses");
        assert_eq!(&read, suggestion);
    }

    /// Removed bytes below a mebibyte still trigger when they are a tenth of
    /// what the partition keeps.
    #[test]
    fn tombstones_trigger_on_a_tenth_of_the_active_bytes() {
        let partition = tombstoned(partition(3, 1024 * 1024), 1, 200 * 1024);
        let suggestions = partition_suggestions(&partition);
        let suggestion = rule(&suggestions, TOMBSTONES_RULE).expect("a tombstone suggestion");
        assert_eq!(suggestion.savings_bytes, Some(200 * 1024));
    }

    /// No tombstones, an empty log field, and bytes too small against a large
    /// partition are no suggestion.
    #[test]
    fn tombstones_do_not_trigger_on_small_or_absent_removals() {
        let small = 1024 * 1024 - 1;
        for partition in [
            partition(3, 100 * 1024 * 1024),
            tombstoned(partition(3, 100 * 1024 * 1024), 0, 0),
            tombstoned(partition(3, 100 * 1024 * 1024), 2, small),
            tombstoned(partition(0, 0), 2, small),
        ] {
            assert!(
                rule(&partition_suggestions(&partition), TOMBSTONES_RULE).is_none(),
                "{partition:?}"
            );
        }
    }

    /// Enough small files is a verdict, with a remedy for each engine that
    /// compacts a table; it is not a measured byte saving.
    #[test]
    fn small_files_trigger_on_enough_small_files() {
        let average = 1024 * 1024;
        let partition = partition(16, 16 * average);
        let suggestions = partition_suggestions(&partition);
        let suggestion = rule(&suggestions, SMALL_FILES_RULE).expect("a small-files suggestion");
        assert_eq!(suggestion.verified, None);
        assert_eq!(suggestion.savings_bytes, None);
        assert_eq!(suggestion.partition.as_deref(), Some(PARTITION));
        assert!(
            suggestion.message.contains("16 files") && suggestion.message.contains("1.0 MiB"),
            "{}",
            suggestion.message
        );
        for engine in ["delta", "iceberg", "spark"] {
            assert!(
                suggestion
                    .remedies
                    .iter()
                    .any(|remedy| remedy.engine == engine),
                "{engine} has no remedy"
            );
        }
    }

    /// Too few files, or files that are already large, is no suggestion.
    #[test]
    fn small_files_do_not_trigger_on_few_or_large_files() {
        let average = SMALL_FILES_AVERAGE_BYTES;
        for partition in [
            partition(
                SMALL_FILES_MIN_COUNT - 1,
                (SMALL_FILES_MIN_COUNT - 1) * average,
            ),
            partition(SMALL_FILES_MIN_COUNT, SMALL_FILES_MIN_COUNT * average),
            partition(0, 0),
        ] {
            assert!(
                rule(&partition_suggestions(&partition), SMALL_FILES_RULE).is_none(),
                "{partition:?}"
            );
        }
    }

    /// A partition whose log reports `delete_file_count` merge-on-read delete
    /// files totalling `delete_size_bytes`.
    fn deleting(
        partition: v1::Partition,
        delete_file_count: u64,
        delete_size_bytes: u64,
    ) -> v1::Partition {
        v1::Partition {
            delete_file_count: Some(delete_file_count),
            delete_size_bytes: Some(delete_size_bytes),
            ..partition
        }
    }

    /// Enough delete files, large enough ones, or a large enough share of the
    /// data they narrow earns the rewrite.
    #[test]
    fn delete_files_trigger_on_the_count_the_bytes_or_the_share() {
        for partition in [
            deleting(partition(1, 1_000_000), DELETE_FILES_MIN_COUNT, 4_000),
            deleting(partition(1, 100 * 1024 * 1024), 1, DELETE_FILES_MIN_BYTES),
            // 64 KiB of deletes against 1 MiB of data is over one twentieth.
            deleting(partition(1, 1024 * 1024), 1, 64 * 1024),
        ] {
            let suggestions = partition_suggestions(&partition);
            let suggestion =
                rule(&suggestions, DELETE_FILES_RULE).expect("a delete-files suggestion");
            assert_eq!(suggestion.verified, None);
            assert_eq!(suggestion.savings_bytes, None);
            assert_eq!(suggestion.partition.as_deref(), Some(PARTITION));
            assert_eq!(
                suggestion.name,
                format!("{PARTITION}/suggestions/{DELETE_FILES_RULE}")
            );
            assert!(suggestion.column.is_empty());
            assert!(
                suggestion.message.contains("delete files"),
                "{}",
                suggestion.message
            );
            assert!(
                suggestion.remedy.contains("rewrite_data_files")
                    && suggestion.remedy.contains("rewrite_position_delete_files"),
                "{}",
                suggestion.remedy
            );
            for engine in ["iceberg", "spark"] {
                assert!(
                    suggestion
                        .remedies
                        .iter()
                        .any(|remedy| remedy.engine == engine),
                    "{engine} has no remedy"
                );
            }
            let json = serde_json::to_string(suggestion).expect("a suggestion serializes");
            let read: v1::Suggestion = serde_json::from_str(&json).expect("it parses");
            assert_eq!(&read, suggestion);
        }
    }

    /// One delete file is a normal write; it earns a rewrite only when it is
    /// large, or large next to the data it narrows.
    #[test]
    fn delete_files_do_not_trigger_on_one_small_file() {
        let small = DELETE_FILES_MIN_BYTES - 1;
        for partition in [
            // A partition with no delete files at all: an un-merged table.
            partition(10, 100 * 1024 * 1024),
            deleting(partition(10, 100 * 1024 * 1024), 0, 0),
            deleting(
                partition(10, 100 * 1024 * 1024),
                DELETE_FILES_MIN_COUNT - 1,
                small,
            ),
            deleting(partition(0, 0), 1, 1024),
        ] {
            assert!(
                rule(&partition_suggestions(&partition), DELETE_FILES_RULE).is_none(),
                "{partition:?}"
            );
        }
    }

    /// All three rules can trigger on one partition, in rule order, and each
    /// names its own rule.
    #[test]
    fn a_partition_can_earn_every_partition_rule() {
        let partition = deleting(
            tombstoned(partition(20, 20 * 1024 * 1024), 2, 3 * 1024 * 1024),
            2,
            2 * 1024 * 1024,
        );
        let suggestions = partition_suggestions(&partition);
        assert_eq!(suggestions.len(), 3);
        assert_eq!(suggestions[0].rule, TOMBSTONES_RULE);
        assert_eq!(suggestions[1].rule, SMALL_FILES_RULE);
        assert_eq!(suggestions[2].rule, DELETE_FILES_RULE);
        // The rules that predate delete files keep their Delta-first remedy.
        assert_eq!(suggestions[0].remedy, "VACUUM <table> RETAIN 168 HOURS");
        assert_eq!(suggestions[1].remedy, "OPTIMIZE <table>");
    }
}
