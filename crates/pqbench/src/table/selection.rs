//! Deterministic selection of active snapshot files before data reads.
use super::{Error, TableFile};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Filters are applied before sampling. Empty/default selects all files.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileSelection {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub partitions: BTreeMap<String, Option<String>>,
    /// all, first:N, every:N, or median:N; empty is equivalent to all.
    pub sample: String,
}
impl FileSelection {
    /// Whether this selection leaves the input unchanged.
    pub fn unrestricted(&self) -> bool {
        self.include.is_empty()
            && self.exclude.is_empty()
            && self.partitions.is_empty()
            && matches!(self.sample.as_str(), "" | "all")
    }
    /// Select active files using metadata only, in deterministic path order.
    ///
    /// Median sampling chooses files nearest the lower median byte size,
    /// breaking ties by path. A zero size is treated as unknown and rejected.
    ///
    /// # Errors
    /// Invalid globs/sample methods, zero N, or unknown median-sample sizes.
    pub fn select_files(&self, mut files: Vec<TableFile>) -> Result<Vec<TableFile>, Error> {
        let compile = |patterns: &[String]| {
            patterns
                .iter()
                .map(|p| {
                    glob::Pattern::new(p)
                        .map_err(|e| Error::new(format!("invalid file pattern: {e}")))
                })
                .collect::<Result<Vec<_>, _>>()
        };
        let include = compile(&self.include)?;
        let exclude = compile(&self.exclude)?;
        let method = if matches!(self.sample.as_str(), "" | "all") {
            None
        } else {
            let (method, n) = self
                .sample
                .split_once(':')
                .ok_or_else(|| Error::new("sample must be all, first:N, every:N, or median:N"))?;
            let n: usize = n
                .parse()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| Error::new("sample N must be positive"))?;
            if !matches!(method, "first" | "every" | "median") {
                return Err(Error::new("unknown file sample method"));
            }
            Some((method, n))
        };
        files.retain(|f| {
            (include.is_empty() || include.iter().any(|p| p.matches(&f.path)))
                && !exclude.iter().any(|p| p.matches(&f.path))
                && self
                    .partitions
                    .iter()
                    .all(|(k, v)| f.partition_values.get(k) == Some(v))
        });
        files.sort_by(|a, b| a.path.cmp(&b.path).then(a.uri.cmp(&b.uri)));
        match method {
            Some(("first", n)) => files.truncate(n),
            Some(("every", n)) => files = files.into_iter().step_by(n).collect(),
            Some(("median", n)) if !files.is_empty() => {
                if files.iter().any(|f| f.size_bytes == 0) {
                    return Err(Error::new(
                        "median sampling requires known positive file sizes",
                    ));
                }
                let mut sizes: Vec<_> = files.iter().map(|f| f.size_bytes).collect();
                sizes.sort_unstable();
                let median = sizes[(sizes.len() - 1) / 2];
                files.sort_by_key(|f| f.size_bytes.abs_diff(median));
                files.truncate(n);
                files.sort_by(|a, b| a.path.cmp(&b.path).then(a.uri.cmp(&b.uri)));
            }
            _ => {}
        }
        Ok(files)
    }
}
