//! `--rows`: how many leading rows of a parquet sample to read.

use std::num::NonZeroUsize;

use crate::CliError;

/// How much of a sample to read: every row, or the first `N`.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RowLimit {
    /// Every row.
    All,
    /// The first `N` rows (`N >= 1`).
    First(NonZeroUsize),
}

impl RowLimit {
    /// The bound to hand a reader: `None` reads every row.
    pub(crate) fn max_rows(self) -> Option<usize> {
        match self {
            RowLimit::All => None,
            RowLimit::First(rows) => Some(rows.get()),
        }
    }
}

/// Parse `--rows`: `all` reads every row, `first:N` reads N (N >= 1).
pub(crate) fn parse_rows(method: &str) -> Result<RowLimit, CliError> {
    if method == "all" {
        return Ok(RowLimit::All);
    }
    if let Some(rest) = method.strip_prefix("first:") {
        let rows: usize = rest
            .parse()
            .map_err(|_| format!("bad --rows `{method}`; expected `all` or `first:N`"))?;
        let rows = NonZeroUsize::new(rows)
            .ok_or_else(|| format!("bad --rows `{method}`; N must be at least 1"))?;
        return Ok(RowLimit::First(rows));
    }
    Err(format!("bad --rows `{method}`; expected `all` or `first:N`").into())
}
