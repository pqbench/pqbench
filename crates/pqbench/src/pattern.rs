//! Unix-style path globs for lake table names and table file paths.
//!
//! `*` and `?` do not cross `/`. `**` matches zero or more path components.
//! A pattern without `/` also matches a single path component (gitignore
//! `tmp` skips `tmp` and `sales/tmp`).

use glob::Pattern;
use serde::{Deserialize, Serialize};

/// Errors from a glob or a sample spec.
#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pattern: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// How to pick files after include/exclude. `ALL` keeps every remaining path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sample {
    /// Keep every remaining file, in path order.
    ALL,
    /// Keep every `n`th file (`n >= 1`), starting at the first.
    Every(u32),
    /// Keep the first `n` files (`n >= 1`).
    First(u32),
}

impl Sample {
    /// Parse `all`, `every:N`, or `first:N`.
    ///
    /// # Errors
    /// Fails when the spec is unknown or `N` is not a positive integer.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value == "all" {
            return Ok(Self::ALL);
        }
        if let Some(count) = value.strip_prefix("every:") {
            return Ok(Self::Every(positive(count, "every")?));
        }
        if let Some(count) = value.strip_prefix("first:") {
            return Ok(Self::First(positive(count, "first")?));
        }
        Err(Error(format!(
            "unknown sample `{value}`; expected all, every:N, or first:N"
        )))
    }

    /// Wire form: `all`, `every:N`, or `first:N`.
    #[must_use]
    pub fn as_str(self) -> String {
        match self {
            Self::ALL => "all".into(),
            Self::Every(count) => format!("every:{count}"),
            Self::First(count) => format!("first:{count}"),
        }
    }
}

/// The file-selection options a command applied. Omitted from JSON when unused
/// so a full-table analysis stays a short document.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selection {
    /// Keep files whose partition path matches any of these globs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,
    /// Drop files whose partition path matches any of these globs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<String>,
    /// `all`, `every:N`, or `first:N`. Empty or `all` is omitted.
    #[serde(default, skip_serializing_if = "is_all_sample")]
    pub sample: String,
    /// Drop files whose log modification time is before this RFC3339 instant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_modified_before: Option<String>,
    /// Drop files whose log modification time is after this RFC3339 instant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_modified_after: Option<String>,
    /// Drop files added before this snapshot version (Delta add version).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_version_before: Option<u64>,
    /// Drop files added after this snapshot version (Delta add version).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_version_after: Option<u64>,
    /// Drop snapshots created before this RFC3339 instant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_snapshot_before: Option<String>,
    /// Drop snapshots created after this RFC3339 instant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_snapshot_after: Option<String>,
}

fn is_all_sample(sample: &str) -> bool {
    sample.is_empty() || sample == "all"
}

impl Selection {
    /// Whether no selection option was set.
    #[must_use]
    pub fn is_default(&self) -> bool {
        self.include.is_empty()
            && self.exclude.is_empty()
            && is_all_sample(&self.sample)
            && self.exclude_modified_before.is_none()
            && self.exclude_modified_after.is_none()
            && self.exclude_version_before.is_none()
            && self.exclude_version_after.is_none()
            && self.exclude_snapshot_before.is_none()
            && self.exclude_snapshot_after.is_none()
    }

    /// Whether a snapshot-time exclude was set.
    #[must_use]
    pub fn snapshot_time(&self) -> bool {
        self.exclude_snapshot_before.is_some() || self.exclude_snapshot_after.is_some()
    }

    /// Whether a file modified-time exclude was set.
    #[must_use]
    pub fn modified_time(&self) -> bool {
        self.exclude_modified_before.is_some() || self.exclude_modified_after.is_some()
    }

    /// Whether a file add-version exclude was set.
    #[must_use]
    pub fn add_version(&self) -> bool {
        self.exclude_version_before.is_some() || self.exclude_version_after.is_some()
    }
}

/// Whether `value` is kept: any `--include` (if given), then no `--exclude`.
///
/// # Errors
/// Fails when a pattern is not a valid glob.
pub fn keep(value: &str, include: &[String], exclude: &[String]) -> Result<bool, Error> {
    let value = normalize(value);
    if !include.is_empty() {
        let mut matched = false;
        for pattern in include {
            if matches(pattern, &value)? {
                matched = true;
                break;
            }
        }
        if !matched {
            return Ok(false);
        }
    }
    for pattern in exclude {
        if matches(pattern, &value)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether a glob matches a relative path.
///
/// # Errors
/// Fails when `pattern` is not a valid glob.
pub fn matches(pattern: &str, value: &str) -> Result<bool, Error> {
    let pattern = normalize(pattern);
    let value = normalize(value);
    if pattern.contains('/') || pattern.contains("**") {
        return match_path(&pattern, &value);
    }
    if match_path(&pattern, &value)? {
        return Ok(true);
    }
    Ok(value.split('/').any(|part| match_component(&pattern, part)))
}

/// Whether a walk should enter `prefix`. False when every table under it
/// would be dropped by include/exclude.
///
/// # Errors
/// Fails when a pattern is not a valid glob.
pub fn walk(prefix: &str, include: &[String], exclude: &[String]) -> Result<bool, Error> {
    let prefix = normalize(prefix);
    if prefix.is_empty() {
        return Ok(true);
    }
    if excluded_tree(&prefix, exclude)? {
        return Ok(false);
    }
    if include.is_empty() {
        return Ok(true);
    }
    for pattern in include {
        if can_match_under(pattern, &prefix)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Keep items whose path is selected, in path order, then apply `sample`.
///
/// # Errors
/// Fails when a pattern is invalid, the sample is empty, or nothing remains.
pub fn select<T>(
    mut items: Vec<T>,
    path: impl Fn(&T) -> &str,
    include: &[String],
    exclude: &[String],
    sample: Sample,
) -> Result<Vec<T>, Error> {
    let mut kept = Vec::new();
    for item in items.drain(..) {
        if keep(path(&item), include, exclude)? {
            kept.push(item);
        }
    }
    kept.sort_by(|left, right| path(left).cmp(path(right)));
    let kept = take(kept, sample);
    if kept.is_empty() {
        return Err(Error("no paths matched".into()));
    }
    Ok(kept)
}

fn take<T>(items: Vec<T>, sample: Sample) -> Vec<T> {
    match sample {
        Sample::ALL => items,
        Sample::Every(stride) => items.into_iter().step_by(stride as usize).collect(),
        Sample::First(count) => items.into_iter().take(count as usize).collect(),
    }
}

fn excluded_tree(prefix: &str, exclude: &[String]) -> Result<bool, Error> {
    for pattern in exclude {
        let pattern = normalize(pattern);
        if !pattern.contains('/')
            && prefix
                .split('/')
                .any(|part| match_component(&pattern, part))
        {
            return Ok(true);
        }
        if let Some(root) = pattern.strip_suffix("/**") {
            if match_path(root, prefix)? || match_path(&pattern, prefix)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn can_match_under(pattern: &str, prefix: &str) -> Result<bool, Error> {
    let pattern = normalize(pattern);
    if matches(&pattern, prefix)? {
        return Ok(true);
    }
    if !pattern.contains('/') && !pattern.contains("**") {
        return Ok(true);
    }
    let pat: Vec<&str> = pattern.split('/').collect();
    let pre: Vec<&str> = prefix.split('/').filter(|part| !part.is_empty()).collect();
    Ok(prefix_parts(&pat, &pre))
}

fn prefix_parts(pattern: &[&str], prefix: &[&str]) -> bool {
    match (pattern, prefix) {
        (_, []) => true,
        ([], _) => false,
        (["**"], _) => true,
        (["**", rest @ ..], prefix) => (0..=prefix.len()).any(|i| prefix_parts(rest, &prefix[i..])),
        ([pattern, rest @ ..], [part, prefix @ ..]) => {
            match_component(pattern, part) && prefix_parts(rest, prefix)
        }
    }
}

fn match_path(pattern: &str, value: &str) -> Result<bool, Error> {
    validate(pattern)?;
    let pattern: Vec<&str> = pattern.split('/').collect();
    let value: Vec<&str> = if value.is_empty() {
        Vec::new()
    } else {
        value.split('/').collect()
    };
    Ok(match_parts(&pattern, &value))
}

fn match_parts(pattern: &[&str], value: &[&str]) -> bool {
    match (pattern, value) {
        ([], []) => true,
        (["**"], _) => true,
        ([], _) | (_, []) => false,
        (["**", rest @ ..], value) => {
            if rest.is_empty() {
                return true;
            }
            (0..=value.len()).any(|i| match_parts(rest, &value[i..]))
        }
        ([pattern, rest @ ..], [part, value @ ..]) => {
            match_component(pattern, part) && match_parts(rest, value)
        }
    }
}

fn match_component(pattern: &str, value: &str) -> bool {
    Pattern::new(pattern).is_ok_and(|pattern| pattern.matches(value))
}

fn validate(pattern: &str) -> Result<(), Error> {
    for part in pattern.split('/') {
        if part == "**" {
            continue;
        }
        Pattern::new(part).map_err(|error| Error(error.to_string()))?;
    }
    Ok(())
}

fn normalize(value: &str) -> String {
    value.replace('\\', "/").trim_matches('/').to_string()
}

fn positive(value: &str, method: &str) -> Result<u32, Error> {
    let count: u32 = value
        .parse()
        .map_err(|_| Error(format!("{method} needs a positive integer, got `{value}`")))?;
    if count == 0 {
        return Err(Error(format!("{method} needs a positive integer, got 0")));
    }
    Ok(count)
}
