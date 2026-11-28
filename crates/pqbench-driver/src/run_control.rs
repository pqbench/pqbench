//! The driver's run-control file: the operator's two knobs, in the INI
//! `pqbench` already reads (`PQB_CONFIG_FILE`, else `PQB_CONFIG`, else
//! `~/.pqbenchrc`).
//!
//! ```ini
//! [DEFAULT]
//! endpoint          = https://dbc-abc123.cloud.databricks.com
//! token             = dapi…
//! step-timeout      = 300
//!
//! [long-walks]
//! step-timeout      = 3600
//! ```
//!
//! The setting changes rarely, so it belongs in the run-control file rather
//! than on the command line, which carries what changes per run. The file is
//! shared with `pqbench` itself, so the keys the driver does not know
//! (`endpoint`, `token`, `table_format`) are ignored, not errors.
//!
//! `[DEFAULT]` applies; `PQB_PROFILE` names another section.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

/// The section that applies when `PQB_PROFILE` names none.
const DEFAULT_SECTION: &str = "DEFAULT";

/// How long one step may run when the file names no `step-timeout`.
const DEFAULT_STEP_TIMEOUT_SECONDS: u64 = 300;

/// What the driver reads from the run-control file.
#[derive(Debug)]
pub(crate) struct RunControl {
    /// How long one worker command may run before it is killed.
    pub step_timeout: Duration,
}

impl Default for RunControl {
    fn default() -> Self {
        RunControl {
            step_timeout: Duration::from_secs(DEFAULT_STEP_TIMEOUT_SECONDS),
        }
    }
}

/// Read the run-control file the driver runs under; a missing file is the
/// defaults.
pub(crate) fn read() -> Result<RunControl, String> {
    let Some(path) = path() else {
        return Ok(RunControl::default());
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RunControl::default());
        }
        Err(error) => return Err(format!("cannot read {}: {error}", path.display())),
    };
    let profile = std::env::var("PQB_PROFILE")
        .ok()
        .filter(|name| !name.trim().is_empty());
    parse(&text, profile.as_deref()).map_err(|error| format!("{}: {error}", path.display()))
}

/// The file's path: `PQB_CONFIG_FILE`, else `PQB_CONFIG`, else `~/.pqbenchrc`.
/// The commands read the same file the same way.
fn path() -> Option<PathBuf> {
    ["PQB_CONFIG_FILE", "PQB_CONFIG"]
        .iter()
        .find_map(|name| std::env::var_os(name).map(PathBuf::from))
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".pqbenchrc")))
}

/// The driver's settings from the `profile` section (or `[DEFAULT]`) of the
/// INI `text`. A key's hyphen and underscore spellings are the same key.
fn parse(text: &str, profile: Option<&str>) -> Result<RunControl, String> {
    let wanted = profile.unwrap_or(DEFAULT_SECTION);
    let mut run_control = RunControl::default();
    let mut section = String::new();
    let mut sections = BTreeSet::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(name) = line
            .strip_prefix('[')
            .and_then(|line| line.strip_suffix(']'))
        {
            section = name.trim().to_string();
            sections.insert(section.clone());
            continue;
        }
        if section != wanted {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = unquote(value.trim());
        if value.is_empty() {
            continue;
        }
        if key.trim().replace('_', "-") == "step-timeout" {
            run_control.step_timeout = Duration::from_secs(seconds(&value)?);
        }
    }
    if profile.is_some() && !sections.contains(wanted) {
        return Err(format!("no profile {wanted:?}"));
    }
    Ok(run_control)
}

/// A positive whole number of seconds.
fn seconds(value: &str) -> Result<u64, String> {
    match value.parse::<u64>() {
        Ok(seconds) if seconds > 0 => Ok(seconds),
        _ => Err(format!(
            "step-timeout expects a positive number of seconds, got {value:?}"
        )),
    }
}

/// A value with matching surrounding quotes removed.
fn unquote(value: &str) -> String {
    let bytes = value.as_bytes();
    let quoted = bytes.len() >= 2
        && (bytes[0] == b'"' || bytes[0] == b'\'')
        && bytes[bytes.len() - 1] == bytes[0];
    if quoted {
        value[1..value.len() - 1].to_string()
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILE: &str = "# a run-control file\n\
                        [DEFAULT]\n\
                        endpoint = https://example.cloud.databricks.com\n\
                        step-timeout = 30\n\
                        \n\
                        [long-walks]\n\
                        step-timeout = 3600\n";

    /// The driver's key parses beside the keys the commands own, and a key it
    /// does not know — an old `checkpoint-codec` — is ignored.
    #[test]
    fn the_run_control_file_reads_the_driver_knob() {
        let text = format!("{FILE}checkpoint-codec = lz4\n");
        let run_control = parse(&text, None).unwrap();
        assert_eq!(run_control.step_timeout, Duration::from_secs(30));
    }

    /// `PQB_PROFILE`'s section applies in place of `[DEFAULT]`, and a knob the
    /// section leaves out keeps its default.
    #[test]
    fn a_profile_section_replaces_the_default_section() {
        let run_control = parse(FILE, Some("long-walks")).unwrap();
        assert_eq!(run_control.step_timeout, Duration::from_secs(3600));
        assert!(parse(FILE, Some("missing")).is_err());
    }

    /// A file with no such key, an empty one, or a missing file leaves the
    /// defaults.
    #[test]
    fn an_unset_knob_keeps_its_default() {
        for text in [
            "",
            "[DEFAULT]\nstep-timeout =\n",
            "[other]\nstep-timeout = 1\n",
        ] {
            let run_control = parse(text, None).unwrap();
            assert_eq!(run_control.step_timeout, Duration::from_secs(300));
        }
    }

    /// A timeout that is not a positive number of seconds fails, naming the
    /// key.
    #[test]
    fn a_bad_timeout_is_an_error() {
        for timeout in ["0", "-1", "soon", "1.5"] {
            let text = format!("[DEFAULT]\nstep-timeout = {timeout}\n");
            let error = parse(&text, None).unwrap_err();
            assert!(error.contains("step-timeout"), "{timeout}: {error}");
        }
    }

    /// The hyphen and underscore spellings of a key are the same key.
    #[test]
    fn keys_accept_either_spelling() {
        let run_control = parse("[DEFAULT]\nstep_timeout = '7'\n", None).unwrap();
        assert_eq!(run_control.step_timeout, Duration::from_secs(7));
    }
}
