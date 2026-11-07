//! Where the daemon's socket lives.
//!
//! The path is derived from the resolved identity — endpoint, dialect, and a
//! fingerprint of the token — so a client can find the daemon without an
//! environment variable, and two principals on the same workspace never share
//! one. `PQB_DAEMON_SOCK` overrides it for a sidecar or a test.

use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use pqbench::Config;

/// Linux's `sun_path` holds 108 bytes; keep a margin for the NUL and a rename.
const SOCKET_PATH_LIMIT: usize = 100;

/// The socket for `config`'s identity, or `PQB_DAEMON_SOCK`.
pub(crate) fn socket_path(config: &Config) -> PathBuf {
    if let Some(path) = std::env::var_os("PQB_DAEMON_SOCK") {
        return absolute(Path::new(&path));
    }
    let name = format!("d-{:016x}.sock", fingerprint(config));
    let mut candidates = Vec::with_capacity(3);
    if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        candidates.push(PathBuf::from(dir).join("pqbench"));
    }
    if let Some(dir) = std::env::var_os("TMPDIR") {
        candidates.push(PathBuf::from(dir).join(format!("pqbench-{}", uid())));
    }
    candidates.push(PathBuf::from(format!("/tmp/pqbench-{}", uid())));
    let paths: Vec<PathBuf> = candidates.into_iter().map(|dir| dir.join(&name)).collect();
    if let Some(path) = paths.iter().find(|path| fits(path)) {
        return path.clone();
    }
    paths
        .into_iter()
        .min_by_key(|path| socket_length(path))
        .unwrap_or_else(|| PathBuf::from(format!("/tmp/pqbench-{}/{}", uid(), name)))
}

/// Whether the path fits a socket address.
fn fits(path: &Path) -> bool {
    socket_length(path) < SOCKET_PATH_LIMIT
}

fn socket_length(path: &Path) -> usize {
    path.as_os_str().as_bytes().len()
}

/// A stable fingerprint of the identity the daemon serves.
fn fingerprint(config: &Config) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for part in [
        config.endpoint.as_str(),
        config.dialect.as_str(),
        config.token.as_deref().unwrap_or_default(),
    ] {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

fn uid() -> u32 {
    // SAFETY: `getuid` has no preconditions and cannot fail.
    unsafe { libc::getuid() }
}

/// Create the socket's directory with owner-only access, and clear a stale
/// socket left by a daemon that was killed. A live listener is an error.
pub(crate) fn prepare(socket: &Path) -> Result<(), String> {
    let dir = socket
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .ok_or_else(|| format!("{} has no parent directory", socket.display()))?;
    ensure_dir(dir)?;
    if socket.exists() {
        if std::os::unix::net::UnixStream::connect(socket).is_ok() {
            return Err(format!(
                "a daemon is already listening on {}",
                socket.display()
            ));
        }
        std::fs::remove_file(socket)
            .map_err(|error| format!("cannot remove stale {}: {error}", socket.display()))?;
    }
    Ok(())
}

/// An absolute socket path: a relative one resolves against the current
/// directory, the way `PQB_DAEMON_SOCK` does.
pub(crate) fn absolute(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|dir| dir.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }
}

/// The socket's mode: only its owner may connect.
pub(crate) fn set_socket_mode(socket: &Path) -> Result<(), String> {
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))
        .map_err(|error| format!("cannot set {} to 0600: {error}", socket.display()))
}

fn ensure_dir(dir: &Path) -> Result<(), String> {
    if !dir.exists() {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .map_err(|error| format!("cannot create {}: {error}", dir.display()))?;
    }
    // Do not follow a planted symlink, and do not chmod a directory the caller
    // already had (a socket override can live under $HOME).
    let metadata = std::fs::symlink_metadata(dir)
        .map_err(|error| format!("cannot stat {}: {error}", dir.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(format!("{} is a symlink", dir.display()));
    }
    if !metadata.is_dir() {
        return Err(format!("{} is not a directory", dir.display()));
    }
    if metadata.uid() != uid() && metadata.mode() & 0o1000 == 0 {
        // A shared temp directory with the sticky bit (the usual `/tmp`) is
        // safe to bind in; a private parent must belong to this user.
        return Err(format!("{} is not owned by this user", dir.display()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pqbench::Dialect;

    fn config(token: &str) -> Config {
        Config {
            endpoint: "https://example.cloud.databricks.com".into(),
            token: Some(token.into()),
            dialect: Dialect::Unity,
            request_timeout: None,
        }
    }

    #[test]
    fn the_fingerprint_is_stable_and_identity_specific() {
        assert_eq!(fingerprint(&config("a")), fingerprint(&config("a")));
        assert_ne!(fingerprint(&config("a")), fingerprint(&config("b")));
        let mut other = config("a");
        other.dialect = Dialect::Iceberg;
        assert_ne!(fingerprint(&config("a")), fingerprint(&other));
    }
}
