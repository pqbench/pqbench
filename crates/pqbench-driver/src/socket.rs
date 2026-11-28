//! Where the driver's socket lives.
//!
//! The socket sits beside the store it serves — `{store}/driver.sock` — so a
//! client that knows the store finds the driver without an environment
//! variable. `PQB_DRIVER_SOCK` overrides it for a sidecar or a test.

use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

/// The environment variable that names the driver's socket.
pub const ENV: &str = "PQB_DRIVER_SOCK";

/// Linux's `sun_path` holds 108 bytes; keep a margin for the NUL and a rename.
const SOCKET_PATH_LIMIT: usize = 100;

/// The socket `PQB_DRIVER_SOCK` names, if it is set.
pub fn from_env() -> Option<PathBuf> {
    std::env::var_os(ENV).map(|path| absolute(Path::new(&path)))
}

/// The socket beside the store at `store`.
pub fn default_path(store: &Path) -> PathBuf {
    absolute(store).join("driver.sock")
}

/// The driver's socket: an explicit `socket`, else `PQB_DRIVER_SOCK`, else the
/// one beside `store`.
pub fn resolve(socket: Option<&Path>, store: Option<&Path>) -> Result<PathBuf, String> {
    match (socket, from_env(), store) {
        (Some(socket), ..) => Ok(absolute(socket)),
        (None, Some(from_env), _) => Ok(from_env),
        (None, None, Some(store)) => Ok(default_path(store)),
        (None, None, None) => Err(format!(
            "no driver socket: pass --socket, --store, or set {ENV}"
        )),
    }
}

/// Create the socket's directory with owner-only access, and clear a stale
/// socket left by a listener that was killed. A live listener is an error.
/// `label` names the listener in the messages; the daemon and the driver share
/// the security rules and differ only in what they call themselves.
pub fn prepare(socket: &Path, label: &str) -> Result<(), String> {
    check_length(socket)?;
    let dir = socket
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .ok_or_else(|| format!("{} has no parent directory", socket.display()))?;
    ensure_dir(dir)?;
    if socket.exists() {
        if std::os::unix::net::UnixStream::connect(socket).is_ok() {
            return Err(format!(
                "a {label} is already listening on {}",
                socket.display()
            ));
        }
        std::fs::remove_file(socket)
            .map_err(|error| format!("cannot remove stale {}: {error}", socket.display()))?;
    }
    Ok(())
}

/// An absolute socket path: a relative one resolves against the current
/// directory, the way `PQB_DRIVER_SOCK` does.
pub fn absolute(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|dir| dir.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }
}

/// The socket's mode: only its owner may connect.
pub fn set_socket_mode(socket: &Path) -> Result<(), String> {
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))
        .map_err(|error| format!("cannot set {} to 0600: {error}", socket.display()))
}

/// Check the path fits a socket address, so a deep `--store` path fails with a
/// hint rather than the OS's raw `bind` error.
fn check_length(socket: &Path) -> Result<(), String> {
    let length = socket.as_os_str().as_bytes().len();
    if length < SOCKET_PATH_LIMIT {
        return Ok(());
    }
    Err(format!(
        "the socket path {} is {length} bytes; a Unix socket path must be under \
         {SOCKET_PATH_LIMIT} bytes (use a shallower --store directory, or set {ENV})",
        socket.display()
    ))
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

fn uid() -> u32 {
    // SAFETY: `getuid` has no preconditions and cannot fail.
    unsafe { libc::getuid() }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An explicit socket wins, and no socket at all is an error naming the
    /// three ways to say it.
    #[test]
    fn an_explicit_socket_wins_and_no_socket_is_an_error() {
        let resolved = resolve(Some(Path::new("/tmp/explicit.sock")), Some(Path::new("/s")));
        assert_eq!(resolved.unwrap(), PathBuf::from("/tmp/explicit.sock"));
        let saved = std::env::var_os(ENV);
        std::env::remove_var(ENV);
        let error = resolve(None, None).unwrap_err();
        if let Some(saved) = saved {
            std::env::set_var(ENV, saved);
        }
        assert!(error.contains("--socket") && error.contains("--store"));
        assert!(error.contains(ENV));
    }

    /// The default socket is absolute, because the store may be relative.
    #[test]
    fn the_default_socket_is_absolute() {
        let path = default_path(Path::new("store"));
        assert!(path.is_absolute());
        assert_eq!(path.file_name().unwrap(), "driver.sock");
    }

    /// A path too deep for `sun_path` is refused with the limit and the ways to
    /// shorten it, before the OS sees it.
    #[test]
    fn a_socket_path_that_does_not_fit_sun_path_is_refused() {
        let deep = PathBuf::from(format!("/tmp/{}/driver.sock", "d".repeat(200)));
        let error = prepare(&deep, "driver").unwrap_err();
        assert!(error.contains("Unix socket path"), "{error}");
        assert!(error.contains("--store"), "{error}");
        assert!(error.contains(ENV), "{error}");
    }
}
