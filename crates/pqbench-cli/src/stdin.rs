//! Whether standard input is a stream worth reading.
//!
//! A command reads standard input only when a producer put a stream there: a
//! shell pipeline's pipe, or a `<file` redirect. Every other kind of descriptor
//! — a terminal, a socket, a character device — has no records or never reaches
//! end of input, so a read blocks. A notebook kernel hands its subprocess such
//! a descriptor, so "not a terminal" is not the same as "has input".

use std::fs::File;
use std::os::fd::AsFd;
use std::os::unix::fs::FileTypeExt;

/// Whether standard input carries a producer's records: a pipe or a redirected
/// file. `false` for a terminal, a socket, and a character device, which a
/// command treats as no input — it falls back to its flags instead of blocking
/// on a read that never returns.
pub(crate) fn is_stream() -> bool {
    // Duplicate the descriptor: `File` owns what it is given, and dropping it
    // must not close standard input.
    let Ok(duplicate) = std::io::stdin().as_fd().try_clone_to_owned() else {
        return false;
    };
    let Ok(metadata) = File::from(duplicate).metadata() else {
        return false;
    };
    let kind = metadata.file_type();
    kind.is_fifo() || kind.is_file()
}
