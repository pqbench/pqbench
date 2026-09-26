//! The Snappy implementation, backed by the C library via `snappy_src`.
//!
//! This module is private; callers use [`super::api`]. It is the only file that
//! names `snappy_src`.

use super::api::Error;

/// Compress `src`, sizing the output from the C library's worst case.
pub(crate) fn compress(src: &[u8]) -> Result<Vec<u8>, Error> {
    let max_len = unsafe { snappy_src::snappy_max_compressed_length(src.len()) };
    let mut dst = Vec::with_capacity(max_len);
    let mut len = max_len;
    let status = unsafe {
        snappy_src::snappy_compress(
            src.as_ptr().cast(),
            src.len(),
            dst.spare_capacity_mut().as_mut_ptr().cast(),
            &mut len,
        )
    };
    if status != snappy_src::snappy_status_SNAPPY_OK {
        return Err(Error("compress failed".to_string()));
    }
    unsafe { dst.set_len(len) };
    Ok(dst)
}

/// Decompress `src` into a buffer of `out_cap` bytes.
pub(crate) fn decompress(src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
    let mut dst = Vec::with_capacity(out_cap);
    let mut len = out_cap;
    let status = unsafe {
        snappy_src::snappy_uncompress(
            src.as_ptr().cast(),
            src.len(),
            dst.spare_capacity_mut().as_mut_ptr().cast(),
            &mut len,
        )
    };
    if status != snappy_src::snappy_status_SNAPPY_OK {
        return Err(Error("decompress failed".to_string()));
    }
    unsafe { dst.set_len(len) };
    Ok(dst)
}
