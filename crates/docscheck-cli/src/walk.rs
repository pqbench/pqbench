//! Markdown discovery, mirroring `aipnaming-cli`'s source walk.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// The `.md` files under the given inputs, sorted.
///
/// Directories are searched recursively; hidden entries and `target` are
/// skipped, as is anything under `node_modules`. A missing input path is an
/// error, not an empty result.
pub(crate) fn markdown_files(inputs: &[PathBuf]) -> io::Result<Vec<PathBuf>> {
    let roots: Vec<PathBuf> = if inputs.is_empty() {
        vec![PathBuf::from(".")]
    } else {
        inputs.to_vec()
    };
    let mut files = Vec::new();
    for root in roots {
        let metadata = fs::metadata(&root)?;
        if metadata.is_dir() {
            collect_dir(&root, &mut files)?;
        } else if is_markdown(&root) {
            files.push(root);
        }
    }
    files.sort();
    files.dedup();
    Ok(files)
}

fn collect_dir(dir: &Path, out: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            if name != "target" && name != "node_modules" {
                collect_dir(&path, out)?;
            }
        } else if is_markdown(&path) {
            out.push(path);
        }
    }
    Ok(())
}

fn is_markdown(path: &Path) -> bool {
    path.extension().is_some_and(|extension| extension == "md")
}
