//! Source discovery and parallel linting, kept apart from argument handling.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use aipnaming::lint::{Finding, Linter, Options};

/// What a lint run over a set of paths produced.
pub(crate) struct Report {
    pub findings: Vec<(PathBuf, Finding)>,
    pub read_errors: Vec<(PathBuf, String)>,
}

/// The `.rs` files under the given inputs, sorted.
///
/// Directories are searched recursively; hidden entries and `target` are
/// skipped. A missing input path is an error, not an empty result.
pub(crate) fn rust_files(inputs: &[PathBuf]) -> io::Result<Vec<PathBuf>> {
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
        } else if root.extension().is_some_and(|extension| extension == "rs") {
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
            if name != "target" {
                collect_dir(&path, out)?;
            }
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
    Ok(())
}

/// Lint every file, spread over worker threads, each with its own parser.
pub(crate) fn lint_files(files: &[PathBuf], options: Options) -> Report {
    let next = AtomicUsize::new(0);
    let report = Mutex::new(Report {
        findings: Vec::new(),
        read_errors: Vec::new(),
    });
    let workers = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(files.len().max(1));

    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                let mut linter = Linter::new(options.clone());
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(path) = files.get(index) else {
                        break;
                    };
                    match fs::read_to_string(path) {
                        Ok(source) => {
                            let findings = linter.lint(&source);
                            if !findings.is_empty() {
                                let mut report = report.lock().expect("report mutex");
                                report.findings.extend(
                                    findings.into_iter().map(|finding| (path.clone(), finding)),
                                );
                            }
                        }
                        Err(error) => {
                            let mut report = report.lock().expect("report mutex");
                            report.read_errors.push((path.clone(), error.to_string()));
                        }
                    }
                }
            });
        }
    });

    let mut report = report.into_inner().expect("report mutex");
    report
        .findings
        .sort_by(|(left_path, left), (right_path, right)| {
            (left_path, left.line, left.column, left.rule).cmp(&(
                right_path,
                right.line,
                right.column,
                right.rule,
            ))
        });
    report
}
