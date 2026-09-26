//! `docscheck` — turn the commands in Markdown into Rust tests.
//!
//! `sync` writes one generated test file per Markdown document into
//! `--out` (default `crates/pqbench-cli/tests`) as `gen_<path>.rs`, plus the
//! shared `gen_support.rs`. `check` verifies those files are up to date and
//! exits non-zero when they are not, so CI can gate on the generated tests
//! matching the docs.
//!
//! A fenced block becomes a test only when its info string carries `run`:
//!
//! ```sh run
//! pqbench bytemass examples/quickstart.parquet
//! ```

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use docscheck::markdown::MarkdownParser;

mod walk;

/// The committed support module, written next to the generated tests.
const SUPPORT: &str = include_str!("support.rs.template");

/// The file name the support module is written as.
const SUPPORT_NAME: &str = "gen_support.rs";

#[derive(Parser)]
#[command(
    name = "docscheck",
    about = "Turn the commands in Markdown into Rust tests",
    after_help = r#"
A fenced block is generated only when its info string carries `run`:

    ```sh run
    pqbench bytemass examples/quickstart.parquet
    ```

The first info word is the language (sh, bash); a later word may be `run` or
`no-run`. One block becomes one `#[test]`; the test runs the block's commands
through a shared `support` module that resolves `pqbench` to the binary under
test. A `# docscheck: cd: PATH` or `# docscheck: env: KEY=VALUE` line on the
first body line sets the directory or environment.

Examples:
  docscheck sync README.md docs/
  docscheck check README.md docs/        # freshness gate for CI
  docscheck emit README.md               # print one file, no writing
"#
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write the generated tests (and `gen_support.rs`) into `--out`.
    Sync(Args),
    /// Fail if the generated tests are stale; write nothing.
    Check(Args),
    /// Print one document's generated test to stdout.
    Emit {
        /// The Markdown file to emit.
        path: PathBuf,
        /// The repository root the generated commands run from.
        #[arg(long, value_name = "DIR")]
        root: Option<PathBuf>,
    },
}

#[derive(clap::Args)]
struct Args {
    /// Files or directories to read (default: the current directory).
    paths: Vec<PathBuf>,
    /// Directory to write the generated `gen_*.rs` files into.
    #[arg(long, value_name = "DIR", default_value = "crates/pqbench-cli/tests")]
    out: PathBuf,
    /// The repository root the generated commands run from (for `cd:`).
    #[arg(long, value_name = "DIR")]
    root: Option<PathBuf>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Sync(args) => run_sync(args, false),
        Command::Check(args) => run_sync(args, true),
        Command::Emit { path, root: _ } => run_emit(&path),
    }
}

/// A generated file: its output name and contents.
struct Generated {
    name: String,
    contents: String,
}

/// Generate every file for the given inputs, or report what is stale.
fn run_sync(args: Args, check_only: bool) -> ExitCode {
    let files = match walk::markdown_files(&args.paths) {
        Ok(files) => files,
        Err(error) => {
            eprintln!("docscheck: {error}");
            return ExitCode::from(2);
        }
    };

    let generated = match generate_all(&files) {
        Ok(generated) => generated,
        Err(error) => {
            eprintln!("docscheck: {error}");
            return ExitCode::from(2);
        }
    };

    if check_only {
        return check_freshness(&args.out, &generated);
    }
    match write_all(&args.out, &generated) {
        Ok(()) => {
            eprintln!(
                "docscheck: wrote {} file(s) into {}",
                generated.len() + 1,
                args.out.display()
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("docscheck: cannot write {}: {error}", args.out.display());
            ExitCode::from(2)
        }
    }
}

/// Generate the test files for every Markdown input, sorted by output name.
///
/// Documents with no runnable block are skipped: no empty test target.
fn generate_all(files: &[PathBuf]) -> io::Result<Vec<Generated>> {
    let mut parser = MarkdownParser::new();
    let mut generated = Vec::new();
    let mut names = BTreeSet::new();
    for path in files {
        let source = fs::read_to_string(path)?;
        let blocks = parser.parse(&source);
        if !docscheck::has_runnable(&blocks) {
            continue;
        }
        let name = output_name(path);
        let contents = docscheck::generate(&path.display().to_string(), &blocks);
        if names.insert(name.clone()) {
            generated.push(Generated { name, contents });
        }
    }
    generated.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(generated)
}

/// The generated file name for a Markdown path: `docs/demo.md` -> `gen_docs_demo.rs`.
///
/// The `gen_` prefix both groups the files and tells Cargo to pick each up as
/// its own integration-test target (Cargo only auto-discovers `tests/*.rs`).
///
/// The path is interpreted relative to the current directory, so an absolute
/// path under it collapses to the same name as the relative one.
fn output_name(path: &Path) -> String {
    let relative = path
        .strip_prefix(std::env::current_dir().unwrap_or_default())
        .unwrap_or(path);
    let stem = relative.with_extension("").to_string_lossy().into_owned();
    let slug: String = stem
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect();
    format!("gen_{}.rs", slug.trim_matches('_'))
}

/// Write the generated files and `gen_support.rs`, creating `out` if needed.
///
/// A previously generated file that is no longer produced (the document lost
/// its `run` blocks, or was removed) is deleted, so the directory always matches
/// the docs.
fn write_all(out: &Path, generated: &[Generated]) -> io::Result<()> {
    fs::create_dir_all(out)?;
    fs::write(out.join(SUPPORT_NAME), formatted(SUPPORT))?;
    let keep: BTreeSet<&str> = generated.iter().map(|file| file.name.as_str()).collect();
    for entry in fs::read_dir(out)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // Only the generated files are ours to remove; `tests/` also holds
        // hand-written integration tests.
        if is_generated(&name) && name != SUPPORT_NAME && !keep.contains(name.as_ref()) {
            fs::remove_file(entry.path())?;
        }
    }
    for file in generated {
        fs::write(out.join(&file.name), formatted(&file.contents))?;
    }
    Ok(())
}

/// Format generated source with `rustfmt`, so `make fmt-check` is a no-op.
///
/// The emitted source is valid Rust already; rustfmt makes it canonical (line
/// wrapping, argument layout) without the generator tracking rustfmt's rules.
/// A missing or failing `rustfmt` returns the source unchanged.
fn formatted(source: &str) -> String {
    use std::io::Write as _;
    use std::process::{Command, Stdio};

    let Ok(mut child) = Command::new("rustfmt")
        .arg("--edition")
        .arg("2021")
        .arg("--emit")
        .arg("stdout")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    else {
        return source.to_owned();
    };
    if child
        .stdin
        .take()
        .expect("stdin is piped")
        .write_all(source.as_bytes())
        .is_err()
    {
        return source.to_owned();
    }
    match child.wait_with_output() {
        Ok(output) if output.status.success() => {
            String::from_utf8(output.stdout).unwrap_or_else(|_| source.to_owned())
        }
        _ => source.to_owned(),
    }
}

/// Whether a file name looks like one of ours: `gen_*.rs`.
fn is_generated(name: &str) -> bool {
    name.starts_with("gen_") && name.ends_with(".rs")
}

/// Whether the on-disk files match what generation would write.
fn check_freshness(out: &Path, generated: &[Generated]) -> ExitCode {
    let mut stale = Vec::new();
    compare(&mut stale, out, SUPPORT_NAME, &formatted(SUPPORT));
    let keep: BTreeSet<&str> = generated.iter().map(|file| file.name.as_str()).collect();
    for file in generated {
        compare(&mut stale, out, &file.name, &formatted(&file.contents));
    }
    if let Ok(entries) = fs::read_dir(out) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if is_generated(&name) && name != SUPPORT_NAME && !keep.contains(name.as_ref()) {
                stale.push(name.into_owned());
            }
        }
    }
    if stale.is_empty() {
        return ExitCode::SUCCESS;
    }
    eprintln!(
        "docscheck: {} generated file(s) out of date; run `docscheck sync`",
        stale.len()
    );
    for name in stale {
        eprintln!("docscheck: stale {name}");
    }
    ExitCode::from(1)
}

fn compare(stale: &mut Vec<String>, out: &Path, name: &str, expected: &str) {
    let path = out.join(name);
    match fs::read_to_string(&path) {
        Ok(actual) if actual == expected => {}
        _ => stale.push(name.to_owned()),
    }
}

/// Print one document's generated test to stdout.
fn run_emit(path: &Path) -> ExitCode {
    let source = match fs::read_to_string(path) {
        Ok(source) => source,
        Err(error) => {
            eprintln!("docscheck: cannot read {}: {error}", path.display());
            return ExitCode::from(2);
        }
    };
    let blocks = MarkdownParser::new().parse(&source);
    print!(
        "{}",
        formatted(&docscheck::generate(&path.display().to_string(), &blocks))
    );
    ExitCode::SUCCESS
}
