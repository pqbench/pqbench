//! `docscheck` — turn the commands in Markdown into Rust tests.
//!
//! The tool has the same shape as its sibling `aipnaming`: a tree-sitter front
//! end ([`markdown`]) fills a backend-neutral model ([`model`]), and a generator
//! ([`generate`]) turns that model into Rust integration tests, one file per
//! Markdown document.
//!
//! A block is generated only when it is a `console` transcript whose fence
//! carries the `run` word, so prose examples with placeholder paths stay
//! documentation. A `$ ` line is a command; the lines under it are its expected
//! stdout, and a lone `...` matches any run of lines:
//!
//! ~~~text
//! ```console run
//! $ pqbench bytemass examples/quickstart.parquet --json
//! {"kind":"pqbench.bytemass","version":1,"event":"begin"}
//! ...
//! ```
//! ~~~
//!
//! A further word gates the test on a cargo feature (` ```console run delta `).
//! Directives on a leading `#`-comment line set the working directory
//! (`# docscheck: cd: PATH`) or the environment (`# docscheck: env: KEY=VALUE`).
//!
//! The generated tests live in `crates/pqbench-cli/tests/gen_*.rs` and call a
//! committed `gen_support.rs` that resolves `pqbench` to the built binary, so
//! `cargo test` runs them like any other integration test.

pub mod console;
pub mod generate;
pub mod markdown;
pub mod model;
pub mod plan;

pub use console::{invokes_pqbench, untested_pqbench_blocks};
pub use generate::{generate, has_runnable};
pub use markdown::MarkdownParser;
pub use model::{Block, BlockInfo, Directive};
pub use plan::{block_steps, Plan, Step};
