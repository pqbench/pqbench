//! `docscheck` — turn the commands in Markdown into Rust tests.
//!
//! The tool has the same shape as its sibling `aipnaming`: a tree-sitter front
//! end ([`markdown`]) fills a backend-neutral model ([`model`]), and a generator
//! ([`generate`]) turns that model into Rust integration tests, one file per
//! Markdown document.
//!
//! A block is generated only when its opening fence carries the `run` word, so
//! illustrative examples with placeholder paths stay documentation:
//!
//! ~~~text
//! ```sh run
//! pqbench bytemass examples/quickstart.parquet
//! ```
//! ~~~
//!
//! The fence's first word is the language (`sh`, `bash`); a later word may be
//! `run` or `no-run`. Directives on a leading `#`-comment line set the working
//! directory (`# docscheck: cd: PATH`) or the environment
//! (`# docscheck: env: KEY=VALUE`).
//!
//! The generated tests live in `crates/pqbench-cli/tests/gen/` and call a
//! committed `support.rs` that resolves `pqbench` to the built binary, so
//! `cargo test` runs them like any other integration test.

pub mod generate;
pub mod markdown;
pub mod model;
pub mod plan;

pub use generate::{generate, has_runnable};
pub use markdown::MarkdownParser;
pub use model::{Block, BlockInfo, Directive};
pub use plan::{block_steps, Plan, Step};
