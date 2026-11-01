//! Every `docscheck` integration test, as one Cargo test target.

#[path = "console.rs"]
mod console;

#[path = "generate.rs"]
mod generate;

#[path = "markdown.rs"]
mod markdown;

#[path = "ndjson.rs"]
mod ndjson;

#[path = "plan.rs"]
mod plan;
