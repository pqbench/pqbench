//! Every `aipnaming` integration test, as one Cargo test target.

#[path = "backend.rs"]
mod backend;

#[path = "rules.rs"]
mod rules;

#[path = "words.rs"]
mod words;
