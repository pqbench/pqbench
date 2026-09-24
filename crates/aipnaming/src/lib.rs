//! AIP naming linter for Rust source.
//!
//! The tool is split into a backend-neutral declaration model ([`decl`]) and a
//! tree-sitter front end ([`rust`]) that fills it. Rule modules (added on top)
//! check the model against the Google API Improvement Proposal naming
//! conventions as adapted for Rust libraries by the workspace AGENTS.md
//! (AIP-126/136/140/141/142/190).
//!
//! The rule set is seeded from Google's `api-linter` (Apache-2.0), which
//! applies the same AIPs to protobuf descriptors; rule ids mirror its
//! `aip-<proposal>/<rule>` naming so every finding maps back to a published
//! rule page.
//!
//! Two Rust facts shape the adaptation. Enum variants stay UpperCamelCase in
//! existing Rust code, so AIP-126 casing is available but not on by default;
//! and type information comes from the source as written rather than from a
//! type checker, so rules that would need a full type (repeated fields, bool
//! fields behind aliases) are advisory by design.

mod data;
pub mod decl;
pub mod lint;
mod rules;
pub mod rust;
pub mod words;
