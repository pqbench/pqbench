//! Check the PyO3 stub against the bindings it mirrors.
//!
//! The wheel's types are hand-written, so they can drift from the Rust that
//! produces them; this tool reads both, fills one surface model, and reports
//! where they disagree. It is a sibling of `aipnaming`: a tree-sitter front end
//! over a backend-neutral model, a rule set over the model, and a CLI that
//! renders findings. Where `aipnaming` reads one file, this compares two.
//!
//! Two front ends fill the model: [`rust`] reads a binding source (the
//! `#[pyfunction]`, `#[pyclass]`, and the registration calls that name the
//! Python path), and [`python`] reads the stub. [`library`] adds the serde
//! models, so a record's fields are checked against the struct that serializes
//! them.
//!
//! What it recovers that a type checker cannot: PyO3 erases a command's return
//! type and awaitability at the boundary (`Py<PyAny>`), so the wrapper the
//! binding calls — `one`, `many`, `stream_listing`, `document` — is the only
//! statement of them. The return-shape and async rules read that wrapper and
//! hold the stub to it.

pub mod library;
pub mod lint;
pub mod model;
pub mod python;
pub mod rust;

mod rules;
mod source;
