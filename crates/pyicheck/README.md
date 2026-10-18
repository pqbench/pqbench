# pyicheck

Check the PyO3 stub against the bindings it mirrors. The wheel's types
(`python/pqbench/native.pyi`) are hand-written, so they can drift from the Rust
that produces them (`python/src`) and the models that serialize them
(`crates/pqbench/src`). This tool reads all three into one surface model and
reports where they disagree.

It is a sibling of [`aipnaming`](../aipnaming): a tree-sitter front end over a
backend-neutral model (`src/model.rs`), a rule set over the model
(`src/rules.rs`), and a CLI that renders findings. Where `aipnaming` reads one
file, this compares two surfaces plus the library's models.

## Why it exists

PyO3 erases what a reader most needs to check. A command is a
`fn(...) -> PyResult<Py<PyAny>>` that returns a coroutine, so neither the return
type nor the awaitability is in the signature; the stub is the only place they
are written down. The wrapper the binding calls — `one`, `many`,
`stream_listing`, `document` — is the last statement of them, so `pyicheck`
recovers the return shape and awaitability from it and holds the stub to that.

`mypy`'s `stubtest` is the usual drift guard, but it does not handle PyO3's
nested submodules, which are how the sibling bindings write a verb's module
tree. This checks the stub against the source instead, so it needs no running
interpreter and fills that gap.

## Rules

| Rule | Checks |
| --- | --- |
| `surface/parity` | Every name on one side exists on the other (a missing verb, a stray class) |
| `surface/params` | A function's parameters agree, name for name and default for default |
| `surface/returns` | The return shape agrees (`Table`, `list[Table]`, `AsyncIterator[TableFile]`, `dict[str, Any]`) |
| `surface/async` | The stub's `async def` agrees with the binding's awaitability |
| `surface/method` | A class's public methods (and `__init__`) exist on both sides |
| `surface/method-params` | A shared method's parameters agree — `__init__`, built from `#[new]` and `#[pyo3(signature)]`, most of all |
| `model/base` | A class a command returns is declared `(Record)` in the stub, and a `(Record)` class is one a command returns |
| `model/field` | A record's fields match a `pub struct` of the same name in the library |
| `model/optionality` | A field the model skips when empty (`skip_serializing_if = "Option::is_none"`) is `| None` in the stub, and only that field |

The field rule matches a record against *any* model of its name, because a name
can be declared twice (the wire `v1::TableFile` and the streaming
`table::TableFile`); the stub has to match one, not all.

## Running it

```sh
cargo run -p pyicheck-cli                       # the repo's own wheel
cargo run -p pyicheck-cli -- --rule surface/returns
cargo run -p pyicheck-cli -- --output-format github   # PR annotations
cargo run -p pyicheck-cli -- --list-rules
```

Inputs default to the wheel's layout (`--bindings python/src`, `--stub
python/pqbench/native.pyi`, `--library crates/pqbench/src`). Findings print as
`file:line:column: severity[rule]: message`; exit status is 0 when clean, 1 when
findings exist, and 2 for usage or io errors. `make check-pyi` runs it, and the
`pyicheck` CI job gates on it.

## Limits

Syntax, not types. The Rust front end reads the source as written, so a wrapper
behind an alias, a command whose return shape is not one of the four wrappers,
and any behaviour that only a running module shows are out of reach. It
complements `stubtest`, it does not replace it.

Dunders that PyO3 generates from a `#[pyclass]` flag are not compared — a stub
does not spell out the `__eq__` an `eq` pyclass gets — so only public methods
and `__init__` are checked for existence. A class the library returns but the
bindings never `attach` (a plain `dict` document, a scalar) has no model to
check against and is skipped.
