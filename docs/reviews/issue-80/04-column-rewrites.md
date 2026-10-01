# 4. Column selectors and exact casts

## Verdict and evidence

Valid. `experiment.rs::TrialSpec` and the adapter's `WriteOptions` have global
dictionary/encoding settings. Existing encoding selection skips incompatible
kinds; it cannot isolate three nested timestamp leaves. Existing casts target
top-level names and only int64/double/string. They are not safe-cast proofs:
large integer-to-double conversion rounds, and an integral out-of-range float
can saturate when converted to i64.

The reported encoding win is plausible but data-specific. Disabling dictionary
and choosing DELTA_BINARY_PACKED are separate writer decisions. A timestamp
stored as INT96 needs an explicit physical conversion before this encoding
can apply; selecting a logical timestamp alone is insufficient.

## Proposed contract

Retain global syntax. Add per-leaf forms such as
`dictionary:path=off` and `encoding:path=delta_binary_packed`, with explicit
selector forms `path:...`, `glob:...`, and `physical:INT64` in the typed API.
Finalize the CLI grammar with escaping for literal dots, colons, equals signs,
semicolons, and wildcard characters before implementing it. Represent paths
as segments internally; flattened dotted strings can be ambiguous.

Resolve all selectors against the original schema before writing. Apply
global defaults, then type selectors, globs, exact paths. Reject contradictory
equal-priority assignments, unmatched selectors, and explicitly selected
incompatible encodings. Report the resolved effective per-leaf policy. Check
encoding compatibility against the post-cast physical type. Map it to the
writer's per-column property setters inside the adapter.

## Exact-cast rules

- Numeric conversions require representability, finite/range checks, and an
  exact round trip. Do not use a saturating Rust `as` as validation.
- Decimal narrowing operates on the unscaled integer: discarded digits must
  be zero and target precision must hold. Never validate through `f64`.
- Timestamp coarsening requires divisibility of every value by the unit factor;
  finer units require checked multiplication. Preserve UTC/local semantics.
  INT96 needs an explicit interpretation and representable date range.
- UUID text to 16 bytes is a schema change. Exact text restoration requires a
  canonical spelling rule (case, hyphens); arbitrary spellings are not lossless
  strings. Make that policy explicit and reject noncanonical text in strict
  mode. Reader compatibility remains a separate requirement.

Validate all selected values before committing any rewrite artifact; include
path, row location, and reason in errors. Success applies only to sampled rows,
not the whole source table. Nulls are preserved. Depend on point 3 for exact
typed input and nested traversal.

## Acceptance criteria

Public experiments must show only selected leaves changed, matching decoded
values elsewhere. Test selector overlap/escaping/no-match, INT96 incompatibility,
integers around 2^53 and i64 bounds, NaN/infinity, negative timestamp remainders,
decimal overflow/rescaling, UUID spellings, nulls, and invalid nested elements.
Read written values independently and require explicit rejection on loss.
