# Issue 80: investigation and implementation drafts

Reviewed 2026-10-01 against upstream main `1b4e43c`, with parquet-rs 59.3.0.
Source: [issue #80](https://github.com/pqbench/pqbench/issues/80).
These are design drafts, not implemented features or reproduced production
benchmarks. The production files were not provided; their measured savings,
memory figures, and Photon-specific behavior remain unverified.

| Point | Assessment | Draft |
| --- | --- | --- |
| 1 | Valid gap; page observations do not prove writer configuration | [Page headers](01-page-headers.md) |
| 2 | Valid, partly available as column-chunk facts already | [File metadata](02-file-metadata.md) |
| 3 | Confirmed correctness problem; Arrow alone does not bound memory | [Schema-preserving experiments](03-schema-preservation.md) |
| 4 | Valid; safe casts need explicit semantic contracts | [Column rewrites and casts](04-column-rewrites.md) |
| 5 | Valid needs; exact source reconstruction generally impossible | [Baselines and execution](05-baselines-and-execution.md) |
| 6 | Valid; sample facts cannot establish whole-table cast safety | [Precision facts](06-profile-facts.md) |
| 7 | Valid; stream comparison fits the existing command composition | [Byte deltas](07-diff.md) |
| 8 | Confirmed gaps; recipes require engine/version qualifications | [Inputs, selection, recipes](08-inputs-and-recipes.md) |

## Evidence and scope

Source inspection followed the CLI through the public library APIs and the
isolated Parquet/object-storage implementations. The older starting branch,
`feat/delta-stats`, lacks experiment/profile and is not the review baseline.
In particular, its file-selection flags must not be confused with current
main's catalog/table-name filters.

Important additional findings in `third_party/parquet/impl.rs::field_value`:
decimals pass through `f64`, oversized unsigned integers saturate to
`i64::MAX`, and logical annotations are lost. In
`experiment.rs::cast_value`, integer-to-double can round and float-to-integer
can saturate. Treat these as correctness prerequisites to trustworthy tuning.

## Delivery order

1. Correct the misleading experiment help and document present limitations
   (point 5); prevent silent value changes (points 3/4).
2. Add footer facts and independent byte-stream comparison (points 2/7).
3. Introduce schema-preserving experiment I/O (point 3), then column overrides,
   exact casts, typed profiles, and comparable baselines (points 4/6/5).
4. Add the optional page scan (point 1) after resolving the upstream API gap.
5. Deliver input selection and verified recipes (point 8); add parallel trials
   only after defining memory budgets and cost measurement (point 5).

Each numbered draft is committed separately. Its acceptance criteria describe
future blackbox tests, not tests added by this review. Keep any prerequisite
structural tidyings in separate behavior-preserving commits. Preserve the
thin CLI and third-party isolation; benchmark dependency/build cost before
enabling Arrow on the default build. Run `make check` on every implementation
commit and feature-specific checks where relevant.

## Validation performed

`make check` passed for this documentation-only review on the default feature
set, including documentation consistency, clippy, isolation, LFS integrity,
and workspace tests. The first sandboxed attempt failed because existing
catalog tests bind localhost sockets; the complete gate passed when rerun
with the required permissions. No test was disabled.

The built CLI's experiment help reproduces the stale exclusions. An eight-row
run on the checked-in Reddit fixture accepts both `encoding:delta_length` and
`page-size:1024`, emitting a control and both trials with completion records.
Their sizes happened to equal the control; accepting a setting does not prove
it changes the resulting pages (dictionary encoding can take precedence).
An incompatible `encoding:delta` correctly reports that no column applies.
Production performance claims and external-engine recipes were not executed.
