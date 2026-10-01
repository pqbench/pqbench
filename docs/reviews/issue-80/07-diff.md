# 7. Per-column byte deltas

## Verdict and evidence

Valid. There is no diff command in `pqbench-cli/src/main.rs`. Existing
bytemass streams already carry per-file/per-row-group leaf paths and byte
counts; a comparison can compose those facts without decoding values.
`bytemass/aggregate.rs` provides related aggregation, but comparison policy
should remain a separate domain module.

## Proposed contract

A new diff command accepts two file paths or saved bytemass streams. Read
file inputs through bytemass's footer API, then use the same aggregation as
stream inputs. Auto-detection must use the existing document conventions;
at most one input may be stdin. Support the existing saved lz4 stream format.

Emit versioned `diff-column` records with path segments, left/right byte
totals, signed byte delta (right minus left), percentage change, left/right
rows and bytes per row, and ADDED/REMOVED/CHANGED/UNCHANGED status. Avoid an
unchecked u64-to-i64 cast: choose checked wider signed arithmetic or a
sign/magnitude wire representation with a documented range. Percentage is
absent when the left baseline is zero; a missing path is distinct from an
existing zero-byte path.

Default alignment uses full leaf paths for a single table on each side.
For multiple tables require explicit table selection/mapping rather than
merging equal column names across unrelated tables. Aggregate every chunk
once and count each physical file's rows once. Differences in logical schema,
sample rows, or selection provenance must be visible; equal row counts alone
do not prove equal data. Describe savings as lossless only with independent
schema/value validation.

Add positive `--depth N` for rollup over logical path segments. Specify how
physical list/map wrapper names map to logical nodes using schema facts. For
legacy streams with only dotted paths, expose physical-prefix mode explicitly
and reject ambiguous paths rather than pretending logical depth is known.
Roll up first, then compute ratios; never average child percentages.

## Implementation and acceptance

Build a pure compare/aggregate API over owned summaries, then thin CLI stream
adapters and table/JSON rendering. Validate record versions, completion, and
duplicate identities; fail on a truncated stream instead of accepting partial
totals as a complete comparison. Memory should scale with column/table
identities, not all page records. Check arithmetic overflow throughout.

Tests use hand-specified independent stream totals and tiny files: multi-group
aggregation, added/removed/zero columns, negative deltas, unequal row counts,
renamed files with the same logical columns, ambiguous path segments, depth
rollups, multiple tables, unknown versions, duplicate records, and incomplete
input. File and equivalent saved-stream comparisons must agree. No new heavy
dependency or network access is required. Point 2 enriches schema/provenance,
but an explicit physical-path comparison can ship independently.
