# 2. File-level footer facts

## Verdict and evidence

Valid, with existing pieces. `third_party/parquet/api.rs::FileMass` already
has row count, row-group count, and sorting columns. `ColumnMass` and
`bytemass::MassRow` carry row-group identity/rows and chunk byte sizes. Sum
chunk sizes per group for compressed/uncompressed column bytes; do not sum
the repeated row-group row counts across columns.

Neither the adapter's FileMass nor `bytemass::FileStat` preserves created-by,
key/value metadata, footer version, or per-chunk index presence. Index
presence can be read from footer offsets/lengths without fetching the indexes.
No decoding is necessary.

## Proposed contract and implementation

Add owned `FileMetadata`, `MetadataEntry`, and `RowGroupMetadata` facts at
the Parquet boundary. Include optional `created_by`, `format_version`, an
ordered list of key/optional-value entries, per-group rows and column-byte
totals, and separate ColumnIndex/OffsetIndex presence per chunk. A file-wide
summary distinguishes NONE, PARTIAL, and ALL; empty files are explicit.

Preserve metadata keys as entries, not a map that silently drops duplicates.
For short displayed values, define a UTF-8-safe byte limit (proposed 256),
original byte length, and a truncation flag. Machine consumers must be able
to tell a preview from an exact value. Do not try to parse arbitrary metadata
values as JSON. Test absent values and empty strings separately.

Carry file facts through a file-oriented bytemass result or visitor so they
survive empty files. The current `Vec<MassRow>` return cannot represent an
empty file's facts; copying metadata onto each row would also multiply output.
Retain the existing convenience API and add a richer file API, then populate
the existing `bytemass-file` stream record once per file. Use additive optional
fields with serde defaults and update Python exposure and renderers.

Footer format version, data-page version, writer identity, and codec version
are different facts. Footer version must not be labeled proof of v2 pages;
the optional scan from point 1 can establish actual page types. Created-by is
diagnostic context, not a guarantee about the writer's configuration.

## Acceptance criteria

Create tiny files through public writer APIs with known created-by and
metadata, including duplicate keys, Unicode previews, and missing values.
Verify exact metadata, per-group rows/bytes, mixed index availability, and
empty files through public bytemass output. Deserialize old stream records.
Use a fake ranged reader to ensure the new default facts require only the
footer. This point is independent of the page scanner and decoded sample work.
