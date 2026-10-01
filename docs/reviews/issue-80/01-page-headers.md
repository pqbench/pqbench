# 1. Optional page-header scan

## Verdict and evidence

Valid. `BytemassRequest` has only an `indexes` switch.
`third_party/parquet/impl.rs::create_masses` gets `page_count` and
`page_compressed_bytes` from the OffsetIndex. Without it these fields are
unknown, although the ordinary footer masses still work. The existing
`PageParser` reads payloads and requires uncompressed input; it is not the
requested scanner.

The issue's inferences need qualification: one page per row group does not
prove that no row limit was configured. A dictionary-to-PLAIN transition is
observable, but its exact size threshold and cause are not recoverable from
that observation alone. Dictionary page header encoding and data-page value
encoding must be reported separately.

## Proposed contract

Add opt-in `--pages`, independent of `--indexes`. Default reads remain footer
only. Emit versioned `bytemass-page` records in file/row-group/leaf/page order:
file identity, path segments, row group, page ordinal, header offset and
length, page type, compressed/uncompressed payload bytes, value count,
dictionary entry count, and applicable value/level encodings. A v2 row count
may be reported; a v1 value count is not a row count for repeated data.

Keep byte definitions explicit: header fields exclude the header itself;
chunk totals and OffsetIndex page sizes include headers. Never substitute
zero for unavailable counts. Preserve dictionary and unknown page types.

## Implementation plan

1. Add owned header facts at the Parquet adapter boundary and a bounded
   scan interface, then expose them through bytemass without leaking library
   types. Use footer chunk offsets/lengths to delimit each scan.
2. Resolve the parser prerequisite first: parquet-rs 59.3.0 keeps `PageHeader`
   private. Its public `PageMetadata` only exposes row/level counts and a
   dictionary flag. Request a public header API upstream, or evaluate a small
   maintained format decoder behind the adapter. Do not assume `peek` gives
   encodings/sizes, fork the entire parser, or hand-roll Thrift casually.
3. Read a bounded header window, extend on incomplete input within a documented
   limit, then skip the compressed payload by its length. Check signed sizes,
   checked offset arithmetic, chunk bounds, truncation, and forward progress.
   Reject unsupported encrypted headers explicitly.
4. For remote objects, reuse bounded range reads and the identity pinning in
   `third_party/object_store/api.rs`. Cache small windows to avoid a request
   per field; document that read-ahead may fetch payload bytes even though
   none are decompressed. Stream records rather than retaining all pages.

## Acceptance criteria

Tiny in-memory v1/v2 files, compressed and uncompressed, without indexes:
verify page types, encodings, dictionary entries, counts, and byte offsets
against independently specified headers. Include dictionary fallback,
repeated values, multiple row groups, and corrupt/truncated sizes. An injected
remote reader must enforce identity/bounds and prove default bytemass needs
no page access. No network or external fixture generation in unit tests.

Reference: [Parquet data pages](https://parquet.apache.org/docs/file-format/data-pages/).
Inspect the pinned parquet-rs `column/page.rs::PageMetadata` and
`file/serialized_reader.rs` before choosing the decoder dependency.
