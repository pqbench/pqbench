# 8. Remote globs, file selection, and advisor recipes

## Remote glob verdict and draft

Confirmed. `bytemass/collection.rs::expand_inputs` only expands metacharacters
when the input lacks `://`; remote URIs go to the object reader literally.
Generic quoted-glob help is ambiguous. First clarify local-only expansion and
return a useful unsupported-pattern error instead of a misleading HEAD 404.
Retain a documented way to address literal wildcard characters in object keys.

For full support, add remote pattern expansion inside the object-store
adapter: parse bucket/key correctly, list from the longest literal key prefix,
consume every listing page, apply a documented path-glob grammar, sort and
deduplicate deterministically, and error on no matches. Do not treat encoded
key characters as URI wildcards accidentally. Reuse the optional aws backend
and existing listing facilities; credentials remain scoped to the input.
Exact URIs must continue to work with GetObject-only permissions. Listing
permission errors need context; never fall back to an unbounded bucket scan.

Test an injected in-memory listing with multiple pages, nested keys, literal
special characters, zero matches, duplicate inputs, and access errors. Check
that exact inputs do not list and the non-aws build reports capability errors.

## File-selection verdict and draft

Valid on the reviewed main. `table` and `dump` have no include/exclude/sample
flags. `lake` filters table names, not active files. Partition metadata and
file sizes exist in `TableFile`; selection can happen before downloading any
data. The original checkout had file filters, but that is not evidence that
current upstream supports them.

Put file selection in a composable transformation over table-file streams,
shared by convenience flags if they are later added. Preserve begin/end and
credential context, and emit selection provenance and corrected selected
counts. Select only active snapshot files, never reconstruct a Delta snapshot
by globbing its storage directory.

Offer partition/path filters first, then deterministic `first:N`, `every:N`,
and `median:N` policies. Define median selection as N files nearest the median
known byte size after filtering, with stable path tie-breaking and explicit
handling of missing sizes. Median selection buffers file metadata until a
table ends; it must not download data. This samples typical size, not
representative content; document stratified partition/time sampling as a
separate policy. Keep one implementation shared by table/dump workflows.

Test partitions including null values and escaped paths, ordering/ties, N=0,
no matches, missing sizes, multiple tables, and begin/end provenance. Public
dump tests should show that only selected files are copied, with no network.

## Recipes: verified, with qualifications

The proposed settings are real, but engine scope matters. Add each recipe to
`skills/parquet-advisor/recipes.md` with supported versions, reader checks, and
whether it affects future writes or rewrites existing files. Editing this
companion file does not require invoking the advisor workflow.

- Databricks documents `delta.parquet.format.version = '2.12.0'` for Runtime
  18.1+. Setting it affects subsequent writes. Its documented REORG rewrite
  requires Runtime 18.2+. This property value is not the Parquet footer integer
  version. [Databricks Parquet v2](https://docs.databricks.com/aws/en/tables/features/parquet-v2).
- parquet-mr supports `parquet.enable.dictionary#column.path` and
  `parquet.compression.codec.zstd.level`. Document them as Hadoop writer
  configuration, not universal Delta table properties or Photon guarantees.
  Verify the effective writer honors them by inspecting the output.
  [Apache parquet-java configuration](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/README.md).
- Databricks SQL only permits its listed configuration parameters; the Hadoop
  zstd level option is absent. Do not recommend arbitrary Spark/Hadoop SET
  statements on SQL warehouses. The specific observed rejection was not
  reproduced here. [Databricks SQL parameters](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-parameters).

Keep external engine execution out of unit tests. Add generated runnable docs
only for supported local commands; mark external SQL/Python examples with
their engine/version and record any manual engine validation separately.
