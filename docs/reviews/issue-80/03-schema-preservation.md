# 3. Schema-preserving experiment samples

## Verdict and evidence

Confirmed. `third_party/parquet/impl.rs::read_typed_sample` infers four kinds
from row values. `field_kind` maps groups/lists/maps to Bytes; `field_value`
stringifies them, and the writer emits a flat schema. The textual form is
not a faithful nested representation, regardless of whether it resembles
JSON. All-null columns default to Bytes rather than retaining source types.

The loss extends to flat columns: decimal values pass through floating point,
unsigned values above signed range saturate, and timestamp/date annotations
are discarded. `experiment.rs::write_trial` clones all decoded rows for each
trial and writes to an in-memory byte vector. Arrow batches can reduce
overhead but cannot make whole-sample sorting or retained output bounded.
The production ratios and 33 GB memory report cannot be reproduced without
the source data.

## Implementation plan

1. First fail explicitly for unsupported schemas/value conversions before
   publishing trial results; remove silent saturation/fallback conversion.
   Preserve an intentional conversion only under an explicit rewrite contract.
2. Prototype Parquet-to-Arrow batches and Arrow-to-Parquet round trips inside
   the third-party adapter. Expose an opaque owned sample/schema handle and
   semantic operations, not Arrow types, through its public API. Resolve types
   from the schema, including empty/all-null columns. Preserve logical types,
   nested nullability, decimal scale/precision, unsignedness, and timestamps.
3. Define preservation precisely: equal logical schema and values, with no
   intentional cast. Physical encodings may change. Record physical changes
   such as INT96 normalization separately; do not silently present them as a
   codec-only experiment. Do not copy stale sorting/index metadata blindly.
4. Stream batches through codec/dictionary-only trials, spooling temporary
   output and reading its footer instead of retaining whole files in RAM.
   Keep sort/zorder/hilbert trials materialized under an explicit memory
   budget initially; fail before exceeding it. External sorting is separate
   work. Row limits and complete-row-group limits need distinct selection.
5. Measure incremental/default compile cost and dependency additions. The
   current parquet dependency disables Arrow. If the cost violates the small
   default build constraint, provide an optional implementation with an honest
   unsupported error in the lightweight build. Cover Python as well as CLI.

## Acceptance criteria

Round-trip nested structs/lists/maps with empty and null containers, null
elements, repeated leaves, all-null typed columns, exact decimals, unsigned
boundaries, timezone/unit annotations, binary values, and multiple batches.
Assert decoded schema/value equality using a reference reader, not internal
helper names. Run control and codec-only trials on the same sample and verify
leaf identities and row counts. Test budget rejection through the public API.

This is the prerequisite for nested leaf overrides, precise profiles, and
credible source comparisons. Exact file-byte equality is not its contract.
