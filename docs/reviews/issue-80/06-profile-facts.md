# 6. Precision and pattern facts

## Verdict and evidence

Valid. `profile.rs::ColumnProfile` contains cardinality, text lengths, runs,
bounds, and monotonicity, but none of the requested exact precision or nested
list facts. `profile` accepts `third_party/parquet/api.rs::Sample`, whose
values are optional strings. That representation cannot distinguish declared
decimals/timestamps/nested lists reliably. Type guessing from string content
is insufficient for safe schema advice.

Depend on point 3's schema-aware input. Facts must identify selected rows and
the number of evaluated values. A sample with no violations supports a
candidate cast; the rewrite must still validate every value it converts.

## Proposed facts and definitions

| Domain | Facts and semantics |
| --- | --- |
| Decimal | Declared precision/scale; greatest integer-digit requirement; greatest fractional scale actually needed after removing trailing zeros from the exact unscaled integer; evaluated/non-null count. Zero requires one integer digit by convention. Negative values use magnitude. |
| Timestamp | Declared unit and UTC/local annotation; coarsest exact unit among seconds/milliseconds/microseconds/nanoseconds using integer divisibility; violation counts for candidate units and range failures. All-null columns report unknown rather than seconds. |
| List | Non-null, null, empty, and comparable-list counts; exact nearest-rank length p50 and greatest length; nondecreasing/nonincreasing fractions over explicitly eligible lists. Equal neighbors satisfy both orders. Exclude fewer-than-two-element lists from sortedness evidence and count them separately. |
| Gaps | Median signed adjacent difference for eligible numeric/timestamp lists, with explicit units and evaluated-pair count. Null elements/NaN make a list ineligible for ordering; do not bridge over them. Overflow must not wrap. |
| String | Independent canonical lowercase UUID, uppercase UUID, exact 32-ASCII-hex, and syntactically valid JSON predicates, with match/nonmatch counts over non-null strings. Predicates can overlap; define mixed case and digit-only UUID behavior. JSON scalars count as JSON; object/array can be separate classes. |

Describe precision as observed, not declared. Preserve exact integer/decimal
arithmetic through the aggregator. UUID pattern facts do not establish
application-level UUID semantics. High-entropy UUIDs are not automatically
good dictionary candidates.

## Implementation and acceptance

Add typed reducers in the profile domain, with decoding confined to the
Parquet adapter. Traverse logical list nodes and scalar leaves using path
segments. Keep existing generic facts for compatibility; add optional typed
fact objects. Bound parser work for JSON and report evaluation limits/errors
separately from actual pattern mismatches. Reuse serde_json and simple ASCII
validators; no regex framework is necessary for these fixed shapes.

Use tiny typed samples with trailing-zero decimals, negative/zero values,
precision boundaries, pre-epoch timestamps, null/empty/singleton lists,
duplicates, mixed order, overflow gaps, malformed JSON, and canonical and
noncanonical UUIDs. Verify counts, quantiles and units through the public
profile API. Include a sample-safe/full-input-unsafe cast example to prevent
documentation from promising more than the measured scope.
