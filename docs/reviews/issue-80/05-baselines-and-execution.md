# 5. Baselines, cost, concurrency, sampling, and help

## Verdict and evidence

The needs are valid, but an exact source-matching control is generally not
recoverable. `TrialSpec::control` uses zstd, dictionary enabled, and writer
defaults; `measure` computes trial/control bytes. `write_trial` materializes
rows and output, without cost measurements. The CLI reads leading rows only,
and the trial loop is sequential. `help.rs::EXPERIMENT_LONG_ABOUT` explicitly
denies zorder/hilbert/cast/encoding/page-size despite their implementation and
presence in `Capabilities::default`.

Codec identity and observed page layout are available, but codec level,
dictionary threshold, page targets, and original writer heuristics usually
are not. Replaying observed settings with a different writer does not promise
source-equivalent bytes. The reported 10% control overhead is unverified.

There is a further reporting trap: the adapter writes default zstd at level 3,
but reading that file's codec through parquet-rs displays `ZstdLevel(1)` in
the trial column record. Parquet stores codec identity, not its compression
level; the reader supplies a default level in its codec type. Report observed
codec name separately from the known requested/effective write level. Never
treat the decoded codec display string as evidence of the source level.

## Separate implementation increments within this issue point

1. **Help:** enumerate the current supported grammar, including dictionary
   byte limits, encoding, page size, casts and layouts. Keep unsupported drop
   and index editing explicit. State control defaults and current schema/cast
   limitations. Verify rendered CLI help against behavior/capability names;
   update runnable docs and regenerate their tests.
2. **Baselines:** keep existing control ratios for compatibility. Emit an
   explicit baseline kind, source selection, and effective writer settings.
   Support a caller-specified control spec. A best-effort source-derived
   control must list unknown/defaulted settings instead of claiming to match.
   Require point 3 before presenting nested comparisons as tuning evidence.
3. **Source ratios:** whole-file output may compare whole-file physical bytes
   only when it represents exactly the source rows/schema. Complete row-group
   sampling can compare corresponding column-chunk bytes; report those as
   column data bytes, excluding shared footer/other overhead on both sides.
   A first-N partial row group has no exact source compressed-byte baseline.
   Do not estimate it by proportional division and label it measured.
4. **Cost:** start with monotonic wall `write_duration`, measured around the
   rewrite/write phase, and separately identify read/transform/measure phases.
   CPU duration and peak resident bytes require isolated worker processes for
   useful per-trial attribution: process-lifetime high-water RSS cannot be
   reset per trial, and process CPU overlaps parallel trials. Isolate platform
   resource measurement behind an adapter; report unsupported metrics as absent.
   State whether peak memory includes the decoded sample and worker startup.
5. **Jobs:** add positive `--jobs N`, default 1, with a bounded worker pool,
   deterministic output ordering, one measured control, and cancellation/
   temporary-file cleanup on failure. Define total memory limits before
   multiplying samples across workers. Sorting and ordinary codec trials have
   different memory requirements. Worker CPU work must not block the existing
   one-thread async event loop. Mark parallel timings as contended measurements.
6. **Sampling:** add `--row-groups first:N` with N >= 1 and record actual zero-based
   group identities and rows. Make an explicitly supplied `--rows` mutually
   exclusive; its current default must not trigger a false conflict. Preserve
   source row-group boundaries for comparable baselines. Leading groups are
   reproducible, not necessarily representative of the whole table.

## Acceptance criteria

Tiny multi-group files: selection boundaries, row counts, invalid/empty
selection, same-schema eligibility, no partial-group source ratio, source and
control byte domains, and zero-byte baselines. Use injected timing/resource
providers to verify attribution/output without timing assertions. Compare
sequential/parallel results except timing, and exercise worker error cleanup.
No production-sized performance tests in the unit gate; separately record
manual memory/throughput benchmarks before claiming improvements.
