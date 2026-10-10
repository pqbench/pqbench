# Performance

How to measure the metadata walk of
[issue #58](https://github.com/pqbench/pqbench/issues/58) — the
process-partitioned structure where the shell is the scheduler and each command
resolves one level. This page is the recipe; the numbers belong in the pull
request (see the Perf box in the template).

## The structure

Each command resolves one level and streams its children to the next stage:

```console no-run
$ pqbench metastore ls | pqbench catalog ls <catalog> \
    | pqbench schema ls <catalog.schema> \
    | pqbench credentials check | pqbench credentials get \
    | pqbench table info | pqbench table ls \
    | pqbench partition ls | pqbench bytemass
```

`credentials check` is the walk's single filter: it drops a `system` catalog
ref, a view, and a table on managed default storage, so a mixed schema keeps
going. Measure a catalog with many tables, not one big table — the design
target is "jobs are limited not by big tables, but by the number of tables in a
catalog".

## Point it at a target

`pqbench setup` prints the walk's environment for the shell to evaluate
(`PQB_ENDPOINT` / `PQB_TOKEN`, and `AWS_REGION` for external storage):

```console no-run
$ eval "$(pqbench setup --endpoint <url> --token <token> --region us-east-2)"
```

`make dbx-e2e` drives the live Databricks workspace and its external fixture
(`pqbench_ext`, thirty Delta tables on customer S3); `make lakehouse` drives the
compose stand for the file-level commands.

## Run perf

`perf stat` measures the cost of a run; `perf record` finds the hot symbols.
Use the events the issue names — `task-clock` and `context-switches` are not
valid event names in this perf build:

```console no-run
$ perf stat -e cycles,instructions,duration_time -r 3 <the walk>
$ perf record -g -o /tmp/perf.data -- <the walk>
$ perf report --stdio -i /tmp/perf.data --no-children | head
```

`-r 3` repeats the run and reports the spread. Profile the cold and the warm
path (the first run pays a fresh TLS handshake). For a remote read, record wall
time and bytes read as well.

## Read it

- Compare `cycles` and `instructions` across the levels. A level that costs far
  more than one HTTPS round trip plus process startup is the one to look at.
- Expect the walk to be **startup-bound, not data-bound**. One process per
  table multiplies the fixed startup — dynamic linking plus the shared HTTP
  client's certificate-bundle decode — by the number of tables. That is the
  price of process partitioning, and the numbers should show it.
- Flag obvious latency (retries, serial round trips beyond one per command,
  whole-object reads) and fix the obvious cause. No micro-optimization.
