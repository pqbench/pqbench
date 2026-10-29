# Configuration

`pqbench` resolves its configuration from three layers, least to most specific:

1. a profile in `~/.pqbenchrc`,
2. the environment,
3. the command line.

A later layer overrides an earlier one, setting by setting.

## Profiles

`~/.pqbenchrc` is an INI of named profiles — the shape of `~/.databrickscfg` —
each naming an endpoint, a bearer, a dialect, and a catalog pace:

```ini
[DEFAULT]
endpoint                    = https://dbc-abc123.cloud.databricks.com
token                       = dapi…
table_format                = unity
catalog_requests_per_second = 15

[iceberg]
endpoint                    = https://dbc-abc123.cloud.databricks.com
token                       = dapi…
table_format                = iceberg
catalog_requests_per_second = 5
```

Two profiles over one workspace let a run switch dialect with `--profile`. The
profile is chosen by `--profile` or `PQB_PROFILE`; without either, `pqbench`
uses `[DEFAULT]`, or the only profile there is, and otherwise fails naming the
profiles it found.

The file holds a bearer, so keep it readable only by its owner:

```console no-run
$ chmod 600 ~/.pqbenchrc
```

Set `PQB_CONFIG_FILE` to read a different file.

## Environment

The environment overrides the selected profile:

| Variable | Role |
| --- | --- |
| `PQB_PROFILE` | the `~/.pqbenchrc` profile to run under |
| `PQB_ENDPOINT` | workspace URL, Unity OSS, or Iceberg REST base |
| `PQB_TOKEN` | bearer PAT (`dapi-…`) or OAuth token; list API only |
| `PQB_TABLE_FORMAT` | `unity` (the default) or `iceberg` |
| `PQB_CATALOG_REQUESTS_PER_SECOND` | catalog requests per second: 15 by default, `0` disables pacing |
| `DATABRICKS_HOST` / `DATABRICKS_TOKEN` | a lower-priority fallback for the endpoint and bearer |
| `AWS_*` | object-store region and credentials, read by the storage client |

`pqbench setup` prints the resolved endpoint, bearer, dialect, and pace for the
shell to evaluate:

```console no-run
$ eval "$(pqbench setup --profile iceberg)"
```

## Flags

Each layer is also a global flag, accepted before or after the
subcommand:

- `--profile NAME`
- `--endpoint URL`
- `--token TOKEN`
- `--table-format unity|iceberg`

The catalog pace is the exception: it is read from the profile and
`PQB_CATALOG_REQUESTS_PER_SECOND`, with no flag. Everything else is a
per-invocation switch on its own command — `--every`, `--last`, `--fan-out` —
and is not read from the run-control file.

## Example

The catalog levels read a profile; a single command can still override the
endpoint or bearer for one run:

```console no-run
$ echo '"dbx_samples.nyctaxi"' | pqbench --profile unity list tables
$ echo '"dbx_samples.nyctaxi.trips"' | pqbench --profile iceberg get table
$ pqbench list catalogs --endpoint https://dbc-abc123.cloud.databricks.com --token dapi-…
```
