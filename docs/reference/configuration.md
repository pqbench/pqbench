# Configuration

`pqbench` resolves its configuration from three layers, least to most specific:

1. a profile in `~/.pqbenchrc`,
2. the environment,
3. the command line.

A later layer overrides an earlier one, setting by setting.

## Profiles

`~/.pqbenchrc` is an INI of named profiles — the shape of `~/.databrickscfg` —
each naming an endpoint, a bearer, and a dialect:

```ini
[DEFAULT]
endpoint     = https://dbc-abc123.cloud.databricks.com
token        = dapi…
table_format = unity

[iceberg]
endpoint     = https://dbc-abc123.cloud.databricks.com
token        = dapi…
table_format = iceberg

[principal]
endpoint      = https://dbc-abc123.cloud.databricks.com
client-id     = 0123-4567
client-secret = …
table_format  = unity
```

A profile may name a service principal's `client-id` and `client-secret`
(`client_id`/`client_secret` are the same keys) instead of a bearer; see
[Service principal](authentication.md#service-principal-oauth-m2m). A static
`token` wins when a profile carries both. Any key takes either spelling: its
hyphens and underscores name the same key (`table-format` is `table_format`).

Two profiles over one workspace let a run switch dialect with `--profile`. The
profile is chosen by `--profile` or `PQB_PROFILE`; without either, `pqbench`
uses `[DEFAULT]`, or the only profile there is, and otherwise fails naming the
profiles it found.

The file holds a bearer, so keep it readable only by its owner:

```console no-run
$ chmod 600 ~/.pqbenchrc
```

Set `PQB_CONFIG_FILE` (or `PQB_CONFIG`) to read a different file, as the driver
does. The repository's
`examples/pqbenchrc` is a committed copy of the file above with a placeholder
bearer, so the layering can be tried without a workspace.

## Environment

The environment overrides the selected profile:

| Variable | Role |
| --- | --- |
| `PQB_PROFILE` | the `~/.pqbenchrc` profile to run under |
| `PQB_ENDPOINT` | workspace URL, Unity OSS, or Iceberg REST base |
| `PQB_TOKEN` | bearer PAT (`dapi-…`) or OAuth token; catalog reads and vending |
| `PQB_CLIENT_ID` / `PQB_CLIENT_SECRET` | a service principal, minting the bearer when no token is set |
| `PQB_TABLE_FORMAT` | `unity` (the default) or `iceberg` |
| `PQB_TABLE_FAN_OUT` | tables in flight for `get table` (default: the core count) |
| `DATABRICKS_HOST` / `DATABRICKS_TOKEN` | a lower-priority fallback for the endpoint and bearer |
| `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` | the same fallback for a service principal |
| `AWS_*` | object-store region and ambient credentials, read by the storage client |
| `PQB_DAEMON_SOCK` | override the daemon socket path (a sidecar or a test) |

`pqb setup` prints the resolved endpoint, bearer, and dialect for the shell to
evaluate; a service principal's bearer is minted for the export, and the secret
is never printed. `pqb setup --check` prints no exports: it reports the resolved
configuration, the credential, and the endpoint's answer as three `ok`/`fail`
lines, and exits non-zero when any check failed. A later layer wins, so the
environment can override the profile. `pqb daemon start` runs the daemon for
those values, minting and refreshing a service principal's bearer; without it
each command vends directly, which is correct but asks the catalog once per
stage.

```console run
$ PQB_CONFIG_FILE=examples/pqbenchrc pqb setup --profile iceberg
export PQB_ENDPOINT='https://dbc-abc123.cloud.databricks.com'
export PQB_TOKEN='dapi-example'
export PQB_TABLE_FORMAT='iceberg'
export AWS_EC2_METADATA_DISABLED='true'
$ PQB_CONFIG_FILE=examples/pqbenchrc PQB_TABLE_FORMAT=unity pqb setup --profile iceberg
export PQB_ENDPOINT='https://dbc-abc123.cloud.databricks.com'
export PQB_TOKEN='dapi-example'
export PQB_TABLE_FORMAT='unity'
export AWS_EC2_METADATA_DISABLED='true'
```

## Flags

Every layer is also a global flag, accepted before or after the
subcommand:

- `--profile NAME`
- `--endpoint URL`
- `--token TOKEN`
- `--client-id ID` / `--client-secret SECRET`
- `--table-format unity|iceberg`

Everything else is a per-invocation switch on its own command — `--last`,
`--partition`, `--fan-out`, `--rate`, `--depth`, `--samples`, `--rows`,
`--top`, `--per-column`, `--output` — and is not read from the run-control
file. See the [CLI reference](cli.md) for each command's flags.

## Driver settings

`pqbench-driver` keeps a map of partition versions and drives each through
price, download, bytemass, lint, and verify; its walk re-lists the catalog on a
period. It reads one setting of its own from the same run-control file, from
`[DEFAULT]` or from the section `PQB_PROFILE` names. The file is
`~/.pqbenchrc`, or what `PQB_CONFIG_FILE` (or `PQB_CONFIG`) names:

```ini
[DEFAULT]
step-timeout = 300
```

| Key | Role | Default |
| --- | --- | --- |
| `step-timeout` | seconds one worker command may run before it is killed | `300` |

It changes rarely, so it lives only in the file: it is not a flag and has no
environment variable of its own. The command line carries what changes per run,
the file what an operator sets once. The key sits beside the profile's, and
either side ignores the other's.

The checkpoint is `checkpoint.ndjson.zst` under the store: one standard
Zstandard frame of NDJSON, which `zstdcat` reads. The driver assembles it in a
temporary file, syncs it, renames it into place, and syncs the store directory,
so a crash leaves the old checkpoint or the new one and never half of either.

A command that outlives `step-timeout` is killed and reaped, so no hung worker
outlives its step, and the version goes back to its previous state with the
failure counted, retrying after the usual backoff. A walk of the whole
metastore is not bounded by it: it runs table after table, and a per-step limit
would kill a healthy walk halfway.

## Example

The catalog levels read a profile; a single command can still override the
endpoint or bearer for one run:

```console no-run
$ echo '"dbx_samples.nyctaxi"' | pqb --profile unity list tables
$ echo '"dbx_samples.nyctaxi.trips"' | pqb --profile iceberg get table
$ pqb ls catalogs --endpoint https://dbc-abc123.cloud.databricks.com --token dapi-…
```
