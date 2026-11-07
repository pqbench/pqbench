#!/usr/bin/env python3
"""Live Databricks metastore e2e, as Python tests.

    python scripts/dbx_e2e.py -v
    PQB=target/debug/pqbench python scripts/dbx_e2e.py -v

Each test runs ``pqbench`` against the live workspace and checks the NDJSON it
emits; plumbum composes the multi-stage pipes. The suite is the live half of
``scripts/dbx-walk.sh``: `get metastore`, the catalog levels through both
dialects, the external S3 fixture, lease-backed table reads, and the walk from
`list tables` to a treemap page. The whole metastore walk itself lives in
``scripts/walk_metastore.py`` and is run from here.

The workspace URL comes from ``DBX_HOST`` (a repository secret in CI); the
service principal's credentials come from ``DBX_SAMPLES_SP_CLIENT_ID`` /
``DBX_SAMPLES_SP_CLIENT_SECRET``, or a ready ``DBX_TOKEN`` is used instead.
Without ``DBX_HOST`` or credentials every test skips, so a leg without secrets
stays green.

Run with ``make dbx-e2e`` (or ``python -m unittest`` for one test).
"""

from __future__ import annotations

import base64
import json
import os
import sys
import tempfile
import unittest
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from time import monotonic

from plumbum import local

# PQBench under test: `make dbx-e2e` points this at the built binary.
PQ_BENCH = os.environ.get("PQB", "target/debug/pqbench")

# Variables the tests own: a developer's shell must not leak a profile, a
# token, or an S3 identity into the child.
SCRUBBED = [
    "DBX_TOKEN",
    "DBX_HOST",
    "DBX_SAMPLES_SP_CLIENT_ID",
    "DBX_SAMPLES_SP_CLIENT_SECRET",
    "PQB_ENDPOINT",
    "PQB_TOKEN",
    "PQB_PROFILE",
    "PQB_CONFIG_FILE",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
    "AWS_ENDPOINT",
    "AWS_ENDPOINT_URL",
    "AWS_REGION",
    "AWS_ALLOW_HTTP",
    "AWS_VIRTUAL_HOSTED_STYLE_REQUEST",
]

_WORK = tempfile.TemporaryDirectory(prefix="pqbench-e2e-")
_SOCKET = os.path.join(_WORK.name, "daemon.sock")

# The schemas `dbx_samples` answers with, from either dialect.
DBX_SAMPLES_SCHEMAS = [
    "bakehouse",
    "information_schema",
    "nyctaxi",
    "tpch_sf1",
    "wanderbricks",
]

# The `pqbench_ext` schemas the e2e service principal can see, sorted: the
# synthetic `events`/`sales`/`reference` plus the real sample data copied
# server-side from `dbx_samples` (see scripts/dbx-ext/).
EXTERNAL_FIXTURE_SCHEMAS = [
    "accuweather",
    "bakehouse",
    "clickbench",
    "events",
    "healthverity",
    "information_schema",
    "nyctaxi",
    "reference",
    "sales",
    "tpcds_sf1",
    "tpch_sf1",
    "tpch_sf10",
]

_TOKEN: str | None = None


def environment(name: str) -> str | None:
    """A non-empty environment variable, or None."""
    value = os.environ.get(name)
    return value or None


def unity_endpoint(host: str) -> str:
    return f"{host.rstrip('/')}/api/2.1/unity-catalog"


def iceberg_endpoint(host: str, catalog: str = "dbx_samples") -> str:
    return f"{unity_endpoint(host)}/iceberg-rest/v1/catalogs/{catalog}"


@contextmanager
def scrubbed(extra: dict[str, str] | None = None):
    """Run the child with the tests' environment, not the caller's."""
    with local.env():
        for name in SCRUBBED:
            local.env.pop(name, None)
        local.env["PQB_DAEMON_SOCK"] = _SOCKET
        local.env.update(extra or {})
        # Do not attach to a developer's daemon at the derived socket path.
        try:
            os.unlink(_SOCKET)
        except FileNotFoundError:
            pass
        yield


def run(args: list[str], stdin: str | None = None, env: dict[str, str] | None = None):
    """Run pqbench and return `(status, stdout, stderr)`."""
    with scrubbed(env):
        command = local[PQ_BENCH][args]
        if stdin is None:
            return command.run(retcode=None)
        return (command << stdin).run(retcode=None)


def pipeline(stages: list[list[str]], env: dict[str, str] | None = None):
    """Compose pqbench stages into one pipe and return `(status, stdout, stderr)`."""
    with scrubbed(env):
        commands = [local[PQ_BENCH][args] for args in stages]
        combined = commands[0]
        for command in commands[1:]:
            combined = combined | command
        return combined.run(retcode=None)


def ndjson(text: str) -> list[dict]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def resource(record: dict) -> str:
    """The record's resource type: the collection before its id."""
    segments = record.get("name", "").split("/")
    return segments[-2] if len(segments) >= 2 else ""


def resource_id(record: dict) -> str:
    """The record's id: the last segment of its resource name."""
    return record.get("name", "").rsplit("/", 1)[-1]


def fqn(record: dict) -> str:
    """The dotted `catalog.schema[.table]` a record's name names."""
    segments = record.get("name", "").split("/")
    return ".".join(segments[1::2])


def find(records: list[dict], name: str) -> dict | None:
    """The record whose name is `name`, or None."""
    return next((record for record in records if fqn(record) == name), None)


def is_blob(record: dict) -> bool:
    parts = record.get("name", "").split("/")
    return len(parts) == 3 and parts[0] == "blobs"


def is_column(record: dict) -> bool:
    parts = record.get("name", "").split("/")
    return len(parts) == 5 and parts[0] == "blobs" and parts[3] == "columns"


def is_table(record: dict) -> bool:
    parts = record.get("name", "").split("/")
    return (
        len(parts) == 6
        and parts[0] == "catalogs"
        and parts[2] == "schemas"
        and parts[4] == "tables"
    )


def catalog_ref(name: str) -> str:
    return json.dumps({"name": f"catalogs/{name}"}) + "\n"


def schema_ref(name: str) -> str:
    catalog, schema = name.split(".")
    return json.dumps({"name": f"catalogs/{catalog}/schemas/{schema}"}) + "\n"


def table_ref(name: str) -> str:
    catalog, schema, table = name.split(".")
    return json.dumps({"name": f"catalogs/{catalog}/schemas/{schema}/tables/{table}"}) + "\n"


def mint_token(host: str) -> str | None:
    """`POST /oidc/v1/token` with HTTP Basic `client_id:client_secret`."""
    client_id = environment("DBX_SAMPLES_SP_CLIENT_ID")
    client_secret = environment("DBX_SAMPLES_SP_CLIENT_SECRET")
    if not client_id or not client_secret:
        return None
    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    body = urllib.parse.urlencode(
        {"grant_type": "client_credentials", "scope": "all-apis"}
    ).encode()
    request = urllib.request.Request(
        f"{host}/oidc/v1/token",
        data=body,
        headers={"Authorization": f"Basic {credentials}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response).get("access_token")
    except (urllib.error.URLError, OSError) as error:
        print(f"OAuth M2M mint failed: {error}", file=sys.stderr)
        return None


def any_token(host: str) -> str | None:
    """A ready bearer from the environment, or the SP's minted one."""
    global _TOKEN
    if _TOKEN is None:
        _TOKEN = environment("DBX_TOKEN") or mint_token(host)
    return _TOKEN


def source(host: str, token: str | None) -> dict[str, str]:
    """The walk's config: `PQB_ENDPOINT` / `PQB_TOKEN` for the live endpoint."""
    env = {"PQB_ENDPOINT": unity_endpoint(host)}
    if token:
        env["PQB_TOKEN"] = token
    return env


class LiveE2E(unittest.TestCase):
    def require_host(self) -> str:
        host = environment("DBX_HOST")
        if not host:
            self.skipTest("DBX_HOST is not set")
        return host

    def require_token(self, host: str) -> str:
        token = any_token(host)
        if not token:
            self.skipTest("DBX_TOKEN and DBX_SAMPLES_SP_CLIENT_ID/SECRET are not set")
        return token

    def assert_ok(self, args: list[str], *, stdin=None, env=None, label=None) -> str:
        status, out, err = run(args, stdin=stdin, env=env)
        self.assertEqual(status, 0, f"{label or ' '.join(args)} stderr: {err}")
        return out

    def assert_fails(self, args: list[str], *, stdin=None, env=None) -> str:
        status, _, err = run(args, stdin=stdin, env=env)
        self.assertNotEqual(status, 0, f"{' '.join(args)} unexpectedly succeeded")
        return err

    # `get metastore` reads the live metastore record.
    def test_metastore_get_reads_the_live_metastore(self):
        host = self.require_host()
        token = self.require_token(host)
        records = ndjson(self.assert_ok(["get", "metastore"], env=source(host, token)))
        self.assertEqual(len(records), 1)
        self.assertEqual(resource(records[0]), "metastores")
        self.assertTrue(records[0]["name"])
        self.assertTrue(records[0]["cloud_provider"])
        self.assertTrue(records[0]["region"])

    # `list catalogs` lists the live catalogs, sorted by name.
    def test_catalog_ls_lists_the_live_catalogs(self):
        host = self.require_host()
        token = self.require_token(host)
        env = source(host, token)
        records = ndjson(self.assert_ok(["list", "catalogs"], env=env))
        for record in records:
            self.assertEqual(resource(record), "catalogs")
        self.assertEqual(
            [resource_id(record) for record in records],
            ["dbx_samples", "pqbench_ext", "samples", "system", "workspace"],
        )
        self.assertEqual(len(ndjson(self.assert_ok(["list", "catalogs"], env=env))), 5)

    # The live metastore walk is a streaming shell pipeline driven from Python
    # (`scripts/walk_metastore.py`): `list catalogs | list schemas | list tables`
    # fans each level out instead of walking one schema at a time.
    def test_metastore_walk_streams_the_whole_catalog(self):
        host = self.require_host()
        token = self.require_token(host)
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "walk_metastore.py")
        env = source(host, token)
        env.update({"PQB_CONFIG_FILE": "/dev/null"})
        with scrubbed(env):
            status, out, err = (
                local[sys.executable][script, "--pqbench", PQ_BENCH, "--fan-out", "64"]
            ).run(retcode=None)
        self.assertEqual(status, 0, f"walk stderr: {err}")
        self.assertIn("walk: 5 catalogs,", out, "the walk did not finish")
        print(out, end="")

    # `get catalog` reads the live catalog.
    def test_catalog_get_reads_the_live_catalog(self):
        host = self.require_host()
        token = self.require_token(host)
        out = self.assert_ok(
            ["get", "catalog"],
            stdin=catalog_ref("dbx_samples"),
            env=source(host, token),
        )
        records = ndjson(out)
        self.assertEqual(len(records), 1)
        self.assertEqual(resource(records[0]), "catalogs")
        self.assertEqual(records[0]["name"], "catalogs/dbx_samples")
        self.assertEqual(records[0]["catalog_type"], "CATALOG_TYPE_MANAGED")

    # `list schemas` reads the schemas `dbx_samples` answers with.
    def test_schema_ls_lists_the_live_schemas(self):
        host = self.require_host()
        token = self.require_token(host)
        records = ndjson(
            self.assert_ok(
                ["list", "schemas"],
                stdin=catalog_ref("dbx_samples"),
                env=source(host, token),
            )
        )
        names = []
        for record in records:
            self.assertEqual(resource(record), "schemas")
            self.assertEqual(fqn(record), f"dbx_samples.{resource_id(record)}")
            names.append(resource_id(record))
        self.assertEqual(names, DBX_SAMPLES_SCHEMAS)

    def _schema_ids(self, env: dict[str, str], schema: str) -> list[str]:
        out = self.assert_ok(
            ["list", "tables"],
            stdin=schema_ref(f"pqbench_ext.{schema}"),
            env=env,
            label=f"list tables pqbench_ext.{schema}",
        )
        return [fqn(record) for record in ndjson(out)]

    # `list schemas` and `list tables` on the external-location fixture: the
    # synthetic `events`/`sales`/`reference` (ten two-column tables each) and the
    # real sample data copied from `dbx_samples`, all external Delta on customer
    # S3 through the `pqbench_uc_e2e` external location.
    def test_schema_ls_lists_the_external_fixture(self):
        host = self.require_host()
        token = self.require_token(host)
        env = source(host, token)
        schemas = [
            resource_id(record)
            for record in ndjson(
                self.assert_ok(
                    ["list", "schemas"], stdin=catalog_ref("pqbench_ext"), env=env
                )
            )
        ]
        self.assertEqual(schemas, EXTERNAL_FIXTURE_SCHEMAS)

        # The synthetic tables name t01..t10.
        synthetic = 0
        for schema in ["events", "sales", "reference"]:
            ids = self._schema_ids(env, schema)
            self.assertEqual(ids, [f"pqbench_ext.{schema}.t{i:02}" for i in range(1, 11)])
            synthetic += len(ids)
        self.assertEqual(synthetic, 30)

        # The copied sample tables. The counts pin the copy set; the singletons
        # pin the names the walk tests read.
        copies = {
            "bakehouse": 6,
            "accuweather": 12,
            "healthverity": 1,
            "tpcds_sf1": 24,
            "tpch_sf1": 8,
            "tpch_sf10": 8,
            "nyctaxi": 1,
            "clickbench": 1,
        }
        for schema, count in copies.items():
            ids = self._schema_ids(env, schema)
            self.assertEqual(len(ids), count, f"{schema}: {ids}")
        self.assertEqual(self._schema_ids(env, "nyctaxi"), ["pqbench_ext.nyctaxi.trips"])
        self.assertEqual(self._schema_ids(env, "clickbench"), ["pqbench_ext.clickbench.hits"])
        self.assertEqual(
            self._schema_ids(env, "healthverity"),
            ["pqbench_ext.healthverity.claims_sample_synthetic"],
        )

    # `get schema` reads `dbx_samples.nyctaxi` from both dialects.
    def test_schema_get_reads_the_live_schema(self):
        host = self.require_host()
        token = self.require_token(host)
        for endpoint, iceberg in [
            (unity_endpoint(host), False),
            (iceberg_endpoint(host), True),
        ]:
            env = {"PQB_ENDPOINT": endpoint, "PQB_TOKEN": token}
            if iceberg:
                env["PQB_TABLE_FORMAT"] = "iceberg"
            records = ndjson(
                self.assert_ok(
                    ["get", "schema"], stdin=schema_ref("dbx_samples.nyctaxi"), env=env
                )
            )
            self.assertEqual(len(records), 1)
            self.assertEqual(resource(records[0]), "schemas")
            self.assertEqual(records[0]["name"], "catalogs/dbx_samples/schemas/nyctaxi")

    # `list tables` lists `dbx_samples.nyctaxi` from both dialects: Unity fills
    # the storage path in the listing, Iceberg REST leaves it to `get table`.
    def test_table_ls_lists_the_live_tables(self):
        host = self.require_host()
        token = self.require_token(host)
        records = ndjson(
            self.assert_ok(
                ["list", "tables"],
                stdin=schema_ref("dbx_samples.nyctaxi"),
                env=source(host, token),
            )
        )
        self.assertTrue(records)
        for record in records:
            self.assertEqual(resource(record), "tables")
            self.assertIn("/tables/", record["name"])
            self.assertIsNotNone(record.get("uri"), f"Unity fills the storage URI: {record}")
        names = [fqn(record) for record in records]
        self.assertIn("dbx_samples.nyctaxi.trips", names)

        endpoint = iceberg_endpoint(host)
        env = {
            "PQB_ENDPOINT": endpoint,
            "PQB_TOKEN": token,
            "PQB_TABLE_FORMAT": "iceberg",
        }
        listing = self.assert_ok(
            ["list", "tables"], stdin=schema_ref("dbx_samples.nyctaxi"), env=env
        )
        trips = find(ndjson(listing), "dbx_samples.nyctaxi.trips")
        self.assertIsNotNone(trips, listing)
        self.assertEqual(trips["name"], "catalogs/dbx_samples/schemas/nyctaxi/tables/trips")
        self.assertIsNone(trips.get("uri"), trips)

        # The new tree enriches its own v2 refs: `get table` reads the
        # `loadTable` metadata inline, so no storage read runs.
        info = ndjson(self.assert_ok(["get", "table"], stdin=listing, env=env))
        trips = find(info, "dbx_samples.nyctaxi.trips")
        self.assertIsNotNone(trips, info)
        self.assertEqual(resource(trips), "tables")
        self.assertEqual(trips["format"], "TABLE_FORMAT_ICEBERG")
        self.assertTrue(trips["declared_columns"])
        self.assertGreater(trips["snapshot_version"], 0)
        self.assertTrue(trips["uri"])

    # `get table` reads the live Iceberg REST table: the `loadTable` response
    # carries the metadata inline, so the record is complete without any storage
    # read (the default-storage Delta path cannot read its log).
    def test_table_get_reads_the_live_iceberg_table(self):
        host = self.require_host()
        token = self.require_token(host)
        env = {
            "PQB_ENDPOINT": iceberg_endpoint(host),
            "PQB_TOKEN": token,
            "PQB_TABLE_FORMAT": "iceberg",
        }
        records = ndjson(
            self.assert_ok(
                ["get", "table"],
                stdin=table_ref("dbx_samples.nyctaxi.trips"),
                env=env,
            )
        )
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(resource(record), "tables")
        self.assertEqual(fqn(record), "dbx_samples.nyctaxi.trips")
        self.assertEqual(record["format"], "TABLE_FORMAT_ICEBERG")
        self.assertGreater(record["snapshot_version"], 0)
        self.assertGreaterEqual(len(record["declared_columns"]), 6)
        self.assertIsInstance(record["properties"], dict)

    # `get table` on the live Unity catalog: the table read runs with the env it
    # is given — here none — so the `without_files()` Delta log read of the
    # managed default-storage table fails outside compute and the error names the
    # table and its location.
    def test_table_get_names_the_location_when_the_metadata_cannot_be_read(self):
        host = self.require_host()
        token = self.require_token(host)
        stderr = self.assert_fails(
            ["get", "table"],
            stdin=table_ref("dbx_samples.nyctaxi.trips"),
            env=source(host, token),
        )
        self.assertIn("dbx_samples.nyctaxi.trips", stderr)
        self.assertIn("s3://", stderr)

    # `get table` on the live Unity catalog: the UniForm Delta fixture
    # (`pqbench_delta_test`) is on default storage, so it gets no lease and its
    # storage read fails with the table named.
    def test_get_table_refuses_default_storage(self):
        host = self.require_host()
        token = self.require_token(host)
        env = source(host, token)
        env["AWS_REGION"] = aws_region()
        stderr = self.assert_fails(
            ["get", "table"],
            stdin=table_ref("dbx_samples.nyctaxi.pqbench_delta_test"),
            env=env,
        )
        self.assertIn(
            "catalogs/dbx_samples/schemas/nyctaxi/tables/pqbench_delta_test", stderr
        )

    # `get table` on the external fixture: every table in the synthetic schemas
    # and in a copied schema is eligible, so each read resolves its own lease and
    # the emitted records carry no credentials.
    def test_get_table_reads_the_external_fixture(self):
        host = self.require_host()
        token = self.require_token(host)
        env = source(host, token)
        env["AWS_REGION"] = aws_region()

        tables = 0
        for schema in ["events", "sales", "reference", "tpch_sf1", "clickbench"]:
            listing = self.assert_ok(
                ["list", "tables"],
                stdin=schema_ref(f"pqbench_ext.{schema}"),
                env=env,
            )
            for record in ndjson(self.assert_ok(["get", "table"], stdin=listing, env=env)):
                self.assertNotIn("credentials", record, f"the lease rode on the record: {record}")
                tables += 1
        self.assertEqual(tables, 39)

    def _timed(self, args: list[str], stdin: str, env: dict[str, str]):
        started = monotonic()
        out = self.assert_ok(args, stdin=stdin, env=env)
        return out, monotonic() - started

    # The whole issue-#58 walk on a copied schema, all the way to a treemap:
    # `list tables` → `get table` → `list files` → `list columns` →
    # `summarize columns` → `format --html`, each stage fed the previous stage's
    # NDJSON. Set `DBX_WALK_SCHEMA` to walk another schema (default
    # `pqbench_ext.tpch_sf1`).
    def test_external_fixture_walks_to_a_treemap(self):
        host = self.require_host()
        token = self.require_token(host)
        schema = environment("DBX_WALK_SCHEMA") or "pqbench_ext.tpch_sf1"
        env = source(host, token)
        env["AWS_REGION"] = aws_region()

        listed, _ = self._timed(["list", "tables"], schema_ref(schema), env)
        info, _ = self._timed(["get", "table"], listed, env)
        files, _ = self._timed(["list", "files"], info, env)
        measured, _ = self._timed(["list", "columns"], files, env)

        # The walk narrows: tables → files → columns.
        tables = len(ndjson(info))
        file_count = len(ndjson(files))
        records = ndjson(measured)
        self.assertGreater(tables, 0, f"{schema}: no tables")
        self.assertGreaterEqual(
            file_count, tables, f"{schema}: {file_count} files < {tables} tables"
        )
        self.assertTrue(any(is_blob(record) for record in records), f"{schema}: no Blob record")
        self.assertTrue(any(is_column(record) for record in records), f"{schema}: no Column record")

        summarized, _ = self._timed(["summarize", "columns"], measured, env)
        rows = ndjson(summarized)
        self.assertTrue(rows, f"{schema}: no ColumnMass row")
        self.assertTrue(
            all(is_table({"name": row.get("table", "")}) for row in rows),
            f"{schema}: a ColumnMass row names no table",
        )
        self.assertTrue(
            any((row.get("compressed_bytes") or 0) > 0 for row in rows),
            f"{schema}: no measured bytes",
        )

        rendered = self.assert_ok(["format", "--html"], stdin=summarized, env=env)
        self.assertTrue(rendered.startswith("<!DOCTYPE html>"), f"{schema}: no page")
        self.assertIn("d3-hierarchy@3", rendered, f"{schema}: no treemap")
        schema_id = schema.rsplit(".", 1)[-1]
        self.assertIn(
            f'"display_name":"{schema_id}"',
            rendered,
            f"{schema}: the page names no schema",
        )

        print(
            f"walk {schema}: {tables} tables, {file_count} files, "
            f"{len(records)} column records, {len(rows)} rows"
        )

    # `get table` on a table in customer storage: the stage resolves the table's
    # vended lease itself, reads the Delta log under it, and emits a record with
    # no credentials. The fixture lives in `us-east-2`; set `DBX_AWS_TABLE` /
    # `DBX_AWS_REGION` to read another one.
    def test_table_get_reads_the_external_aws_table(self):
        host = self.require_host()
        token = self.require_token(host)
        table = environment("DBX_AWS_TABLE") or "pqbench_ext.events.t01"
        env = source(host, token)
        env["AWS_REGION"] = aws_region()
        records = ndjson(
            self.assert_ok(["get", "table"], stdin=table_ref(table), env=env)
        )
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(fqn(record), table)
        self.assertEqual(record["format"], "TABLE_FORMAT_DELTA")
        self.assertTrue(record.get("uri", "").startswith("s3://"), record)
        self.assertTrue(record["declared_columns"])
        self.assertNotIn("credentials", record, f"the lease rode on the record: {record}")

    # `get table` with `PQB_TABLE_FORMAT=iceberg` on the live catalog: loadTable
    # returns the metadata inline, and the emitted records carry no credentials.
    def test_get_table_reads_the_live_iceberg_tables(self):
        host = self.require_host()
        token = self.require_token(host)
        env = {
            "PQB_ENDPOINT": iceberg_endpoint(host),
            "PQB_TOKEN": token,
            "PQB_TABLE_FORMAT": "iceberg",
        }
        for table in [
            "dbx_samples.nyctaxi.trips",
            "dbx_samples.nyctaxi.pqbench_iceberg_test",
        ]:
            records = ndjson(
                self.assert_ok(["get", "table"], stdin=table_ref(table), env=env, label=table)
            )
            self.assertEqual(len(records), 1, table)
            self.assertNotIn("credentials", records[0], f"{table}: the lease rode on the record")

    # The walk as a pipe, context in `PQB_ENDPOINT` / `PQB_TOKEN`:
    # `list catalogs | get catalog | list schemas` (decision 0004).
    def test_catalog_ls_pipes_into_catalog_get_and_ls(self):
        host = self.require_host()
        token = self.require_token(host)
        env = source(host, token)

        catalogs = self.assert_ok(["list", "catalogs"], env=env)
        names = [resource_id(record) for record in ndjson(catalogs)]
        self.assertEqual(names, ["dbx_samples", "pqbench_ext", "samples", "system", "workspace"])

        status, info, err = pipeline([["list", "catalogs"], ["get", "catalog"]], env)
        self.assertEqual(status, 0, f"get catalog stderr: {err}")
        records = ndjson(info)
        self.assertEqual(len(records), 5)
        for record in records:
            self.assertEqual(resource(record), "catalogs")
            self.assertIn(resource_id(record), names)

        status, schemas, err = pipeline([["list", "catalogs"], ["list", "schemas"]], env)
        self.assertEqual(status, 0, f"list schemas stderr: {err}")
        records = ndjson(schemas)
        self.assertTrue(records)
        for record in records:
            self.assertEqual(resource(record), "schemas")
            self.assertIn(fqn(record).split(".")[0], names)
        self.assertEqual(
            [resource_id(record) for record in records if fqn(record).startswith("dbx_samples.")],
            DBX_SAMPLES_SCHEMAS,
        )

    # `list schemas` reads the live catalog's namespaces through Iceberg REST.
    def test_schema_ls_lists_the_live_iceberg_rest_namespaces(self):
        host = self.require_host()
        token = self.require_token(host)
        env = {
            "PQB_ENDPOINT": iceberg_endpoint(host),
            "PQB_TOKEN": token,
            "PQB_TABLE_FORMAT": "iceberg",
        }
        records = ndjson(
            self.assert_ok(
                ["list", "schemas"], stdin=catalog_ref("dbx_samples"), env=env
            )
        )
        names = []
        for record in records:
            self.assertEqual(resource(record), "schemas")
            self.assertEqual(fqn(record), f"dbx_samples.{resource_id(record)}")
            names.append(resource_id(record))
        self.assertEqual(names, DBX_SAMPLES_SCHEMAS)

    # A missing bearer is rejected by Unity with 401.
    def test_metastore_get_rejects_a_missing_token(self):
        host = self.require_host()
        stderr = self.assert_fails(
            ["get", "metastore"], env={"PQB_ENDPOINT": unity_endpoint(host)}
        )
        self.assertIn("401", stderr)

    # An invalid bearer is rejected by Unity with 401.
    def test_metastore_get_rejects_an_invalid_token(self):
        host = self.require_host()
        stderr = self.assert_fails(
            ["get", "metastore"],
            env={"PQB_ENDPOINT": unity_endpoint(host), "PQB_TOKEN": "not-a-real-token"},
        )
        self.assertIn("401", stderr)


def aws_region() -> str:
    return environment("DBX_AWS_REGION") or "us-east-2"


if __name__ == "__main__":
    unittest.main()
