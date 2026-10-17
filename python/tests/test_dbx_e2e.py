"""Live Databricks e2e for the walk, composed as a single-threaded generator.

The walk mirrors the CLI's pipeline — ``list tables | credentials check |
credentials get | get table | list partitions | list files`` — as one Python
generator: each stage feeds the next, and the whole chain runs on the calling
thread (the bindings drive a current-thread runtime per call, and the generator
adds no threads of its own).

Skipped unless an endpoint and bearer are configured:

* ``PQB_ENDPOINT`` / ``PQB_TOKEN``, or
* ``DBX_HOST`` with ``DBX_TOKEN``, or ``DBX_HOST`` with
  ``DBX_SAMPLES_SP_CLIENT_ID`` / ``DBX_SAMPLES_SP_CLIENT_SECRET`` (minted as an
  OAuth M2M bearer).

``DBX_WALK_SCHEMA`` (default ``pqbench_ext.tpch_sf1``) and ``DBX_AWS_REGION``
(default ``us-east-2``) point the walk at another external fixture.
"""

from __future__ import annotations

import base64
import functools
import json
import os
import threading
import unittest
import urllib.parse
import urllib.request

import pqbench


def _has_credentials() -> bool:
    if os.environ.get("PQB_ENDPOINT") and os.environ.get("PQB_TOKEN"):
        return True
    if not os.environ.get("DBX_HOST"):
        return False
    if os.environ.get("DBX_TOKEN"):
        return True
    return bool(
        os.environ.get("DBX_SAMPLES_SP_CLIENT_ID")
        and os.environ.get("DBX_SAMPLES_SP_CLIENT_SECRET")
    )


def _mint(host: str, client_id: str, client_secret: str) -> str:
    """An OAuth M2M bearer for the service principal."""
    body = urllib.parse.urlencode(
        {"grant_type": "client_credentials", "scope": "all-apis"}
    ).encode()
    request = urllib.request.Request(host.rstrip("/") + "/oidc/v1/token", data=body)
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    request.add_header("Authorization", f"Basic {basic}")
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)["access_token"]


@functools.lru_cache(maxsize=1)
def _live() -> tuple[str, str]:
    """The endpoint and bearer, from the environment; minted once if needed."""
    endpoint = os.environ.get("PQB_ENDPOINT")
    token = os.environ.get("PQB_TOKEN")
    if endpoint and token:
        return endpoint, token
    host = os.environ["DBX_HOST"]
    if os.environ.get("DBX_TOKEN"):
        return host, os.environ["DBX_TOKEN"]
    return host, _mint(
        host,
        os.environ["DBX_SAMPLES_SP_CLIENT_ID"],
        os.environ["DBX_SAMPLES_SP_CLIENT_SECRET"],
    )


def _aws_env(record: dict) -> dict:
    """A ref's vended lease as the ``AWS_*`` options a storage read needs."""
    credentials = record.get("credentials") or {}
    return {
        key: value
        for key, value in {
            "AWS_ACCESS_KEY_ID": credentials.get("access_key_id"),
            "AWS_SECRET_ACCESS_KEY": credentials.get("secret_access_key"),
            "AWS_SESSION_TOKEN": credentials.get("session_token"),
        }.items()
        if value
    }


def walk(endpoint: str, token: str, schema_fqn: str):
    """Walk one schema to its files, lazily, on the calling thread.

    Yields ``(level, record)`` for the metastore, the schema's catalog and
    schema, each readable table, its partitions, and their files.
    """
    catalog, schema = schema_fqn.split(".", 1)
    yield "metastore", pqbench.get_metastore(endpoint=endpoint, token=token)
    yield "catalog", pqbench.get_catalog(catalog, endpoint=endpoint, token=token)
    yield "schema", pqbench.get_schema(catalog, schema, endpoint=endpoint, token=token)
    listed = pqbench.list_tables(catalog, schema, endpoint=endpoint, token=token)
    readable = pqbench.credentials_check(listed, endpoint=endpoint, token=token)
    vended = pqbench.credentials_get(readable, endpoint=endpoint, token=token)
    for ref in vended:
        name = ref["name"].rsplit("/", 1)[-1]
        env = _aws_env(ref)
        table = pqbench.get_table(
            catalog, schema, name, env=env, endpoint=endpoint, token=token
        )
        yield "table", table
        uri = table.get("uri")
        if not uri:
            continue
        partitions = pqbench.list_partitions(
            uri, table=table["name"], env=env, endpoint=endpoint, token=token
        )
        for partition in partitions:
            yield "partition", partition
            for file in pqbench.list_files(partition, endpoint=endpoint, token=token):
                yield "file", file


@unittest.skipUnless(_has_credentials(), "no Databricks endpoint/token configured")
class WalkE2ETest(unittest.TestCase):
    schema = os.environ.get("DBX_WALK_SCHEMA") or "pqbench_ext.tpch_sf1"
    region = os.environ.get("DBX_AWS_REGION") or "us-east-2"

    def setUp(self) -> None:
        os.environ.setdefault("AWS_REGION", self.region)

    def test_catalogs_include_the_walk_catalog(self) -> None:
        endpoint, token = _live()
        catalog = self.schema.split(".", 1)[0]
        names = [item["name"] for item in pqbench.list_catalogs(endpoint=endpoint, token=token)]
        self.assertIn(f"catalogs/{catalog}", names)

    def test_walk_reaches_files(self) -> None:
        endpoint, token = _live()
        before = threading.active_count()
        levels: dict[str, int] = {}
        for level, record in walk(endpoint, token, self.schema):
            levels[level] = levels.get(level, 0) + 1
            if level == "file":
                self.assertTrue(record["blob"].startswith("blobs/"), record["blob"])
                break
        # The generator added no threads: the walk is single-threaded.
        self.assertEqual(threading.active_count(), before)
        for level in ("metastore", "catalog", "schema", "table", "partition", "file"):
            self.assertGreaterEqual(levels.get(level, 0), 1, f"{self.schema}: {levels}")

    def test_credentials_check_drops_managed_storage(self) -> None:
        endpoint, token = _live()
        external = {"name": "catalogs/pqbench_ext/schemas/nyctaxi/tables/trips"}
        managed = {"name": "catalogs/dbx_samples/schemas/nyctaxi/tables/pqbench_delta_test"}
        kept = pqbench.credentials_check(
            [managed, external], endpoint=endpoint, token=token
        )
        self.assertEqual([ref["name"] for ref in kept], [external["name"]])


if __name__ == "__main__":
    unittest.main()
