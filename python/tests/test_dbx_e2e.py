"""Live Databricks e2e for the walk over the whole metastore.

``pqbench.walk`` composes the library's per-resource functions —
``catalog.list.list | schema.list.list | table.list.list | credentials.check.check |
credentials.get.vend_unity | table.get.read | partition.list.list |
file.list.list`` — as one async generator that multiplexes the tables and yields
their files. The catalog levels span the whole Unity Catalog; the table levels
descend the one catalog the principal may read outside compute
(``DBX_WALK_CATALOG``, default ``pqbench_ext``), because Unity refuses
``EXTERNAL USE SCHEMA`` on the sample and managed-default catalogs and
``credentials.check.check`` filters by eligibility, not by grant. The library
work runs on the shared tokio runtime and the walk adds no threads of its own.

Skipped unless an endpoint and bearer are configured:

* ``PQB_ENDPOINT`` / ``PQB_TOKEN``, or
* ``DBX_HOST`` with ``DBX_TOKEN``, or ``DBX_HOST`` with
  ``DBX_SAMPLES_SP_CLIENT_ID`` / ``DBX_SAMPLES_SP_CLIENT_SECRET`` (minted as an
  OAuth M2M bearer).

``DBX_WALK_CATALOG`` (default ``pqbench_ext``, the external fixture) and
``DBX_AWS_REGION`` (default ``us-east-2``) point the walk at another readable
catalog.
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


def _workspace() -> pqbench.Workspace:
    return pqbench.Workspace(pqbench.Config(*_live()))


@unittest.skipUnless(_has_credentials(), "no Databricks endpoint/token configured")
class WalkE2ETest(unittest.IsolatedAsyncioTestCase):
    catalog = os.environ.get("DBX_WALK_CATALOG") or "pqbench_ext"
    region = os.environ.get("DBX_AWS_REGION") or "us-east-2"

    def setUp(self) -> None:
        os.environ.setdefault("AWS_REGION", self.region)

    async def test_catalog_ls_includes_the_walk_catalog(self) -> None:
        names = [
            item["name"] for item in await pqbench.native.catalog.list.list(_workspace())
        ]
        self.assertIn(f"catalogs/{self.catalog}", names)

    async def test_walk_yields_the_files(self) -> None:
        workspace = _workspace()
        before = threading.active_count()
        files = [file async for file in pqbench.walk(workspace, catalog=self.catalog)]
        # The walk descended more than one table of the readable catalog.
        tables = {file["uri"].rsplit("/", 1)[0] for file in files}
        self.assertGreaterEqual(len(tables), 2, f"{self.catalog}: {len(tables)} tables")
        for file in files:
            self.assertTrue(file["path"].endswith(".parquet"), file)
            self.assertTrue(file["uri"], file)
        # The generator added no threads: the walk is single-threaded.
        self.assertEqual(threading.active_count(), before)

    async def test_check_drops_managed_storage(self) -> None:
        workspace = _workspace()
        external = await pqbench.native.credentials.check.check(
            workspace, "pqbench_ext", "nyctaxi", "trips"
        )
        managed = await pqbench.native.credentials.check.check(
            workspace, "dbx_samples", "nyctaxi", "pqbench_delta_test"
        )
        self.assertTrue(external.eligible)
        self.assertFalse(managed.eligible)
        self.assertIsNotNone(managed.reason)


if __name__ == "__main__":
    unittest.main()
