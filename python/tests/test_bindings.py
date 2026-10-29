"""Blackbox tests for the pqbench bindings.

``pqbench.native`` mirrors the ``pqbench`` library — every command is the
function of the same path — while ``pqbench`` is the ergonomic surface over it
(the client handles, and ``walk``). The walk verbs are awaitable and
``file.list.list`` is an async iterator, so the tests are async
(``IsolatedAsyncioTestCase`` gives each one a running loop).
"""

from __future__ import annotations

import unittest
from pathlib import Path

import pqbench

_ROOT = Path(__file__).resolve().parents[2]
_PARQUET = (
    _ROOT
    / "crates"
    / "pqbench-cli"
    / "tests"
    / "fixtures"
    / "small_reddit_none.parquet"
)
_TABLE = _ROOT / "docker" / "e2e-lakehouse" / "table"
# The resource name `v1::name::local_table(str(_TABLE))` builds for the fixture.
_TABLE_NAME = f"catalogs/local/schemas/local/tables/{_TABLE.name}"


def _workspace() -> pqbench.Workspace:
    """A workspace good enough for local reads: no catalog call is made."""
    return pqbench.Workspace(pqbench.Config(endpoint="http://127.0.0.1:1"))


class BindingsTest(unittest.IsolatedAsyncioTestCase):
    def test_surface_mirrors_the_crate(self) -> None:
        paths = [
            "metastore.get.get",
            "catalog.get.get",
            "catalog.list.list",
            "schema.get.get",
            "schema.list.list",
            "table.get.get",
            "table.list.list",
            "file.list.list",
            "credentials.check.check",
            "credentials.get.get",
            "bytemass.bytemass",
        ]
        for path in paths:
            target = pqbench.native
            for part in path.split("."):
                target = getattr(target, part)
            self.assertTrue(callable(target), path)

    def test_surface_keeps_the_handles_and_the_walk(self) -> None:
        # Every command lives under `pqbench.native`, once; `pqbench` carries
        # the handles the commands and `walk` take, and `walk` itself.
        self.assertTrue(callable(pqbench.native.catalog.list.list))
        for name in ("metastore", "catalog", "table", "credentials", "bytemass", "file"):
            self.assertFalse(hasattr(pqbench, name), name)
        for name in ("Config", "Dialect", "Lake", "Layer", "Workspace"):
            self.assertTrue(hasattr(pqbench, name), name)
        self.assertTrue(callable(pqbench.walk))
        self.assertIn("walk", pqbench.__all__)

    def test_surface_stops_at_bytemass(self) -> None:
        # The commands downstream of a byte-mass stream, the standalone codec
        # benchmarks and `skill` are separate tools: they are not bound.
        for name in ("lz", "compression", "profile", "experiment", "diff", "skill", "viz"):
            self.assertFalse(hasattr(pqbench.native, name), name)
            self.assertFalse(hasattr(pqbench, name), name)
        self.assertTrue(callable(pqbench.native.bytemass.bytemass))

    def test_config_and_workspace(self) -> None:
        config = pqbench.Config("https://example.test", token="secret")
        self.assertEqual(config.endpoint, "https://example.test")
        self.assertEqual(config.token, "secret")
        self.assertEqual(config.dialect, pqbench.Dialect.UNITY)
        workspace = pqbench.Workspace(config)
        self.assertEqual(workspace.config.endpoint, "https://example.test")
        self.assertIsInstance(pqbench.Lake(workspace), pqbench.Lake)

    async def test_walk_rejects_a_bad_bound(self) -> None:
        # The bounds are checked before any level is listed, so no endpoint is
        # needed and nothing is sent.
        workspace = _workspace()
        with self.assertRaises(ValueError):
            async for _ in pqbench.walk(workspace, concurrency=0):
                pass
        with self.assertRaises(ValueError):
            async for _ in pqbench.walk(workspace, window_millis=0):
                pass
        for rate in (-1.0, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                async for _ in pqbench.walk(workspace, requests_per_second=rate):
                    pass

    async def test_bytemass_streams_columns(self) -> None:
        columns = await pqbench.native.bytemass.bytemass(
            pqbench.Lake(_workspace()),
            pqbench.native.BytemassRequest(inputs=[str(_PARQUET)]),
        )
        self.assertGreaterEqual(len(columns), 1)
        self.assertIn("name", columns[0])
        self.assertIn("compressed_bytes", columns[0])
        self.assertTrue(columns[0]["encodings"])
        self.assertTrue(
            any("/columns/text/bytemass" in column["name"] for column in columns)
        )

    def test_ratelimit_paces_by_kind(self) -> None:
        limit = pqbench.native.ratelimit.RateLimit(10.0)
        self.assertEqual(limit.delay("catalog"), 0.0)
        self.assertEqual(limit.delay("schema"), 0.0)
        self.assertGreater(limit.delay("catalog"), 0.0)

    async def test_list_files_names_the_paths(self) -> None:
        lake = pqbench.Lake(_workspace())
        files = [
            file
            async for file in pqbench.native.file.list.list(
                lake, _TABLE_NAME, str(_TABLE), {}, None, None
            )
        ]
        self.assertEqual(len(files), 1)
        # A record is its model's class: one field, three ways to read it.
        self.assertIsInstance(files[0], pqbench.native.TableFile)
        self.assertEqual(type(files[0]).__name__, "TableFile")
        self.assertEqual(files[0].path, files[0]["path"])
        self.assertEqual(files[0].asdict()["path"], files[0]["path"])
        self.assertEqual(list(files[0]), list(files[0].asdict()))
        self.assertIsNone(files[0].get("no_such_field"))
        with self.assertRaises(KeyError):
            files[0]["no_such_field"]
        self.assertTrue(files[0]["path"].endswith(".parquet"), files[0])
        self.assertTrue(files[0]["name"].startswith(f"{_TABLE_NAME}/files/"), files[0])
        self.assertTrue(files[0].uri, files[0])

    async def test_missing_input_fails(self) -> None:
        lake = pqbench.Lake(_workspace())
        with self.assertRaises(RuntimeError):
            async for _ in pqbench.native.file.list.list(
                lake, "local", "/nonexistent/pqbench-table", {}, None, None
            ):
                pass


if __name__ == "__main__":
    unittest.main()
