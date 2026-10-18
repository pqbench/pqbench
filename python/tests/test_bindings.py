"""Blackbox tests for the pqbench bindings.

``pqbench.native`` mirrors the ``pqbench`` library — every command is the
function of the same path — while ``pqbench`` is the ergonomic surface over it
(the client handles, and ``walk``). The walk verbs are awaitable and
``partition.list.list`` is an async iterator, so the tests are async
(``IsolatedAsyncioTestCase`` gives each one a running loop).
"""

from __future__ import annotations

import tempfile
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
_DAY_MILLIS = 86_400_000
# The resource name `v1::name::local_table(str(_TABLE))` builds for the fixture.
_TABLE_NAME = f"catalogs/local/schemas/local/tables/{_TABLE.name}"


def _workspace() -> pqbench.Workspace:
    """A workspace good enough for local reads: no catalog call is made."""
    return pqbench.Workspace(pqbench.Config(endpoint="http://127.0.0.1:1"))


class BindingsTest(unittest.IsolatedAsyncioTestCase):
    def test_surface_mirrors_the_crate(self) -> None:
        paths = [
            "metastore.get.read",
            "catalog.get.read",
            "catalog.list.list",
            "schema.get.read",
            "schema.list.list",
            "table.get.read",
            "table.list.list",
            "partition.list.list",
            "file.list.list",
            "credentials.check.check",
            "credentials.get.vend",
            "lz.lz",
            "compression.compression",
            "bytemass.bytemass",
            "profile.profile",
            "experiment.experiment",
            "diff.compare",
            "skill.skills",
            "skill.skill",
            "skill.document",
            "viz.write_report",
            "third_party.parquet.read_sample",
            "third_party.parquet.read_typed_sample",
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
        for name in ("metastore", "catalog", "table", "credentials", "lz", "file"):
            self.assertFalse(hasattr(pqbench, name), name)
        for name in ("Config", "Dialect", "Lake", "Layer", "Workspace"):
            self.assertTrue(hasattr(pqbench, name), name)
        self.assertTrue(callable(pqbench.walk))
        self.assertIn("walk", pqbench.__all__)

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

    def test_lz_report(self) -> None:
        report = pqbench.native.lz.lz(
            pqbench.native.LzRequest(
                file=_PARQUET,
                codec_specs=["snappy"],
                samples=1,
                warmup_iterations=0,
            )
        )
        self.assertEqual(len(report["rows"]), 1)
        self.assertEqual(report["rows"][0]["codec"], "snappy")

    def test_lz_rejects_an_unknown_codec(self) -> None:
        with self.assertRaises(RuntimeError):
            pqbench.native.lz.lz(
                pqbench.native.LzRequest(
                    file=_PARQUET,
                    codec_specs=["nonesuch"],
                    samples=1,
                    warmup_iterations=0,
                )
            )

    def test_compression_per_column(self) -> None:
        report = pqbench.native.compression.compression(
            pqbench.native.CompressionRequest(
                file=_PARQUET,
                codec_specs=["snappy"],
                samples=1,
                warmup_iterations=0,
                per_column=True,
            )
        )
        self.assertIn("rows", report)
        self.assertIn("columns", report)

    async def test_bytemass_streams_column_rows(self) -> None:
        rows = await pqbench.native.bytemass.bytemass(
            pqbench.Lake(_workspace()),
            pqbench.native.BytemassRequest(inputs=[str(_PARQUET)]),
        )
        self.assertGreaterEqual(len(rows), 1)
        self.assertIn("column", rows[0])
        self.assertIn("compressed_bytes", rows[0])
        self.assertIn("physical_type", rows[0])
        self.assertTrue(rows[0]["encodings"])
        self.assertTrue(any(row["column"] == "text" for row in rows))

    def test_profile_streams_column_facts(self) -> None:
        sample = pqbench.native.third_party.parquet.read_sample(_PARQUET, 32)
        profile = pqbench.native.profile.profile(
            sample, pqbench.native.ProfileRequest(columns=[], top=5)
        )
        self.assertGreaterEqual(len(profile["columns"]), 1)
        self.assertIn("column", profile["columns"][0])
        self.assertIn("null_fraction", profile["columns"][0])
        self.assertIn("ndv", profile["columns"][0])

    def test_experiment_streams_trials(self) -> None:
        sample = pqbench.native.third_party.parquet.read_typed_sample(_PARQUET, 512)
        experiment = pqbench.native.experiment.experiment(
            sample, pqbench.native.ExperimentRequest(trials=["sort:text"], aim=pqbench.native.Aim.ALL)
        )
        names = [trial["name"] for trial in experiment["trials"]]
        self.assertEqual(names[0], "control")
        self.assertIn("sort:text", names)

    async def test_diff_compares_row_lists(self) -> None:
        rows = await pqbench.native.bytemass.bytemass(
            pqbench.Lake(_workspace()),
            pqbench.native.BytemassRequest(inputs=[str(_PARQUET)]),
        )
        deltas = pqbench.native.diff.compare(rows, rows, 1)
        self.assertGreaterEqual(len(deltas), 1)
        self.assertEqual(deltas[0]["delta_bytes"], 0)

    def test_ratelimit_paces_by_kind(self) -> None:
        limit = pqbench.native.ratelimit.RateLimit(10.0)
        self.assertEqual(limit.delay("catalog"), 0.0)
        self.assertEqual(limit.delay("schema"), 0.0)
        self.assertGreater(limit.delay("catalog"), 0.0)

    async def test_list_partitions_groups_the_log(self) -> None:
        lake = pqbench.Lake(_workspace())
        partitions = [
            partition
            async for partition in pqbench.native.partition.list.list(
                lake, _TABLE_NAME, str(_TABLE), {}, _DAY_MILLIS
            )
        ]
        self.assertGreaterEqual(len(partitions), 1)
        self.assertTrue(
            partitions[0]["name"].startswith(f"{_TABLE_NAME}/partitions/"),
            partitions[0]["name"],
        )
        self.assertEqual(partitions[0]["commit_versions"], [0])
        self.assertIn("commit_info", partitions[0])

    async def test_list_files_names_the_paths(self) -> None:
        lake = pqbench.Lake(_workspace())
        partition = [
            partition
            async for partition in pqbench.native.partition.list.list(
                lake, _TABLE_NAME, str(_TABLE), {}, _DAY_MILLIS
            )
        ][0]
        files = [
            file
            async for file in pqbench.native.file.list.list(
                lake, partition["uri"], {}, partition["commit_versions"]
            )
        ]
        self.assertEqual(len(files), 1)
        # A record is its model's class: one field, three ways to read it.
        self.assertIsInstance(files[0], pqbench.native.TableFile)
        self.assertEqual(type(files[0]).__name__, "TableFile")
        self.assertEqual(files[0].uri, files[0]["uri"])
        self.assertEqual(files[0].asdict()["uri"], files[0]["uri"])
        self.assertEqual(list(files[0]), list(files[0].asdict()))
        self.assertIsNone(files[0].get("no_such_field"))
        with self.assertRaises(KeyError):
            files[0]["no_such_field"]
        self.assertTrue(files[0]["path"].endswith(".parquet"), files[0])
        self.assertTrue(files[0]["uri"], files[0])

    async def test_viz_writes_the_report(self) -> None:
        rows = await pqbench.native.bytemass.bytemass(
            pqbench.Lake(_workspace()),
            pqbench.native.BytemassRequest(inputs=[str(_PARQUET)]),
        )
        self.assertIsInstance(rows[0], pqbench.native.MassRow)
        self.assertEqual(rows[0].column, rows[0]["column"])
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "report"
            # A list of records goes back into a command unchanged.
            pqbench.native.viz.write_report(prefix, rows)
            page = prefix.with_suffix(".html").read_text()
            self.assertTrue(page.startswith("<!DOCTYPE html>"))
            self.assertIn("d3-hierarchy@3", page)

    def test_skill_returns_the_advisor(self) -> None:
        listed = pqbench.native.skill.skills()
        self.assertTrue(any(item["name"] == "parquet-advisor" for item in listed))
        body = pqbench.native.skill.skill("parquet-advisor")
        self.assertEqual(body["name"], "parquet-advisor")
        recipes = pqbench.native.skill.document("parquet-advisor", "recipes")
        self.assertIn("zstd@3", recipes)

    async def test_missing_input_fails(self) -> None:
        with self.assertRaises(RuntimeError):
            pqbench.native.lz.lz(pqbench.native.LzRequest(file=Path("/nonexistent/pqbench-input")))
        lake = pqbench.Lake(_workspace())
        with self.assertRaises(RuntimeError):
            async for _ in pqbench.native.partition.list.list(
                lake, "local", "/nonexistent/pqbench-table", {}, _DAY_MILLIS
            ):
                pass


if __name__ == "__main__":
    unittest.main()
