"""Blackbox tests for the PyO3 command bindings."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pqbench

_ROOT = Path(__file__).resolve().parents[2]
_PARQUET = _ROOT / "crates" / "pqbench-cli" / "tests" / "fixtures" / "small_reddit_none.parquet"
_TABLE = _ROOT / "docker" / "e2e-lakehouse" / "table"
_LAKE = _ROOT / "docker" / "e2e-lakehouse"


class BindingsTest(unittest.TestCase):
    def test_commands_match_the_cli(self) -> None:
        self.assertEqual(
            pqbench.commands,
            ("lz", "compression", "bytemass", "table", "lake", "dump", "profile", "experiment", "skill", "viz"),
        )
        for name in pqbench.commands:
            self.assertTrue(callable(getattr(pqbench, name)))

    def test_lz_json_report(self) -> None:
        report = pqbench.lz(
            _PARQUET,
            codecs=["snappy"],
            samples=1,
            warmup_iterations=0,
            json=True,
        )
        self.assertEqual(len(report["rows"]), 1)
        self.assertEqual(report["rows"][0]["codec"], "snappy")

    def test_lz_text_and_mean_mode(self) -> None:
        text = pqbench.lz(
            _PARQUET,
            codecs=["snappy"],
            samples=1,
            warmup_iterations=0,
            mode="mean",
        )
        self.assertIsInstance(text, str)
        self.assertIn("snappy", text)

    def test_lz_invalid_mode(self) -> None:
        with self.assertRaises(ValueError) as caught:
            pqbench.lz(_PARQUET, mode="median")
        self.assertIn("mean", str(caught.exception))

    def test_compression_json_report(self) -> None:
        report = pqbench.compression(
            _PARQUET,
            codecs=["snappy"],
            samples=1,
            warmup_iterations=0,
            per_column=True,
            json=True,
        )
        self.assertIn("rows", report)
        self.assertIn("columns", report)

    def test_bytemass_streams_column_rows(self) -> None:
        rows = pqbench.bytemass(str(_PARQUET))
        self.assertIsInstance(rows, list)
        self.assertGreaterEqual(len(rows), 1)
        self.assertIn("column", rows[0])
        self.assertIn("compressed_bytes", rows[0])
        self.assertIn("physical_type", rows[0])
        self.assertTrue(rows[0]["encodings"])
        self.assertTrue(any(row["column"] == "text" for row in rows))

    def test_bytemass_indexes_is_optional(self) -> None:
        rows = pqbench.bytemass(str(_PARQUET), indexes=True)
        self.assertGreaterEqual(len(rows), 1)
        self.assertIn("physical_type", rows[0])

    def test_bytemass_requires_an_input(self) -> None:
        with self.assertRaises(RuntimeError):
            pqbench.bytemass()

    def test_bytemass_rejects_a_non_aws_env_key(self) -> None:
        with self.assertRaises(ValueError) as caught:
            pqbench.bytemass(str(_PARQUET), env={"PATH": "/"})
        self.assertIn("AWS_", str(caught.exception))

    def test_table_loads_a_delta_snapshot(self) -> None:
        info = pqbench.table(str(_TABLE))
        self.assertEqual(info["kind"], "pqbench.table")
        self.assertEqual(info["format"], "delta")
        self.assertGreaterEqual(len(info["files"]), 1)

    def test_lake_lists_tables(self) -> None:
        lake = pqbench.lake(str(_LAKE))
        self.assertEqual(lake["kind"], "pqbench.lake")
        names = [table["name"] for table in lake["tables"]]
        self.assertTrue(any("table" in name for name in names), names)

    def test_dump_writes_parquet_bytes(self) -> None:
        blob = pqbench.dump(str(_PARQUET), row_groups="first:1")
        self.assertIsInstance(blob, bytes)
        self.assertTrue(blob.startswith(b"PAR1"))
        self.assertTrue(blob.endswith(b"PAR1"))

    def test_dump_reads_a_table_document(self) -> None:
        info = {
            "kind": "pqbench.table",
            "version": 1,
            "format": "delta",
            "uri": str(_PARQUET.parent),
            "snapshot_version": 0,
            "partition_columns": [],
            "log": [],
            "files": [
                {
                    "path": _PARQUET.name,
                    "uri": str(_PARQUET),
                    "size": _PARQUET.stat().st_size,
                }
            ],
        }
        blob = pqbench.dump(info, row_groups="first:1")
        self.assertTrue(blob.startswith(b"PAR1"))

    def test_profile_reads_a_parquet_sample(self) -> None:
        info = pqbench.profile(str(_PARQUET), rows="first:16")
        self.assertGreaterEqual(info["num_rows"], 1)
        self.assertTrue(any(column["column"] == "text" for column in info["columns"]))
        self.assertEqual(info["dependencies"], [])
        narrowed = pqbench.profile(
            str(_PARQUET),
            columns=["text", "label"],
            rows="first:16",
            dependencies=True,
        )
        self.assertEqual(len(narrowed["columns"]), 2)
        self.assertEqual(len(narrowed["dependencies"]), 1)

    def test_experiment_rewrites_a_parquet_sample(self) -> None:
        info = pqbench.experiment(str(_PARQUET), rows="first:16", rewrites=["codec:snappy"])
        self.assertGreaterEqual(info["num_rows"], 1)
        self.assertEqual(info["aim"], "storage")
        names = [trial["name"] for trial in info["trials"]]
        self.assertIn("control", names)
        self.assertIn("codec:snappy", names)

    def test_skill_returns_the_advisor(self) -> None:
        listed = pqbench.skill()
        self.assertTrue(any(item["name"] == "parquet-advisor" for item in listed))
        body = pqbench.skill("parquet-advisor")
        self.assertIn("pqbench experiment", body)
        recipes = pqbench.skill("parquet-advisor", "recipes")
        self.assertIn("zstd@3", recipes)

    def test_dump_rejects_a_lake_that_has_not_been_loaded(self) -> None:
        lake = pqbench.lake(str(_LAKE))
        with self.assertRaises(ValueError) as caught:
            pqbench.dump(lake)
        self.assertIn("table", str(caught.exception).lower())

    def test_version_is_the_crate_version(self) -> None:
        self.assertRegex(pqbench.__version__, r"^\d+\.\d+\.\d+$")

    def test_viz_collects_bytemass_rows(self) -> None:
        rows = pqbench.bytemass(str(_PARQUET))
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "report"
            paths = pqbench.viz(rows, output=str(prefix))
            sqlite = Path(paths["sqlite"])
            html = Path(paths["html"])
            self.assertTrue(sqlite.read_bytes().startswith(b"SQLite format 3"))
            page = html.read_text()
            self.assertTrue(page.startswith("<!DOCTYPE html>"))
            self.assertIn("sql.js", page)

    def test_viz_rejects_a_table_document(self) -> None:
        with self.assertRaises(ValueError) as caught:
            pqbench.viz({"kind": "pqbench.table", "version": 1}, output="report")
        self.assertIn("bytemass", str(caught.exception))

    def test_missing_input_fails(self) -> None:
        with self.assertRaises(RuntimeError):
            pqbench.lz("/nonexistent/pqbench-input")
        with self.assertRaises(RuntimeError):
            pqbench.table("/nonexistent/pqbench-table")


if __name__ == "__main__":
    unittest.main()
