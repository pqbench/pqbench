"""Blackbox tests for the PyO3 command bindings."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pqbench

_ROOT = Path(__file__).resolve().parents[2]
_PARQUET = _ROOT / "crates" / "pqbench-cli" / "tests" / "fixtures" / "small_reddit_none.parquet"
_TABLE = _ROOT / "docker" / "e2e-lakehouse" / "table"


class BindingsTest(unittest.TestCase):
    def test_commands_match_the_cli(self) -> None:
        self.assertEqual(
            pqbench.commands,
            (
                "lz",
                "compression",
                "bytemass",
                "profile",
                "experiment",
                "diff",
                "ratelimit",
                "setup",
                "skill",
                "viz",
                "get_metastore",
                "get_catalog",
                "get_schema",
                "get_table",
                "list_catalogs",
                "list_schemas",
                "list_tables",
                "list_partitions",
                "list_files",
                "credentials_get",
                "credentials_check",
            ),
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

    def test_profile_streams_column_facts(self) -> None:
        columns = pqbench.profile(str(_PARQUET), rows="first:32", top=5)
        self.assertIsInstance(columns, list)
        self.assertGreaterEqual(len(columns), 1)
        self.assertIn("column", columns[0])
        self.assertIn("null_fraction", columns[0])
        self.assertIn("ndv", columns[0])
        self.assertIn("id", columns[0])
        filtered = pqbench.profile(str(_PARQUET), columns=["text"], rows="first:8")
        self.assertEqual([column["column"] for column in filtered], ["text"])

    def test_profile_rejects_a_bad_rows_method(self) -> None:
        with self.assertRaises(ValueError) as caught:
            pqbench.profile(str(_PARQUET), rows="last:5")
        self.assertIn("rows", str(caught.exception))

    def test_experiment_streams_trials(self) -> None:
        trials = pqbench.experiment(
            str(_PARQUET),
            rewrite=["sort:text"],
            rows="first:512",
        )
        self.assertIsInstance(trials, list)
        self.assertGreaterEqual(len(trials), 2)
        names = [trial["name"] for trial in trials]
        self.assertEqual(names[0], "control")
        self.assertIn("sort:text", names)
        self.assertIn("bytes", trials[0])
        self.assertIn("columns", trials[0])

    def test_experiment_rejects_an_unknown_rewrite(self) -> None:
        with self.assertRaises(RuntimeError) as caught:
            pqbench.experiment(str(_PARQUET), rewrite=["correlate:text"])
        self.assertIn("rewrite", str(caught.exception))

    def test_diff_measures_two_inputs(self) -> None:
        deltas = pqbench.diff(str(_PARQUET), str(_PARQUET))
        self.assertGreaterEqual(len(deltas), 1)
        self.assertEqual(deltas[0]["kind"], "pqbench.diff-column")
        self.assertIn("column", deltas[0])
        self.assertEqual(deltas[0]["delta_bytes"], 0)

    def test_diff_compares_row_lists(self) -> None:
        rows = pqbench.bytemass(str(_PARQUET))
        deltas = pqbench.diff(rows, rows, depth=1)
        self.assertGreaterEqual(len(deltas), 1)
        self.assertEqual(deltas[0]["delta_bytes"], 0)

    def test_diff_rejects_depth_zero(self) -> None:
        with self.assertRaises(RuntimeError) as caught:
            pqbench.diff(str(_PARQUET), str(_PARQUET), depth=0)
        self.assertIn("depth", str(caught.exception))

    def test_ratelimit_passes_records_through(self) -> None:
        records = [{"kind": "pqbench.table", "name": "catalogs/c"}]
        self.assertEqual(pqbench.ratelimit(records, rate=0), records)

    def test_ratelimit_rejects_a_negative_rate(self) -> None:
        with self.assertRaises(ValueError):
            pqbench.ratelimit([{}], rate=-1)

    def test_setup_exports_the_configuration(self) -> None:
        env = pqbench.setup(
            endpoint="https://example.test",
            token="secret",
            table_format="unity",
            region="us-east-1",
        )
        self.assertEqual(env["PQB_ENDPOINT"], "https://example.test")
        self.assertEqual(env["PQB_TOKEN"], "secret")
        self.assertEqual(env["PQB_TABLE_FORMAT"], "unity")
        self.assertEqual(env["AWS_REGION"], "us-east-1")
        self.assertEqual(env["AWS_EC2_METADATA_DISABLED"], "true")

    def test_setup_needs_a_token(self) -> None:
        blank = {"PQB_TOKEN": "", "DATABRICKS_TOKEN": ""}
        with mock.patch.dict(os.environ, blank):
            with self.assertRaises(ValueError) as caught:
                pqbench.setup(endpoint="https://example.test")
            self.assertIn("token", str(caught.exception))

    def test_skill_returns_the_advisor(self) -> None:
        listed = pqbench.skill()
        self.assertTrue(any(item["name"] == "parquet-advisor" for item in listed))
        body = pqbench.skill("parquet-advisor")
        self.assertIn("pqbench experiment", body)
        recipes = pqbench.skill("parquet-advisor", "recipes")
        self.assertIn("zstd@3", recipes)

    def test_list_partitions_groups_the_log(self) -> None:
        partitions = pqbench.list_partitions(str(_TABLE))
        self.assertGreaterEqual(len(partitions), 1)
        self.assertTrue(
            partitions[0]["name"].startswith("catalogs/local/schemas/local/tables/")
        )
        self.assertEqual(partitions[0]["commit_versions"], [0])
        self.assertIn("commit_info", partitions[0])

    def test_list_partitions_rejects_a_bad_window(self) -> None:
        with self.assertRaises(ValueError) as caught:
            pqbench.list_partitions(str(_TABLE), every="1y")
        self.assertIn("every", str(caught.exception))

    def test_list_files_names_the_blobs(self) -> None:
        partitions = pqbench.list_partitions(str(_TABLE))
        files = pqbench.list_files(partitions[0])
        self.assertEqual(len(files), 1)
        self.assertTrue(files[0]["name"].endswith(".parquet"))
        self.assertTrue(files[0]["blob"].startswith("blobs/"))
        # The files stage leaves the size to bytemass, which reads the footer.
        self.assertEqual(files[0]["size_bytes"], 0)

    def test_list_files_rejects_a_table_file(self) -> None:
        with self.assertRaises(ValueError):
            pqbench.list_files({"kind": "pqbench.table-file", "id": "t"})

    def test_credentials_check_passes_iceberg_refs(self) -> None:
        ref = {"name": "catalogs/c/schemas/s/tables/t"}
        self.assertEqual(
            pqbench.credentials_check(
                [ref],
                endpoint="http://127.0.0.1:1",
                table_format="iceberg",
            ),
            [ref],
        )

    def test_get_requires_an_endpoint(self) -> None:
        with self.assertRaises(ValueError) as caught:
            pqbench.get_metastore(endpoint="")
        self.assertIn("endpoint", str(caught.exception))

    def test_version_is_the_crate_version(self) -> None:
        self.assertRegex(pqbench.__version__, r"^\d+\.\d+\.\d+$")

    def test_viz_collects_bytemass_rows(self) -> None:
        rows = pqbench.bytemass(str(_PARQUET))
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "report"
            paths = pqbench.viz(rows, output=str(prefix))
            html = Path(paths["html"])
            page = html.read_text()
            self.assertTrue(page.startswith("<!DOCTYPE html>"))
            self.assertIn("d3-hierarchy@3", page)

    def test_viz_rejects_a_table_file(self) -> None:
        with self.assertRaises(ValueError) as caught:
            pqbench.viz({"kind": "pqbench.table-file", "id": "t"}, output="report")
        self.assertIn("bytemass", str(caught.exception))

    def test_missing_input_fails(self) -> None:
        with self.assertRaises(RuntimeError):
            pqbench.lz("/nonexistent/pqbench-input")
        with self.assertRaises(RuntimeError):
            pqbench.list_partitions("/nonexistent/pqbench-table")


if __name__ == "__main__":
    unittest.main()
