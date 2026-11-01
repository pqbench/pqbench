#!/usr/bin/env python3
"""Tests for the pull-request code-analysis reporter."""

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import code_analysis


def function(name, line, complexity, sum_complexity=None):
    """A rust-code-analysis function space with its own and summed CC."""
    return {
        "kind": "function",
        "name": name,
        "start_line": line,
        "end_line": line + 10,
        "metrics": {
            "cyclomatic": {
                "max": float(complexity),
                "sum": float(complexity if sum_complexity is None else sum_complexity),
            }
        },
    }


def document(path, *spaces):
    """A rust-code-analysis file document."""
    return {"name": path, "metrics": {}, "spaces": list(spaces)}


def impl(name, *spaces):
    """An impl space containing functions."""
    return {"kind": "impl", "name": name, "spaces": list(spaces)}


def write(directory, *documents):
    """Write documents as the per-file JSON the CLI produces."""
    for doc in documents:
        path = Path(directory, doc["name"] + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(doc))


def run_comment(head, base, label="main"):
    """Run the comment view and return its Markdown."""
    with tempfile.TemporaryDirectory() as head_dir, tempfile.TemporaryDirectory() as base_dir:
        write(head_dir, *head)
        write(base_dir, *base)
        out = io.StringIO()
        with redirect_stdout(out):
            code_analysis.comment(head_dir, base_dir, label)
        return out.getvalue()


class FindingsTest(unittest.TestCase):
    def test_reports_a_new_function(self):
        head = {"crates/x.rs": document("crates/x.rs", function("dispatch", 10, 18))}
        base = {"crates/x.rs": document("crates/x.rs")}
        [finding] = code_analysis.findings(head, base, True)
        self.assertEqual(finding["name"], "dispatch")
        self.assertIsNone(finding["before"])
        self.assertEqual(finding["after"], 18.0)

    def test_reports_a_worsened_function(self):
        head = {"crates/x.rs": document("crates/x.rs", function("dispatch", 10, 18))}
        base = {"crates/x.rs": document("crates/x.rs", function("dispatch", 8, 17))}
        [finding] = code_analysis.findings(head, base, True)
        self.assertEqual(finding["before"], 17.0)
        self.assertEqual(finding["after"], 18.0)

    def test_catches_and_clears_the_pr_160_add_regression(self):
        base = {
            "crates/x.rs": document(
                "crates/x.rs", impl("Fold", function("add", 43, 11))
            )
        }
        regressed = {
            "crates/x.rs": document(
                "crates/x.rs", impl("Fold", function("add", 49, 20))
            )
        }
        refactored = {
            "crates/x.rs": document(
                "crates/x.rs", impl("Fold", function("add", 168, 10))
            )
        }
        [finding] = code_analysis.findings(regressed, base, True)
        self.assertEqual(finding["name"], "Fold::add")
        self.assertEqual(finding["before"], 11.0)
        self.assertEqual(finding["after"], 20.0)
        self.assertEqual(code_analysis.findings(refactored, base, True), [])

    def test_ignores_an_unchanged_function(self):
        head = {"crates/x.rs": document("crates/x.rs", function("dispatch", 10, 18))}
        base = {"crates/x.rs": document("crates/x.rs", function("dispatch", 9, 18))}
        self.assertEqual(code_analysis.findings(head, base, True), [])

    def test_ignores_an_improved_function_still_over_the_ceiling(self):
        head = {"crates/x.rs": document("crates/x.rs", function("dispatch", 10, 16))}
        base = {"crates/x.rs": document("crates/x.rs", function("dispatch", 8, 17))}
        self.assertEqual(code_analysis.findings(head, base, True), [])

    def test_uses_the_functions_own_complexity_not_the_sum(self):
        # 31 is the sum over the function and its closures; 13 is the function.
        head = {
            "crates/x.rs": document(
                "crates/x.rs", function("select_files", 10, 13, sum_complexity=31)
            )
        }
        base = {"crates/x.rs": document("crates/x.rs")}
        self.assertEqual(code_analysis.findings(head, base, True), [])

    def test_excludes_tests_and_generated_sources(self):
        head = {
            "crates/x/tests/a.rs": document(
                "crates/x/tests/a.rs", function("test_a", 10, 20)
            ),
            "crates/x/tests/gen_docs.rs": document(
                "crates/x/tests/gen_docs.rs", function("test_docs", 10, 20)
            ),
        }
        self.assertEqual(code_analysis.findings(head, {}, True), [])

    def test_qualifies_a_method_with_its_impl(self):
        head = {
            "crates/x.rs": document(
                "crates/x.rs", impl("FileSelection", function("select", 10, 18))
            )
        }
        [finding] = code_analysis.findings(head, {}, True)
        self.assertEqual(finding["name"], "FileSelection::select")

    def test_without_a_base_reports_every_function_over_the_ceiling(self):
        head = {"crates/x.rs": document("crates/x.rs", function("dispatch", 10, 18))}
        [finding] = code_analysis.findings(head, {}, False)
        self.assertIsNone(finding["before"])


class CommentTest(unittest.TestCase):
    def test_lists_only_new_findings(self):
        head = [
            document(
                "crates/x.rs",
                function("dispatch", 10, 18),
                function("old", 30, 20),
            )
        ]
        base = [
            document(
                "crates/x.rs",
                function("dispatch", 8, 17),
                function("old", 28, 20),
            )
        ]
        text = run_comment(head, base)
        self.assertIn("`dispatch`", text)
        self.assertIn("17 → 18 (+1)", text)
        self.assertNotIn("`old`", text)
        self.assertNotIn("SLOC", text)
        self.assertNotIn("MI", text)

    def test_says_so_when_nothing_is_new(self):
        head = [document("crates/x.rs", function("dispatch", 10, 18))]
        base = [document("crates/x.rs", function("dispatch", 9, 18))]
        text = run_comment(head, base)
        self.assertIn("No new functions over cyclomatic 15.", text)

    def test_says_so_when_no_rust_file_changed(self):
        text = run_comment([], [])
        self.assertIn("No Rust files changed.", text)

    def test_caps_a_long_list(self):
        found = [function(f"f{index}", index + 1, 16) for index in range(12)]
        text = run_comment([document("crates/x.rs", *found)], [])
        self.assertIn("12 new findings", text)
        self.assertIn("…and 2 more", text)

    def test_excludes_tests(self):
        head = [
            document(
                "crates/x/tests/a.rs",
                function("test_a", 10, 20),
            )
        ]
        text = run_comment(head, [])
        self.assertIn("No new functions over cyclomatic 15.", text)


class AnnotateTest(unittest.TestCase):
    def run_annotate(self, head, base):
        with tempfile.TemporaryDirectory() as head_dir, tempfile.TemporaryDirectory() as base_dir:
            write(head_dir, *head)
            write(base_dir, *base)
            out = io.StringIO()
            with redirect_stdout(out):
                code_analysis.annotate(head_dir, base_dir)
            return out.getvalue()

    def test_reports_a_delta(self):
        text = self.run_annotate(
            [document("crates/x.rs", function("dispatch", 10, 18))],
            [document("crates/x.rs", function("dispatch", 8, 17))],
        )
        self.assertIn("::notice", text)
        self.assertNotIn("::warning", text)
        self.assertIn("title=Cyclomatic complexity increased", text)
        self.assertIn("dispatch rose 17 → 18 (+1) (ceiling 15)", text)

    def test_reports_a_new_function(self):
        text = self.run_annotate(
            [document("crates/x.rs", function("dispatch", 10, 18))],
            [document("crates/x.rs")],
        )
        self.assertIn("title=New function over complexity ceiling", text)
        self.assertIn("dispatch is new at cyclomatic complexity 18", text)

    def test_ignores_the_sum(self):
        text = self.run_annotate(
            [
                document(
                    "crates/x.rs",
                    function("select_files", 10, 13, sum_complexity=31),
                )
            ],
            [document("crates/x.rs")],
        )
        self.assertEqual(text, "")

    def test_without_a_base_reports_the_function(self):
        with tempfile.TemporaryDirectory() as head_dir:
            write(head_dir, document("crates/x.rs", function("dispatch", 10, 18)))
            out = io.StringIO()
            with redirect_stdout(out):
                code_analysis.annotate(head_dir, None)
            self.assertIn("dispatch is cyclomatic complexity 18 (ceiling 15)", out.getvalue())


if __name__ == "__main__":
    unittest.main()
