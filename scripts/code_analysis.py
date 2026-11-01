#!/usr/bin/env python3
"""Report cyclomatic complexity on a pull request.

Two views over the per-file JSON the CLI writes:

    rust-code-analysis-cli --metrics -O json --pr -o METRICS_DIR -p PATH…

  annotate HEAD_DIR [BASE_DIR]
      Print GitHub ``::notice`` workflow commands for functions that are new or
      whose own cyclomatic complexity rose above the ceiling. With no BASE_DIR,
      report every function over the ceiling.

  comment HEAD_DIR BASE_DIR LABEL
      Print the sticky pull-request comment: the new or worsened functions over
      the ceiling in the changed files. Tests and generated files are excluded.

The function's own cyclomatic complexity is ``cyclomatic.max``, not the space's
``cyclomatic.sum``: the sum folds nested closures into the enclosing function,
so the table and the annotation would report different numbers for the same
function. This is a review prompt, not a gate: extracting a helper lowers a
function's maximum without reducing the total complexity. Report-only: the
script never fails on a metric's value.
"""

import json
import sys
from pathlib import Path

CYCLOMATIC_CEILING = 15
MAX_FINDINGS = 10


def documents(metrics_dir):
    """The parseable per-file metric documents under ``metrics_dir``."""
    docs = []
    for path in sorted(Path(metrics_dir).rglob("*.json")):
        try:
            doc = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if doc.get("name") and isinstance(doc.get("metrics"), dict):
            docs.append(doc)
    return docs


def documents_by_name(metrics_dir):
    """The per-file documents under ``metrics_dir``, keyed by source path."""
    return {doc["name"]: doc for doc in documents(metrics_dir)}


def metric(space, group, key):
    """One metric value, or 0.0 when the document does not carry it."""
    try:
        return float(space["metrics"][group][key])
    except (KeyError, TypeError, ValueError):
        return 0.0


def functions(doc):
    """Every named function of a file, with its enclosing type as a prefix.

    A closure is a function space too, but ``<anonymous>`` is not a unit a
    reviewer can point at, so closures are not reported separately. Nested
    named items (a ``fn`` inside a body) are rare and are left out.
    """
    found = []

    def walk(space, prefix):
        kind = space.get("kind")
        name = space.get("name") or ""
        if kind == "function":
            if name and name != "<anonymous>":
                found.append(
                    {
                        "name": f"{prefix}::{name}" if prefix else name,
                        "line": space.get("start_line") or 1,
                        "complexity": metric(space, "cyclomatic", "max"),
                    }
                )
            return
        if kind in ("impl", "trait", "mod") and name:
            namespace = f"{prefix}::{name}" if prefix else name
        else:
            namespace = prefix
        for child in space.get("spaces") or []:
            walk(child, namespace)

    for space in doc.get("spaces") or []:
        walk(space, "")
    return found


def gated(path):
    """Whether a path's functions are reported.

    Tests and generated sources (the docscheck ``gen_*.rs`` transcripts) are
    measured with the changed files but are not a signal for a reviewer.
    """
    parts = path.split("/")
    name = parts[-1]
    return "tests" not in parts and not name.startswith("gen_")


def escaped(text):
    """A workflow-command message with %, CR, LF escaped (GitHub's rules)."""
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def take_function(candidates, name):
    """Remove and return the first function named ``name``, or None."""
    for index, function in enumerate(candidates):
        if function["name"] == name:
            return candidates.pop(index)
    return None


def findings(head, base, differential):
    """Functions over the ceiling that are new or worse than the base.

    ``head`` and ``base`` are documents keyed by source path. With no base
    revision (``differential`` false), every function over the ceiling is a
    finding; with one, an unchanged function is not.
    """
    found = []
    for path, doc in sorted(head.items()):
        if not gated(path):
            continue
        base_functions = functions(base[path]) if path in base else []
        for function in functions(doc):
            if function["complexity"] <= CYCLOMATIC_CEILING:
                continue
            before = None
            if differential:
                match = take_function(base_functions, function["name"])
                before = match["complexity"] if match else None
                if before is not None and function["complexity"] <= before:
                    continue
            found.append(
                {
                    "path": path,
                    "name": function["name"],
                    "line": function["line"],
                    "before": before,
                    "after": function["complexity"],
                }
            )
    found.sort(
        key=lambda finding: (
            -(finding["after"] - (finding["before"] or 0.0)),
            finding["path"],
            finding["line"],
        )
    )
    return found


def annotate(head_dir, base_dir):
    """Advisory notices for new or worsened functions over the ceiling."""
    differential = bool(base_dir)
    head = documents_by_name(head_dir)
    base = documents_by_name(base_dir) if differential else {}
    for finding in findings(head, base, differential):
        name = finding["name"]
        line = finding["line"]
        after = finding["after"]
        if not differential:
            title = "Function over complexity ceiling"
            change = f"is cyclomatic complexity {after:.0f} (ceiling {CYCLOMATIC_CEILING})"
        elif finding["before"] is None:
            title = "New function over complexity ceiling"
            change = f"is new at cyclomatic complexity {after:.0f} (ceiling {CYCLOMATIC_CEILING})"
        else:
            before = finding["before"]
            title = "Cyclomatic complexity increased"
            change = (
                f"rose {before:.0f} → {after:.0f} (+{after - before:.0f}) "
                f"(ceiling {CYCLOMATIC_CEILING})"
            )
        print(
            f"::notice file={finding['path']},line={line},"
            f"title={title}::{escaped(name)} {change}"
        )


def complexity_cell(finding):
    """A finding's base→head complexity, or ``new`` when there is no base."""
    after = finding["after"]
    if finding["before"] is None:
        return f"{after:.0f} (new)"
    before = finding["before"]
    return f"{before:.0f} → {after:.0f} (+{after - before:.0f})"


def comment(head_dir, base_dir, label):
    """A Markdown list of new findings for a sticky pull-request comment."""
    differential = bool(base_dir)
    head = documents_by_name(head_dir)
    base = documents_by_name(base_dir) if differential else {}
    names = set(head) | set(base)

    print("### Complexity prompt")
    print()
    if not names:
        print("_No Rust files changed._")
        return

    found = findings(head, base, differential)
    if not found:
        if differential:
            print(
                f"_No new functions over cyclomatic "
                f"{CYCLOMATIC_CEILING}._"
            )
        else:
            print(f"_No functions over cyclomatic {CYCLOMATIC_CEILING}._")
        return

    count = len(found)
    noun = "finding" if count == 1 else "findings"
    if differential:
        print(f"{count} new {noun} on changed Rust functions, against `{label}`:")
    else:
        print(f"{count} {noun} over cyclomatic {CYCLOMATIC_CEILING}:")
    print()
    print("| Function | File | Cyclomatic |")
    print("| --- | --- | --- |")
    for finding in found[:MAX_FINDINGS]:
        print(
            f"| `{finding['name']}` "
            f"| `{finding['path']}:{finding['line']}` "
            f"| {complexity_cell(finding)} |"
        )
    if count > MAX_FINDINGS:
        print()
        print(
            f"…and {count - MAX_FINDINGS} more; see the code-analysis job log."
        )
    print()
    if differential:
        print(
            "Only new or worsened functions in changed files are listed. This "
            "is a review prompt, not a gate: extracting a helper lowers the "
            "maximum without reducing total complexity. Tests and generated "
            "sources are excluded."
        )
    else:
        print("Tests and generated sources are excluded.")


def main():
    if len(sys.argv) in (3, 4) and sys.argv[1] == "annotate":
        base_dir = sys.argv[3] if len(sys.argv) == 4 else ""
        annotate(sys.argv[2], base_dir or None)
        return
    if len(sys.argv) == 5 and sys.argv[1] == "comment":
        comment(sys.argv[2], sys.argv[3] or None, sys.argv[4])
        return
    sys.exit(
        "usage: code_analysis.py annotate HEAD_DIR [BASE_DIR]\n"
        "       code_analysis.py comment HEAD_DIR BASE_DIR BASE_LABEL"
    )


if __name__ == "__main__":
    main()
