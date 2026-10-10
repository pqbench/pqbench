#!/usr/bin/env python3
"""Report rust-code-analysis metrics on a pull request.

Two views over the per-file JSON the CLI writes:

    rust-code-analysis-cli --metrics -O json --pr -o METRICS_DIR -p PATH…

  annotate METRICS_DIR
      Print GitHub `::warning file=…,line=…::…` workflow commands for every
      function over the complexity ceiling or under the maintainability floor.
      Tests and generated files are measured but not gated.

  comment HEAD_DIR [BASE_DIR]
      Print a Markdown table of per-file metrics — with base→head deltas when a
      BASE_DIR is given — for a sticky pull-request comment.

The ceilings are data, kept in one place: a function is flagged above
``CYCLOMATIC_CEILING`` or below ``MAINTAINABILITY_FLOOR``. Report-only: the
script never fails on a metric's value.
"""

import json
import sys
from pathlib import Path

CYCLOMATIC_CEILING = 15
MAINTAINABILITY_FLOOR = 20


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


def metric(space, group, key):
    """One metric value, or 0.0 when the document does not carry it."""
    try:
        return float(space["metrics"][group][key])
    except (KeyError, TypeError, ValueError):
        return 0.0


def functions(doc):
    """Every function space of a file, however deeply nested."""
    pending = list(doc.get("spaces") or [])
    while pending:
        space = pending.pop()
        pending.extend(space.get("spaces") or [])
        if space.get("kind") == "function":
            yield space


def gated(path):
    """Whether a path's functions count against the gate.

    Tests and generated sources (the docscheck ``gen_*.rs`` transcripts) are
    measured — they appear in the summary and the comment — but their
    complexity is not a signal worth annotating a reviewer about.
    """
    parts = path.split("/")
    name = parts[-1]
    return "tests" not in parts and not name.startswith("gen_")


def escaped(text):
    """A workflow-command message with %, CR, LF escaped (GitHub's rules)."""
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def annotate(metrics_dir):
    """Annotations for functions over complexity, files under maintainability."""
    for doc in documents(metrics_dir):
        if not gated(doc["name"]):
            continue
        # The maintainability index is file-level: it falls with size and
        # structure, while a single function's index is almost always high.
        maintainability = metric(doc, "mi", "mi_visual_studio")
        if maintainability < MAINTAINABILITY_FLOOR:
            print(
                f"::warning file={doc['name']},line=1,"
                f"title=Low maintainability index::"
                f"file maintainability index {maintainability:.0f} "
                f"(floor {MAINTAINABILITY_FLOOR}, Visual Studio scale)"
            )
        for function in functions(doc):
            complexity = metric(function, "cyclomatic", "sum")
            if complexity > CYCLOMATIC_CEILING:
                name = function.get("name") or "<anonymous>"
                line = function.get("start_line") or 1
                print(
                    f"::warning file={doc['name']},line={line},"
                    f"title=High cyclomatic complexity::"
                    f"{escaped(name)} is cyclomatic complexity {complexity:.0f} "
                    f"(ceiling {CYCLOMATIC_CEILING})"
                )


def file_metrics(doc):
    """A file's rollup: SLOC, worst function's cyclomatic, maintainability."""
    return (
        metric(doc, "loc", "sloc"),
        metric(doc, "cyclomatic", "max"),
        metric(doc, "mi", "mi_visual_studio"),
    )


def count_cell(head, base, worse_up=False):
    """An integer metric with its head→base delta; regressions in bold."""
    if head is None:
        return "—"
    if base is None:
        return f"{head:.0f} (new)"
    delta = head - base
    if round(delta) == 0:
        return f"{head:.0f}"
    cell = f"{head:.0f} ({delta:+.0f})"
    return f"**{cell}**" if worse_up and delta > 0 else cell


def index_cell(head, base):
    """The maintainability index with its delta; a drop is bold."""
    if head is None:
        return "—"
    if base is None:
        return f"{head:.1f} (new)"
    delta = head - base
    if round(delta, 1) == 0:
        return f"{head:.1f}"
    cell = f"{head:.1f} ({delta:+.1f})"
    return f"**{cell}**" if delta < 0 else cell


def comment(head_dir, base_dir, label):
    """A Markdown delta table for a sticky pull-request comment."""
    head = {doc["name"]: doc for doc in documents(head_dir)}
    base = {doc["name"]: doc for doc in documents(base_dir)} if base_dir else {}
    names = sorted(set(head) | set(base))

    print("### Code analysis")
    print()
    if not names:
        print("_No Rust files changed._")
        return
    print(f"{len(names)} changed Rust file(s), against `{label}`:")
    print()
    print("| File | SLOC | Cyclomatic (max) | MI |")
    print("| --- | ---: | ---: | ---: |")
    for name in names:
        h = file_metrics(head[name]) if name in head else (None, None, None)
        b = file_metrics(base[name]) if name in base else (None, None, None)
        print(
            f"| `{name}` "
            f"| {count_cell(h[0], b[0])} "
            f"| {count_cell(h[1], b[1], worse_up=True)} "
            f"| {index_cell(h[2], b[2])} |"
        )
    print()
    print(
        f"Regressions in **bold**. Functions above cyclomatic "
        f"{CYCLOMATIC_CEILING} and files below maintainability index "
        f"{MAINTAINABILITY_FLOOR} are annotated on the diff."
    )


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "annotate":
        annotate(sys.argv[2])
        return
    if len(sys.argv) == 5 and sys.argv[1] == "comment":
        comment(sys.argv[2], sys.argv[3] or None, sys.argv[4])
        return
    sys.exit(
        "usage: code_analysis.py annotate METRICS_DIR\n"
        "       code_analysis.py comment HEAD_DIR BASE_DIR BASE_LABEL"
    )


if __name__ == "__main__":
    main()
