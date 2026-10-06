#!/usr/bin/env python3
"""Check observable CLI JSON shapes and their compatibility record."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = ROOT / "contracts" / "cli-output-shapes.json"
REFERENCE = ROOT / "docs" / "io-contracts.md"
SAMPLE = ROOT / "examples" / "quickstart.parquet"
COMMANDS = {
    "pqbench.bytemass": ["bytemass", str(SAMPLE), "--json"],
    "pqbench.profile": ["profile", str(SAMPLE), "--rows", "first:1", "--json"],
    "pqbench.lz": [
        "lz", str(SAMPLE), "-c", "zstd@1", "--samples", "1", "--warmup-iterations", "0", "--json",
    ],
    "pqbench.compression": [
        "compression", str(SAMPLE), "-c", "zstd@1", "--samples", "1", "--warmup-iterations", "0", "--json",
    ],
    "pqbench.diff": ["diff", str(SAMPLE), str(SAMPLE), "--format", "json"],
    "pqbench.skill": ["skill"],
}


def value_kind(value):
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    return "string"


def shape(value, prefix=""):
    """Record public JSON paths and types; values and array lengths may vary."""
    fields = {}
    for key, child in value.items():
        path = f"{prefix}.{key}" if prefix else key
        fields[path] = value_kind(child)
        if isinstance(child, dict):
            fields.update(shape(child, path))
    return fields


def observe(binary):
    families = {}
    for family, args in COMMANDS.items():
        result = subprocess.run([str(binary), *args], cwd=ROOT, capture_output=True, text=True, check=False)
        if result.returncode:
            raise RuntimeError(f"{family} failed: {result.stderr.strip()}")
        records = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
        if not records:
            raise RuntimeError(f"{family} emitted no records")
        version = records[0].get("version")
        if not isinstance(version, int) or isinstance(version, bool):
            raise RuntimeError(f"{family} has no integer version on its first record")
        kinds = {}
        for record in records:
            key = record["kind"] + (":" + record["event"] if "event" in record else "")
            fields = kinds.setdefault(key, {})
            for path, kind in shape(record).items():
                current = set(fields.get(path, []))
                current.add(kind)
                fields[path] = sorted(current)
        families[family] = {"version": version, "records": kinds}
    return {"families": families}


def check_input_versions(binary):
    """Check the documented boundary between the legacy and metadata walks."""
    cases = [
        (
            ["table", "info"],
            {"kind": "pqbench.table-ref", "version": 2, "id": "c.s.t", "uri": "file:///unused"},
            "version 1",
        ),
        (
            ["tablev2", "info"],
            {"kind": "pqbench.table-ref", "version": 1, "id": "c.s.t", "uri": "file:///unused"},
            "version 2",
        ),
        (
            ["metastore", "info"],
            {"kind": "pqbench.lake-source", "version": 2, "endpoint": "http://127.0.0.1:1"},
            "version 1",
        ),
    ]
    for args, record, expected in cases:
        result = subprocess.run(
            [str(binary), *args], cwd=ROOT, input=json.dumps(record) + "\n",
            capture_output=True, text=True, check=False,
            env={**os.environ, "PQB_ENDPOINT": "http://127.0.0.1:1"},
        )
        if result.returncode == 0 or expected not in result.stderr:
            raise RuntimeError(f"{' '.join(args)} accepted an incompatible input version: {result.stderr.strip()}")


def breaking_changes(old, new):
    changes = []
    for family, before in old["families"].items():
        after = new["families"].get(family)
        if after is None:
            changes.append(f"{family}: family removed")
            continue
        if after["version"] < before["version"]:
            changes.append(f"{family}: version decreased")
        breaking = []
        for record, fields in before["records"].items():
            current = after["records"].get(record)
            if current is None:
                breaking.append(f"record {record} removed")
                continue
            for path, types in fields.items():
                if path not in current:
                    breaking.append(f"{record}.{path} removed")
                elif types != current[path]:
                    breaking.append(f"{record}.{path} changed type")
        for record in after["records"]:
            if record not in before["records"]:
                breaking.append(f"record {record} added")
        if breaking and after["version"] <= before["version"]:
            changes.append(f"{family}: breaking shape at version {after['version']}: " + ", ".join(breaking))
    return changes


def baseline(base):
    result = subprocess.run(
        ["git", "show", f"{base}:contracts/cli-output-shapes.json"],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    if result.returncode:
        if "exists on disk, but not in" in result.stderr or "does not exist" in result.stderr:
            return None  # First PR introducing the snapshot.
        raise RuntimeError(f"cannot read contract baseline {base}: {result.stderr.strip()}")
    return json.loads(result.stdout)


def fingerprint(snapshot):
    canonical = json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def has_change_log_entry(document, digest):
    section = document.partition("## Contract changes")[2]
    return any(line.startswith("|") and digest in line for line in section.splitlines())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--base", default=os.environ.get("CONTRACT_BASE", ""))
    parser.add_argument("--update", action="store_true")
    args = parser.parse_args()

    actual = observe(args.binary.resolve())
    check_input_versions(args.binary.resolve())
    if args.update:
        SNAPSHOT.parent.mkdir(exist_ok=True)
        SNAPSHOT.write_text(json.dumps(actual, indent=2, sort_keys=True) + "\n")
        print(f"wrote {SNAPSHOT.relative_to(ROOT)}; add fingerprint {fingerprint(actual)} to the change log")
        return 0

    expected = json.loads(SNAPSHOT.read_text())
    if actual != expected:
        print("CLI output shape differs from contracts/cli-output-shapes.json; inspect the change and run make update-contracts", file=sys.stderr)
        return 1

    digest = fingerprint(expected)
    if not has_change_log_entry(REFERENCE.read_text(), digest):
        print(f"contract fingerprint {digest} is missing from docs/io-contracts.md change log", file=sys.stderr)
        return 1

    if args.base:
        previous = baseline(args.base)
        if previous is not None:
            changes = breaking_changes(previous, expected)
            if changes:
                print("breaking CLI output changes need a new family version:\n" + "\n".join(changes), file=sys.stderr)
                return 1
            if previous != expected:
                old_digest = fingerprint(previous)
                base_doc = subprocess.run(
                    ["git", "show", f"{args.base}:docs/io-contracts.md"],
                    cwd=ROOT, capture_output=True, text=True, check=True,
                ).stdout
                if digest == old_digest or has_change_log_entry(base_doc, digest):
                    print("changed contract snapshot needs a new change-log entry", file=sys.stderr)
                    return 1
    print("CLI contracts: ok")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError) as error:
        print(f"contract check: {error}", file=sys.stderr)
        sys.exit(1)
