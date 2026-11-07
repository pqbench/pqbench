#!/usr/bin/env python3
"""Walk the live metastore with a streaming shell pipeline, from Python.

    python3 scripts/walk_metastore.py --pqbench target/debug/pqbench

The walk is the issue-#58 shape, composed with plumbum's pipe operator:

    list catalogs | list schemas | list tables

Each level is one process, and the CLI fans the next level out as refs arrive;
nothing is walked one schema at a time. The same pattern enriches the stream
with `get catalog` and `get schema`. The script asserts the live workspace's
catalog set and that every level names a well-formed resource.

The environment carries the walk's context (`PQB_ENDPOINT` / `PQB_TOKEN`); the
`dbx-e2e` target runs this against the repository's live fixture.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from typing import Iterable

from plumbum import ProcessExecutionError, local

# The workspace the repository's live e2e targets. `list catalogs` sorts by
# name, so this is the order the listing emits.
EXPECTED_CATALOGS = ["dbx_samples", "pqbench_ext", "samples", "system", "workspace"]


def records(pipeline) -> list[dict]:
    """Run one pipeline and parse its NDJSON output."""
    text = pipeline()
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def kind(record: dict) -> str:
    """The record's resource type: the collection before its id."""
    parts = record.get("name", "").split("/")
    return parts[-2] if len(parts) >= 2 else ""


def resource_id(record: dict) -> str:
    """The record's id: the last segment of its resource name."""
    return record.get("name", "").rsplit("/", 1)[-1]


def catalog_of(record: dict) -> str:
    return record["name"].split("/")[1]


def schema_of(record: dict) -> str:
    return record["name"].split("/")[3]


def fail(message: str) -> None:
    print(f"walk: {message}", file=sys.stderr)
    raise SystemExit(1)


def require_kind(records_: Iterable[dict], expected: str, level: str) -> list[dict]:
    rows = list(records_)
    for record in rows:
        if kind(record) != expected:
            fail(f"{level} emitted a {kind(record) or 'nameless'} record: {record}")
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--pqbench",
        default=os.environ.get("PQB", "pqbench"),
        help="pqbench binary (default: $PQB, then pqbench)",
    )
    parser.add_argument(
        "--fan-out",
        type=int,
        default=64,
        help="catalogs and schemas in flight per listing (default: 64)",
    )
    args = parser.parse_args()

    pqb = local[args.pqbench]
    list_catalogs = pqb["list", "catalogs"]
    list_schemas = pqb["list", "schemas", "--fan-out", str(args.fan_out)]
    list_tables = pqb["list", "tables", "--fan-out", str(args.fan_out)]

    try:
        metastore = require_kind(records(pqb["get", "metastore"]), "metastores", "get metastore")
        catalogs = require_kind(records(list_catalogs), "catalogs", "list catalogs")
        catalog_info = require_kind(
            records(list_catalogs | pqb["get", "catalog"]), "catalogs", "get catalog"
        )
        schemas = require_kind(
            records(list_catalogs | list_schemas), "schemas", "list schemas"
        )
        schema_info = require_kind(
            records(list_catalogs | list_schemas | pqb["get", "schema"]),
            "schemas",
            "get schema",
        )
        tables = require_kind(
            records(list_catalogs | list_schemas | list_tables), "tables", "list tables"
        )
    except ProcessExecutionError as error:
        fail(str(error))

    if len(metastore) != 1:
        fail(f"get metastore emitted {len(metastore)} records")
    print(f"metastore: {metastore[0]['name']}")

    names = [resource_id(record) for record in catalogs]
    if names != EXPECTED_CATALOGS:
        fail(f"list catalogs emitted {names}, expected {EXPECTED_CATALOGS}")
    if sorted(resource_id(record) for record in catalog_info) != sorted(names):
        fail("get catalog did not enrich every catalog")

    schemas_by_catalog: dict[str, list[dict]] = defaultdict(list)
    for record in schemas:
        catalog = catalog_of(record)
        if not resource_id(record):
            fail(f"a schema in {catalog} has no id: {record}")
        schemas_by_catalog[catalog].append(record)
    for catalog in names:
        if not schemas_by_catalog[catalog]:
            fail(f"catalog {catalog} listed no schemas")
        print(f"catalog {catalog}: {len(schemas_by_catalog[catalog])} schema(s)")

    if sorted(record["name"] for record in schema_info) != sorted(
        record["name"] for record in schemas
    ):
        fail("get schema did not enrich every schema")

    tables_by_schema: dict[str, list[dict]] = defaultdict(list)
    for record in tables:
        fqn = f"{catalog_of(record)}.{schema_of(record)}"
        if not resource_id(record):
            fail(f"a table in {fqn} has no id: {record}")
        tables_by_schema[fqn].append(record)
    for record in schemas:
        fqn = f"{catalog_of(record)}.{resource_id(record)}"
        print(f"schema {fqn}: {len(tables_by_schema[fqn])} table(s)")

    print(
        f"walk: {len(catalogs)} catalogs, {len(schemas)} schemas, "
        f"{len(tables)} tables"
    )


if __name__ == "__main__":
    main()
