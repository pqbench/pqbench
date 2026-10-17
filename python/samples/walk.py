"""Walk a catalog to its files with the pqbench bindings, as one generator.

Run against a live endpoint::

    PQB_ENDPOINT=https://<workspace>.cloud.databricks.com PQB_TOKEN=<bearer> \\
    AWS_REGION=us-east-2 python walk.py pqbench_ext.nyctaxi

The generator mirrors the CLI pipeline and yields each resource as a dict; a
``for`` loop is the whole driver, on one thread. Reads ``PQB_ENDPOINT`` and
``PQB_TOKEN`` (the CLI's contract); the storage region comes from
``AWS_REGION``.
"""

from __future__ import annotations

import json
import os
import sys

import pqbench


def aws_env(record: dict) -> dict:
    """A table-ref's vended lease as the ``AWS_*`` options a read needs."""
    credentials = record.get("credentials") or {}
    return {
        key: value
        for key, value in {
            "AWS_ACCESS_KEY_ID": credentials.get("access_key_id"),
            "AWS_SECRET_ACCESS_KEY": credentials.get("secret_access_key"),
            "AWS_SESSION_TOKEN": credentials.get("session_token"),
        }.items()
        if value
    }


def walk(endpoint: str, token: str, schema_fqn: str):
    """Yield ``(level, record)`` for a schema's tables, partitions, and files."""
    catalog, schema = schema_fqn.split(".", 1)
    yield "metastore", pqbench.get_metastore(endpoint=endpoint, token=token)
    yield "catalog", pqbench.get_catalog(catalog, endpoint=endpoint, token=token)
    yield "schema", pqbench.get_schema(catalog, schema, endpoint=endpoint, token=token)
    listed = pqbench.list_tables(catalog, schema, endpoint=endpoint, token=token)
    readable = pqbench.credentials_check(listed, endpoint=endpoint, token=token)
    vended = pqbench.credentials_get(readable, endpoint=endpoint, token=token)
    for ref in vended:
        name = ref["name"].rsplit("/", 1)[-1]
        env = aws_env(ref)
        table = pqbench.get_table(
            catalog, schema, name, env=env, endpoint=endpoint, token=token
        )
        yield "table", table
        if not table.get("uri"):
            continue
        partitions = pqbench.list_partitions(
            table["uri"], table=table["name"], env=env, endpoint=endpoint, token=token
        )
        for partition in partitions:
            yield "partition", partition
            for file in pqbench.list_files(partition, endpoint=endpoint, token=token):
                yield "file", file


def main() -> None:
    endpoint = os.environ["PQB_ENDPOINT"]
    token = os.environ["PQB_TOKEN"]
    schema = sys.argv[1] if len(sys.argv) > 1 else "pqbench_ext.tpch_sf1"
    for level, record in walk(endpoint, token, schema):
        print(f"{level:9} {record['name']}")


if __name__ == "__main__":
    main()
