"""Small catalog producers for pqbench's stdin protocol; stdout is JSON only."""
import argparse
import io
import json
import sys
import time
import urllib.error
import urllib.request

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from pyiceberg.catalog import load_catalog

S3 = "http://rustfs:9000"
UC = "http://unity-catalog:8080/api/2.1/unity-catalog"


def api(path, payload=None):
    request = urllib.request.Request(
        UC + path,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def ensure(path, payload):
    try:
        api(path, payload)
    except urllib.error.HTTPError as error:
        body = error.read().decode()
        if json.loads(body).get("error_code") not in {
            "CATALOG_ALREADY_EXISTS", "SCHEMA_ALREADY_EXISTS", "TABLE_ALREADY_EXISTS"
        }:
            print(body, file=sys.stderr)
            raise


def iceberg():
    return load_catalog("local", **{
        "type": "rest", "uri": "http://iceberg-rest:8181",
        "s3.endpoint": S3, "s3.access-key-id": "test",
        "s3.secret-access-key": "test", "s3.region": "us-east-1",
    })


def lake():
    c = duckdb.connect()
    for extension in ("httpfs", "sqlite", "ducklake"):
        c.execute("LOAD " + extension)
    c.execute("""CREATE SECRET (TYPE s3, KEY_ID 'test', SECRET 'test',
        REGION 'us-east-1', ENDPOINT 'rustfs:9000', URL_STYLE 'path', USE_SSL false)""")
    c.execute("""ATTACH 'ducklake:sqlite:/state/examples.sqlite' AS lake
        (DATA_PATH 's3://lakehouse/ducklake/', DATA_INLINING_ROW_LIMIT 0)""")
    return c


def s3():
    return boto3.client("s3", endpoint_url=S3)


def ensure_bucket():
    client = s3()
    try:
        client.head_bucket(Bucket="lakehouse")
    except ClientError:
        client.create_bucket(Bucket="lakehouse")


def seed():
    # Bound startup waits; do not mistake a started JVM for an available API.
    for attempt in range(60):
        try:
            ensure_bucket()
            api("/catalogs")
            iceberg().list_namespaces()
            break
        except Exception as error:
            if attempt == 59:
                raise RuntimeError("Catalog services did not become ready") from error
            time.sleep(2)
    data = pa.table({"id": pa.array([1, 2, 3], type=pa.int64()),
                     "label": ["local", "remote", "lake"]})
    buffer = io.BytesIO()
    pq.write_table(data, buffer)
    s3().put_object(Bucket="lakehouse", Key="unity/events/part-0.parquet", Body=buffer.getvalue())
    ensure("/catalogs", {"name": "pqbench"})
    ensure("/schemas", {"catalog_name": "pqbench", "name": "demo"})
    ensure("/tables", {
        "catalog_name": "pqbench", "schema_name": "demo", "name": "events",
        "table_type": "EXTERNAL", "data_source_format": "PARQUET",
        "storage_location": "s3://lakehouse/unity/events/",
        "columns": [{"name": name, "type_name": kind, "type_text": kind.lower(),
                     "type_json": json.dumps({"name": name, "type": kind.lower(), "nullable": True, "metadata": {}}),
                     "position": position, "nullable": True}
                    for position, (name, kind) in enumerate((("id", "LONG"), ("label", "STRING")))],
    })
    catalog = iceberg()
    catalog.create_namespace_if_not_exists("demo")
    table = catalog.create_table_if_not_exists("demo.events", schema=data.schema)
    table.overwrite(data)
    assert table.scan().to_arrow().to_pydict() == data.to_pydict()
    with lake() as c:
        c.execute("CREATE OR REPLACE TABLE lake.events AS SELECT * FROM data")
        assert c.execute("SELECT count(*), sum(id) FROM lake.events").fetchone() == (3, 6)
    print("Seeded Unity Parquet, Iceberg and DuckLake: 3 rows each", file=sys.stderr)


def source(engine):
    if engine == "unity":
        table = api("/tables/pqbench.demo.events")
        assert table["data_source_format"] == "PARQUET"
        bucket, prefix = table["storage_location"][5:].split("/", 1)
        inputs = [f"s3://{bucket}/{obj['Key']}"
                  for page in s3().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
                  for obj in page.get("Contents", []) if obj["Key"].endswith(".parquet")]
    elif engine == "iceberg":
        tasks = list(iceberg().load_table("demo.events").scan().plan_files())
        if any(task.delete_files for task in tasks):
            raise ValueError("Example supports tables without delete files only")
        inputs = [task.file.file_path for task in tasks]
    else:
        with lake() as c:
            rows = c.execute("SELECT data_file, delete_file FROM ducklake_list_files('lake', 'events')").fetchall()
        if any(row[1] for row in rows):
            raise ValueError("Example supports tables without delete files only")
        inputs = [row[0] for row in rows]
    if not inputs:
        raise ValueError("No active Parquet files; run seed first")
    print(json.dumps({"kind": "pqbench.remote-source", "version": 1,
                      "inputs": sorted(set(inputs))}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["seed", "unity", "iceberg", "ducklake"])
    args = parser.parse_args()
    seed() if args.command == "seed" else source(args.command)
