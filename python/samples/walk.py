"""Walk the whole metastore with the pqbench bindings.

Run against a live endpoint::

    PQB_ENDPOINT=https://<workspace>.cloud.databricks.com PQB_TOKEN=<bearer> \
    AWS_REGION=us-east-2 python walk.py

``pqbench.walk`` spans every catalog and descends every table the principal may
read outside compute — multiplexed, so the tables are in flight together — and
yields each table file::

    async for file in pqbench.walk(workspace):
        print(file["uri"])
"""

from __future__ import annotations

import asyncio
import os

import pqbench


async def main() -> None:
    os.environ.setdefault("AWS_REGION", "us-east-2")
    workspace = pqbench.Workspace(
        pqbench.Config(os.environ["PQB_ENDPOINT"], os.environ["PQB_TOKEN"])
    )
    count = 0
    async for file in pqbench.walk(workspace):
        count += 1
        print(file["uri"])
    print(f"--- {count} files")


if __name__ == "__main__":
    asyncio.run(main())
