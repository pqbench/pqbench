"""``walk``: the metastore walk, one helper over the verbs.

The verbs compose by hand — the pipeline the CLI pipes together,
``catalog.ls.list | schema.ls.list | table.ls.list | credentials.check.check |
credentials.get.vend | table.get.read | partition.ls.list |
file.ls.list`` — but a hand-written ``for`` loop awaits one table at a time,
and on a real endpoint that is the difference between minutes and seconds.
This helper is that composition with the independence handed to ``asyncio``:
the catalog levels fan out, one task descends each table under a bounded
semaphore, and the files arrive as one async iterator.

    async for file in pqbench.walk(workspace):
        print(file["uri"])

The Unity Catalog API limits requests per endpoint and per workspace, so its
calls are paced — one bucket per endpoint, through the library's own
``ratelimit.RateLimit`` — while the storage reads are not: object storage has
no such limit, and its client retries on its own. Everything else in this
package stays the library function of the same path; this is the one name that
is a composition rather than a binding.
"""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import AsyncIterator, Awaitable
from typing import Any, TypeVar

from pqbench import native

_log = logging.getLogger("pqbench")

CONCURRENCY = 32
"""Tables descended at once by default: the endpoint saturates near 64."""

REQUESTS_PER_SECOND = 15.0
"""Unity Catalog requests per second per endpoint: the library's own default."""

WINDOW_MILLIS = 86_400_000
"""The natural-window width the CLI's ``--every 1d`` names: one UTC day."""

_LevelRecord = TypeVar("_LevelRecord", bound=native.Record)
"""A record a level listing returns: `Catalog`, `Schema` or `TableRef`."""

_DONE = object()


def _leaf(record: native.Record) -> str:
    """The id segment of a resource name (its last path component)."""
    return record["name"].rsplit("/", 1)[-1]


def _lease(credentials: dict[str, Any] | None) -> dict[str, str]:
    """A vended lease as the ``AWS_*`` options a storage read needs."""
    lease = credentials or {}
    options = {
        "AWS_ACCESS_KEY_ID": lease.get("access_key_id"),
        "AWS_SECRET_ACCESS_KEY": lease.get("secret_access_key"),
        "AWS_SESSION_TOKEN": lease.get("session_token"),
    }
    return {name: value for name, value in options.items() if value}


async def walk(
    workspace: native.Workspace,
    lake: native.Lake | None = None,
    catalog: str | None = None,
    concurrency: int = CONCURRENCY,
    requests_per_second: float = REQUESTS_PER_SECOND,
    window_millis: int = WINDOW_MILLIS,
) -> AsyncIterator[native.TableFile]:
    """Yield a table-file record for every file the metastore reaches.

    Each record is a :class:`TableFile` — the library's model, so
    ``file.uri``, ``file["uri"]`` and ``file.asdict()`` agree.

    The catalog levels span the whole Unity Catalog; the table levels descend
    every catalog whose tables this principal may read outside compute —
    ``catalog`` names one to descend instead.

    ``concurrency`` bounds the tables in flight, and with them the requests in
    flight; each table's chain (``credentials.check.check`` →
    ``credentials.get.vend`` → ``table.get.read`` → ``partition.ls.list``
    → ``file.ls.list``) is sequential, so this is the only parallelism there
    is. ``lake`` defaults to one over ``workspace``.

    ``requests_per_second`` paces the Unity Catalog calls — the level listings
    and the per-table ``credentials`` calls — one bucket per endpoint, since
    that is the scope the endpoint limits: 15 by default, the library's own,
    and ``0`` turns pacing off. The storage reads are not paced: object storage
    has no such limit and retries its own.

    A call the principal may not make is skipped — a table it cannot read, a
    catalog whose schema listing it is refused, an endpoint that answers
    ``429 Too Many Requests`` — and the walk logs how many were skipped when it
    finishes, so a rate limit shows up as a count rather than as files that are
    silently missing. Each skip is logged at ``debug``.

    Records stream as they are found, so the walk holds no more than a bounded
    queue of them however large the metastore.
    """
    if concurrency < 1:
        raise ValueError("concurrency must be at least 1")
    if window_millis <= 0:
        raise ValueError("window_millis must be positive")
    if not math.isfinite(requests_per_second) or requests_per_second < 0.0:
        raise ValueError("requests_per_second must be a finite, non-negative rate")
    if lake is None:
        lake = native.Lake(workspace)

    queue: asyncio.Queue[native.TableFile] = asyncio.Queue(maxsize=concurrency)
    inflight = asyncio.Semaphore(concurrency)
    limit = native.ratelimit.RateLimit(requests_per_second)
    tasks: list[asyncio.Task[None]] = []
    skipped = 0

    async def pace(endpoint: str) -> None:
        """Wait for this endpoint's next slot in its own bucket."""
        delay = limit.delay(endpoint)
        if delay > 0.0:
            await asyncio.sleep(delay)

    async def skip_level(
        awaitable: Awaitable[list[_LevelRecord]], default: list[_LevelRecord]
    ) -> list[_LevelRecord]:
        """Await a listing, counting a level this principal cannot read."""
        nonlocal skipped
        try:
            return await awaitable
        except Exception as error:  # noqa: BLE001 - a level we may not list
            skipped += 1
            _log.debug("skipped an unreadable level: %s", error)
            return default

    async def descend(ref: native.TableRef, catalog: str, schema: str) -> None:
        """One table's chain, holding one of the `concurrency` slots."""
        nonlocal skipped
        table = _leaf(ref)
        async with inflight:
            try:
                await pace("credentials.check")
                eligibility = await native.credentials.check.check(
                    workspace, catalog, schema, table
                )
                if not eligibility.eligible:
                    return
                await pace("credentials.get")
                ref = await native.credentials.get.vend(workspace, ref)
                env = _lease(ref.get("credentials"))
                info = await native.table.get.read(
                    workspace, lake, catalog, schema, table, native.Storage(env=env)
                )
                uri = info.get("uri")
                if not uri:
                    return
                async for partition in native.partition.ls.list(
                    lake, info["name"], uri, env, window_millis
                ):
                    files_env = {**env, **_lease(partition.get("credentials"))}
                    async for file in native.file.ls.list(
                        lake, partition["uri"], files_env, partition["commit_versions"]
                    ):
                        await queue.put(file)
            except Exception as error:  # noqa: BLE001 - a table we may not read
                skipped += 1
                _log.debug("skipped %s.%s.%s: %s", name, schema, table, error)

    async def descend_all() -> None:
        """The catalog levels, then one task per table; one queue for all."""
        try:
            await pace("catalog.ls")
            for listed in await native.catalog.ls.list(workspace):
                name = _leaf(listed)
                if catalog is not None and name != catalog:
                    continue
                await pace("schema.ls")
                schemas = await skip_level(
                    native.schema.ls.list(workspace, name), []
                )
                for schema in schemas:
                    schema_name = _leaf(schema)
                    await pace("table.ls")
                    refs = await skip_level(
                        native.table.ls.list(workspace, name, schema_name), []
                    )
                    for ref in refs:
                        tasks.append(
                            asyncio.create_task(descend(ref, name, schema_name))
                        )
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await queue.put(_DONE)

    producer = asyncio.create_task(descend_all())
    try:
        while True:
            record = await queue.get()
            if record is _DONE:
                if skipped:
                    _log.warning(
                        "walk skipped %d unreadable call(s); their files are not "
                        "in this walk (log at DEBUG for each)",
                        skipped,
                    )
                return
            yield record
    finally:
        producer.cancel()
        for task in tasks:
            task.cancel()
        await asyncio.gather(producer, *tasks, return_exceptions=True)
