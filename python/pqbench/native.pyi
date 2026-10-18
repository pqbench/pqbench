"""Types for the ``pqbench.native`` extension module.

Every name here is the Rust function of the same path, with the same
arguments, so the stub is a mirror of the module rather than a second API:
``native.catalog.list.list(workspace)`` is ``pqbench::catalog::list::list``.
Three things the module cannot say about itself live here.

**The awaitables.** The commands whose entry point is async return pyo3's
coroutine object, whose Python type is erased — ``future_into_py`` hands back
"some awaitable", because the runtime, not the signature, owns the completion.
The stub is where the erased type is written down, the way the sibling bindings
do it (lancedb declares ``async def`` for its native module in
``_lancedb.pyi``; delta-rs ships ``_internal.pyi``). So an awaitable command is
an ``async def`` returning its model, and ``await native.table.get.get(…)`` is
a :class:`Table`.

**The records.** A record's fields are its model's, built from the same serde
shape the JSON commands emit, so they are declared here field by field with the
model's own names and types. A deep tree the model embeds (a file's ``stats``,
a table's ``declared_columns``) stays a ``dict``, as the library keeps it.

**The submodules.** A stub cannot declare a submodule of a module (only of a
package), so ``native.catalog`` and its verbs are declared as nested classes
with static methods. The access path and the signature are exact; only the
runtime type differs (a module object, not a class).

The surface is the walk's own pipeline and the measurement that consumes it —
metastore, catalog, schema, table, credentials, partition, file, bytemass — and
stops at :class:`bytemass`: the commands downstream of a byte-mass stream
(``viz``, ``diff``, ``profile``, ``experiment``), the standalone codec
benchmarks (``lz``, ``compression``) and ``skill`` are separate tools, and are
not bound. Only :func:`walk` composes; every other name is a binding.
"""

from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Any, ClassVar

__version__: str

class Dialect:
    """The catalog dialect: Unity Catalog or Iceberg REST."""

    UNITY: ClassVar[Dialect]
    ICEBERG: ClassVar[Dialect]

class Config:
    """The endpoint, token, and dialect a :class:`Workspace` runs under."""

    def __init__(
        self,
        endpoint: str,
        token: str | None = None,
        dialect: Dialect = ...,
    ) -> None: ...
    @property
    def dialect(self) -> Dialect: ...
    @property
    def endpoint(self) -> str: ...
    @property
    def token(self) -> str | None: ...
    def __repr__(self) -> str: ...

class Layer:
    """One configuration layer; :meth:`resolve` combines the set into a `Config`."""

    def __init__(
        self,
        endpoint: str | None = None,
        token: str | None = None,
        dialect: Dialect | None = None,
    ) -> None: ...
    def resolve(self) -> Config: ...
    def __repr__(self) -> str: ...

class Workspace:
    """A catalog client: one HTTP pool for the endpoint."""

    def __init__(self, config: Config) -> None: ...
    @property
    def config(self) -> Config: ...
    def __repr__(self) -> str: ...

class Lake:
    """An object-store client for a :class:`Workspace`."""

    def __init__(self, workspace: Workspace) -> None: ...
    def __repr__(self) -> str: ...


class BytemassRequest:
    """Arguments for ``pqbench.native.bytemass.bytemass``."""

    def __init__(
        self,
        inputs: Sequence[str],
        env: dict[str, str] | None = None,
        indexes: bool = False,
    ) -> None: ...

class RateLimit:
    """The library's rate limiter: per-kind minimum spacing between calls."""

    def __init__(self, requests_per_second: float) -> None: ...
    def delay(self, kind: str) -> float:
        """Seconds to wait before the next call of `kind`; 0.0 when a slot is free."""
        ...

class ratelimit:
    """``pqbench.native.ratelimit``: the library's rate limiter.

    The class is reachable here and at the top level: the module exposes it
    both ways.
    """

    RateLimit: ClassVar[type[RateLimit]]

class ValueIterator:
    """A lazily-consumed stream of built values, driven on the shared runtime."""

    def __aiter__(self) -> ValueIterator: ...
    async def __anext__(self) -> Any: ...

class Record:
    """One record of a library model, its fields, and the mapping interface.

    A field reads the same three ways — ``record["uri"]``, ``record.uri`` and
    ``record.asdict()["uri"]`` — and a record is mutable in one place: a
    composed stage puts the lease that read it on the record. Records compare
    by value and are unhashable, like the dicts they mirror.
    """

    def __contains__(self, key: str) -> bool: ...
    def __eq__(self, value: object) -> bool: ...
    def __getitem__(self, key: str) -> Any:
        """The field, or :exc:`KeyError` when the model has none."""
        ...
    def __setitem__(self, key: str, value: Any) -> None: ...
    def __iter__(self) -> Iterator[str]:
        """The field names, in the model's order."""
        ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def asdict(self) -> dict[str, Any]:
        """The record as a dict, the shape a JSON command emits."""
        ...
    def get(self, key: str, default: Any = None) -> Any:
        """The field, or `default` when the model has none."""
        ...
    def items(self) -> list[tuple[str, Any]]: ...
    def keys(self) -> list[str]: ...
    def values(self) -> list[Any]: ...

class Catalog(Record):
    """One catalog: ``catalogs/{catalog}``."""

    name: str
    uid: str | None
    display_name: str | None
    catalog_type: str | None  # "CATALOG_TYPE_MANAGED" | "CATALOG_TYPE_FOREIGN" | "CATALOG_TYPE_SYSTEM"
    comment: str | None
    owner: str | None

class Schema(Record):
    """One schema: ``catalogs/{catalog}/schemas/{schema}``."""

    name: str
    uid: str | None
    comment: str | None
    uri: str | None
    properties: dict[str, str]

class Table(Record):
    """One table: ``table.get.get``'s record, ``table.list.list``'s element.

    A listing costs what the catalog's page carries — Unity's pages name a
    storage location, Iceberg REST lists identifiers only — so ``uri`` is unset
    there until ``table.get.get`` resolves it.
    """

    name: str
    uid: str | None
    uri: str | None
    format: str | None  # "TABLE_FORMAT_DELTA" | "TABLE_FORMAT_ICEBERG"
    snapshot_version: int | None
    partition_columns: list[str]
    declared_columns: list[dict[str, Any]]
    properties: dict[str, str]
    file_count: int | None
    partition_count: int | None
    credentials: Credentials | None

class Partition(Record):
    """A natural partition: a half-open commit-time window."""

    name: str
    start_time: int | None
    end_time: int | None
    commit_versions: list[int]
    commit_info: list[dict[str, Any]]
    uri: str | None
    credentials: dict[str, Any] | None

class TableFile(Record):
    """One data file the current snapshot treats as active."""

    path: str
    uri: str
    size_bytes: int
    partition_values: dict[str, str | None]
    stats: dict[str, Any] | None

class MassRow(Record):
    """One column chunk's measured byte mass: a row of the ``bytemass`` table."""

    uri: str
    size_bytes: int
    row_count: int
    column: str
    compressed_bytes: int
    uncompressed_bytes: int
    codec: str
    storage_class: str | None
    encodings: list[str]
    num_values: int
    dictionary: bool
    null_count: int | None
    distinct_count: int | None
    min_value: str | None
    max_value: str | None
    physical_type: str
    row_group: int
    row_group_rows: int
    compressed_bytes_per_row: float | None
    uncompressed_bytes_per_row: float | None
    compression_ratio: float | None
    null_fraction: float | None
    page_count: int | None
    page_compressed_bytes: int | None

class Credentials(Record):
    """A vended storage lease: the ``AWS_*`` options a read takes.

    ``table.get.get``, ``partition.list.list`` and ``file.list.list`` take one
    back as their ``credentials`` — the record ``credentials.get.get`` returns.
    """

    access_key_id: str | None
    secret_access_key: str | None
    session_token: str | None
    expire_time: str | None

class Eligibility:
    """Whether a principal may be vended credentials for one table."""

    eligible: bool
    reason: str | None
    def __repr__(self) -> str: ...

class metastore:
    """``pqbench.native.metastore``: the whole metastore as one tree."""

    class get:
        """``pqbench.native.metastore.get``: read verbs."""

        @staticmethod
        async def get(workspace: Workspace) -> dict[str, Any]:
            """The metastore tree, as the crate's record.

            The tree is a document rather than a resource, so it stays a dict;
            ``children`` holds the nested levels.
            """
            ...

class catalog:
    """``pqbench.native.catalog``: one catalog, or the catalogs."""

    class get:
        """``pqbench.native.catalog.get``: read one catalog."""

        @staticmethod
        async def get(workspace: Workspace, name: str) -> Catalog: ...

    class list:
        """``pqbench.native.catalog.list``: list the catalogs."""

        @staticmethod
        async def list(workspace: Workspace) -> list[Catalog]: ...

class schema:
    """``pqbench.native.schema``: one schema, or a catalog's schemas."""

    class get:
        """``pqbench.native.schema.get``: read one schema."""

        @staticmethod
        async def get(workspace: Workspace, name: str) -> Schema: ...

    class list:
        """``pqbench.native.schema.list``: list a catalog's schemas."""

        @staticmethod
        async def list(workspace: Workspace, name: str) -> list[Schema]: ...

class table:
    """``pqbench.native.table``: one table, or a schema's refs."""

    class get:
        """``pqbench.native.table.get``: read one table."""

        @staticmethod
        async def get(
            workspace: Workspace,
            lake: Lake,
            name: str,
            uri: str | None,
            credentials: Credentials | dict[str, Any] | None,
        ) -> Table: ...

    class list:
        """``pqbench.native.table.list``: list a schema's table refs."""

        @staticmethod
        async def list(workspace: Workspace, name: str) -> list[Table]: ...

class partition:
    """``pqbench.native.partition``: a table's natural partitions."""

    class list:
        """``pqbench.native.partition.list``: list a table's partitions."""

        @staticmethod
        def list(
            lake: Lake,
            table: str,
            uri: str,
            credentials: Credentials | dict[str, Any] | None,
            window_millis: int,
        ) -> AsyncIterator[Partition]:
            """Stream the table's partitions, widest window first."""
            ...

class file:
    """``pqbench.native.file``: the files of one partition."""

    class list:
        """``pqbench.native.file.list``: list a partition's files."""

        @staticmethod
        def list(
            lake: Lake,
            uri: str,
            credentials: Credentials | dict[str, Any] | None,
            versions: Sequence[int],
        ) -> AsyncIterator[TableFile]:
            """Stream the files the given log versions added."""
            ...

class credentials:
    """``pqbench.native.credentials``: check eligibility, then vend a lease."""

    class check:
        """``pqbench.native.credentials.check``: may this principal read the table?"""

        @staticmethod
        async def check(workspace: Workspace, name: str) -> Eligibility: ...

    class get:
        """``pqbench.native.credentials.get``: the lease of one table."""

        @staticmethod
        async def get(workspace: Workspace, name: str) -> Credentials | None:
            """The table's vended read credentials, by its resource name.

            ``None`` when the catalog vends nothing for the table, so the read
            falls back to the process environment.
            """
            ...

class bytemass:
    """``pqbench.native.bytemass``: the per-column byte masses of Parquet files."""

    @staticmethod
    async def bytemass(lake: Lake, request: BytemassRequest) -> list[MassRow]:
        """Measure the files the request names, one row per column chunk."""
        ...
