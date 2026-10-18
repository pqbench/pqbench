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

The analysis commands are synchronous: they block the calling thread with the
GIL released (``py.detach``) and return plain dicts, their report being a
document rather than a resource. Only :func:`walk` composes; every other name
is a binding.
"""

from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Any, ClassVar

__version__: str

class Dialect:
    """The catalog dialect: Unity Catalog or Iceberg REST."""

    UNITY: ClassVar[Dialect]
    ICEBERG: ClassVar[Dialect]

class Mode:
    """How a sweep's samples reduce: the fastest pass, or their mean."""

    FASTEST: ClassVar[Mode]
    MEAN: ClassVar[Mode]

class Aim:
    """What an experiment measures after a rewrite."""

    STORAGE: ClassVar[Aim]
    SKIPPING: ClassVar[Aim]
    ALL: ClassVar[Aim]

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

class LzRequest:
    """Arguments for ``pqbench.native.lz.lz``."""

    def __init__(
        self,
        file: str,
        codec_specs: Sequence[str] | None = None,
        samples: int = 10,
        warmup_iterations: int = 3,
        mode: Mode = ...,
    ) -> None: ...

class CompressionRequest:
    """Arguments for ``pqbench.native.compression.compression``."""

    def __init__(
        self,
        file: str,
        codec_specs: Sequence[str] | None = None,
        samples: int = 10,
        warmup_iterations: int = 3,
        mode: Mode = ...,
        per_column: bool = False,
    ) -> None: ...

class BytemassRequest:
    """Arguments for ``pqbench.native.bytemass.bytemass``."""

    def __init__(
        self,
        inputs: Sequence[str],
        env: dict[str, str] | None = None,
        indexes: bool = False,
    ) -> None: ...

class ProfileRequest:
    """Arguments for ``pqbench.native.profile.profile``."""

    def __init__(
        self,
        columns: Sequence[str] | None = None,
        top: int = 5,
    ) -> None: ...

class ExperimentRequest:
    """Arguments for ``pqbench.native.experiment.experiment``."""

    def __init__(
        self,
        trials: Sequence[str] | None = None,
        aim: Aim = ...,
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
    """One table's record, as ``table.get.get`` resolves it."""

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
    credentials: dict[str, Any] | None

class TableRef(Record):
    """One table's reference, as ``table.list.list`` lists it.

    The same model as :class:`Table`, carrying what a listing costs: Unity
    pages carry a storage location, Iceberg REST lists identifiers only, so
    ``uri`` is unset there until ``table.get.get`` resolves it.
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
    credentials: dict[str, Any] | None

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

class Eligibility:
    """Whether a principal may be vended credentials for one table."""

    eligible: bool
    reason: str | None
    def __repr__(self) -> str: ...

class Sample:
    """A read sample: the columns, and how many rows were read."""

    columns: list[str]
    row_count: int

class TypedSample:
    """A typed sample: the columns with their types, and the row count."""

    columns: list[str]
    row_count: int

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
            table: TableRef,
        ) -> Table: ...

    class list:
        """``pqbench.native.table.list``: list a schema's table refs."""

        @staticmethod
        async def list(workspace: Workspace, name: str) -> list[TableRef]: ...

class partition:
    """``pqbench.native.partition``: a table's natural partitions."""

    class list:
        """``pqbench.native.partition.list``: list a table's partitions."""

        @staticmethod
        def list(
            lake: Lake,
            table: str,
            uri: str,
            env: dict[str, str],
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
            env: dict[str, str],
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
        """``pqbench.native.credentials.get``: the lease, written on the ref."""

        @staticmethod
        async def get(workspace: Workspace, table: TableRef) -> TableRef:
            """The ref with its vended read credentials on it, as ``credentials``.

            ``access_key_id``/``secret_access_key``/``session_token`` are the
            lease; the ref comes back unchanged when the catalog vends nothing
            for the table.
            """
            ...

class bytemass:
    """``pqbench.native.bytemass``: the per-column byte masses of Parquet files."""

    @staticmethod
    async def bytemass(lake: Lake, request: BytemassRequest) -> list[MassRow]:
        """Measure the files the request names, one row per column chunk."""
        ...

class lz:
    """``pqbench.native.lz``: the compression codecs, over a local file."""

    @staticmethod
    def lz(request: LzRequest) -> dict[str, Any]:
        """Benchmark the codecs on the file, as the crate's report document."""
        ...

class compression:
    """``pqbench.native.compression``: the codecs, over a Parquet file's column chunks."""

    @staticmethod
    def compression(request: CompressionRequest) -> dict[str, Any]: ...

class profile:
    """``pqbench.native.profile``: a sample's columns, summarized."""

    @staticmethod
    def profile(sample: Sample, request: ProfileRequest) -> dict[str, Any]: ...

class experiment:
    """``pqbench.native.experiment``: what a rewrite would change."""

    @staticmethod
    def experiment(sample: TypedSample, request: ExperimentRequest) -> dict[str, Any]: ...

class diff:
    """``pqbench.native.diff``: two bytemass records, compared."""

    @staticmethod
    def compare(
        left: Sequence[MassRow],
        right: Sequence[MassRow],
        depth: int | None,
    ) -> dict[str, Any]: ...

class skill:
    """``pqbench.native.skill``: the compiled agent skills."""

    @staticmethod
    def skills() -> list[dict[str, Any]]: ...
    @staticmethod
    def skill(name: str) -> dict[str, Any]: ...
    @staticmethod
    def document(name: str, document: str) -> str: ...

class viz:
    """``pqbench.native.viz``: a bytemass stream as a treemap page."""

    @staticmethod
    def write_report(
        prefix: str,
        rows: Sequence[MassRow],
        files: Sequence[TableFile] | None = None,
    ) -> None: ...

class third_party:
    """``pqbench.native.third_party``: the third-party facades the crate exposes."""

    class parquet:
        """``pqbench.native.third_party.parquet``: a Parquet file's rows, sampled."""

        @staticmethod
        def read_sample(path: str, max_rows: int | None) -> Sample: ...
        @staticmethod
        def read_typed_sample(path: str, max_rows: int | None) -> TypedSample: ...
