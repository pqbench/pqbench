# Data modelling

The vocabulary the commands share, and the rules that keep it from becoming a
module everything depends on.

Commands agree on this page, not on a shared implementation. Each one declares
the model itself — its own types, its own parsing — and this document is what
makes those declarations interchangeable. A shared *type* couples every command
to one module: a change reaches every user. A shared *specification* lets each
command change on its own and still speak the same documents. That is the trade
this page makes, and the reason it exists.

## Three kinds of type

| kind | identity | parent | on the wire |
| --- | --- | --- | --- |
| **Resource** | a path-shaped `name` | exactly one | one per line |
| **Value type** | none — a resource carries it | none | nested only |
| **Document** | none — it is a message | none | a method's body |

A resource is addressable: it has a `name`, one canonical parent, and
`Get`/`List`. A value type has no identity and is carried inside a resource —
`FileStats` in a `TableFile`, `LogAction` in a `Commit`, `DeclaredColumn` in a
`Table`. A document is neither: it is the body of a custom method's request or
response, such as `Credentials` or `ReadEligibility`.

## Names

- A resource's first field is `name`, the full resource name (AIP-122).
- The collection segment identifies the type: `…/tables/{table}` is a `Table`,
  `…/blobs/{service}/{locator}` is a `Blob`, `…/columns/{column}/bytemass` is a
  `Bytemass`. A reader never needs a tag to know what a line is.
- Names are relative (AIP-122) and are pqbench's own identifiers, not the
  backend's URL. The dialect maps a name to Unity's or Iceberg's REST endpoint,
  so no document carries a transport detail.
- An id segment is any string that is one URI path segment and decodes back to
  what it names: a plain name, a URL-encoded path (`/` as `%2F`), a dotted
  column path, or a number. Ids are not user-specified, so AIP-122 asks only
  that the format be documented here — its RFC-1034 character-set advice comes
  from the DNS spec and does not apply.
- A resource exposes only three identification fields: `name`, `{resource}_id`
  (the id segment, output only), and `uid` (UUID4, output only). No `id`, no
  self-link, no tuple (AIP-122/148).
- A dotted FQN (`dbx_samples.nyctaxi.trips`) is a **lookup alias**, input only.
  It is never a field and never appears in a document: the CLI translates it to
  the path form, and every returned document carries the canonical name. The CLI
  tells the two apart by the first segment — a known collection identifier is
  canonical, a dotted tuple is an alias.
- A table no catalog named — one read by storage path — still gets a canonical
  name: it takes the `local` catalog and schema, so every partition and file
  under it has one too.

## One canonical parent

A resource has at most one canonical parent, and a `List` must not require two
(AIP-124).

| resource | canonical parent |
| --- | --- |
| `Catalog` | `Metastore` |
| `Schema` | `Catalog` |
| `Table` | `Schema` |
| `Partition`, `TableFile`, `Commit` | `Table` |
| `Column` | `Blob` |
| `Bytemass`, `ColumnProfile` | `Column` |

`Blob` is the exception: it is storage-rooted (`blobs/{service}/{locator}`), so
a bare object or a local path names a resource like any other, and it has no
parent.

Two consequences:

- A `TableFile`'s parent is the **table**, not the partition. A partition is a
  lens over the files a snapshot already lists, not an owner; its files are a
  filtered list on the table.
- An analysis's parent is the **column**. A coarser scope is an aggregate, not a
  second parent: a file-level read uses the `-` wildcard id (AIP-159).

## Fields

- Absence is `Option`. There is no sentinel: a Rust enum has no
  `_UNSPECIFIED` value, and an absent string is `None`, not `""`.
- Where a proto idiom is not the Rust idiom, **Rust wins and the wire keeps the
  proto's spelling through `serde`**. Proto requires a zero-value enum variant,
  a prefixed enum value, and an `int64`; the Rust type has a plain variant, an
  unsigned quantity, and `serde` spells the wire form back.
- A quantity that cannot be negative is unsigned. A count is `_count`, never
  `num_` (AIP-141). Units are spelled out — `size_bytes`, not `size`.
- An instant is `_time`, an RFC-3339 string (AIP-142); a span is `_duration`. A
  half-open window is `[start_time, end_time)` (AIP-145).
- A range is `first_values`/`last_values`, inclusive, never `min`/`max`
  (AIP-145).
- A field is a noun, not a verb (AIP-140), and a `bool` carries no verb prefix.
- A nested message is a value type or a document, never a resource.

## The wire

- One resource per NDJSON line, in the proto3 JSON mapping — except that field
  names are spelled as here (`snake_case`), not the mapping's `lowerCamelCase`.
  An enum is its value name, an `int64` is a JSON string, an instant is an
  RFC-3339 string.

  ```json
  {"name":"catalogs/dbx_samples/schemas/nyctaxi","comment":"NYC taxi data"}
  ```

- There is **no envelope**: no `kind`, no per-document `version`. A stage reads
  exactly one resource type, identified by the name's collection segment, and a
  genuinely mixed stream is a `oneof` — never a tag on a resource.
- The API version is the package's (`pqbench.v1`), not a segment of a name. A
  name is self-describing by its collection segment, and no document carries a
  version of its own.
- AIP's field annotations are deliberately absent. The documents are native
  Rust, so the type system carries what the annotations would describe — an
  absent value is an `Option`, an output-only field is not an input parameter —
  and there is no generator to read them. The AIP *shape* (names, patterns,
  field names) still applies.
- Credentials ride beside the resource that needs them — a `Table`, a
  `Partition`, a `TableFile` — never as a field of it (AIP-147), so a pipeline
  stays in band. They are treated as non-sensitive for now; a later pass moves
  them off the wire.
- A table references a blob through the `TableFile` association, not as its
  parent (AIP-124).

The field-by-field contract is [`v1.proto`](v1.proto); this page is the rules
that keep it coherent.

## Where the model stops

This page is the vocabulary, not the API. Two rules keep it from becoming a
coupling point:

- **A command's signature takes the plain values it needs, not a document.** A
  document passed whole makes every caller know every field, and a change to any
  field reaches every caller. Configuration, pagination, and dialect state
  belong to the implementation, never to the signature.
- **The CLI owns the documents.** It parses one, calls one command, and formats
  the result, so it is the place that stays flexible about inputs — composition
  is its job.

## Adding a type

1. Pick the kind. Addressable and listed? Resource. Carried inside one? Value
   type. A method's body? Document.
2. A resource: `name` first, one canonical parent, only the three
   identification fields.
3. A value type or document: no `name`, no parent, no identity.
4. Absence is `Option`; no sentinel; Rust wins over the proto idiom.
5. A type with no producer yet is the design target. It stays in the model, and
   stays out of the wire until a command emits it.
