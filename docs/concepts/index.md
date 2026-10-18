# Concepts

Background that makes the commands read the way they do — the data model, and
the rules that keep the tool decoupled.

- [What is a partition?](../partition.md) — why a partition is a *lens* on the
  files a snapshot already lists, not a thing the table stores.
- [Data modelling](data-modelling.md) — the shared vocabulary: the three kinds
  of type, how a name is built, and the rules that keep the model from becoming
  a module everything depends on.
- [Architecture](../architecture.md) — the command layout, third-party
  isolation, the feature flags, and the versioned wire format between commands.
