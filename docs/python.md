# Python bindings

The `pqbench` wheel is a PyO3 binding: each CLI command is a function that calls
the same library entry point in-process, so measurement fits inside a notebook or
a pipeline. The text below is included from the source
[`python/README.md`](https://github.com/pqbench/pqbench/blob/main/python/README.md),
so it cannot drift from the wheel.

--8<-- "python/README.md:7:"
