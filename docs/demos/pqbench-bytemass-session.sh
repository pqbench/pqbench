#!/bin/sh
set -eu

pqbench() {
    "${PQBENCH:?set PQBENCH to the pqbench binary}" "$@"
}

prompt() {
    printf '\033[1;32m$\033[0m %s\n' "$*"
    sleep 1
}

parquet=crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet

prompt "pqbench bytemass $parquet"
pqbench bytemass "$parquet"
sleep 2

prompt "pqbench bytemass $parquet --json"
pqbench bytemass "$parquet" --json
sleep 2
