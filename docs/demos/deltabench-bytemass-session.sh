#!/bin/sh
set -eu
printf '\033[1;32m$\033[0m deltabench ./delta-table\n'
sleep 1
docker run --rm --platform "$PQBENCH_DEMO_PLATFORM" \
    -v "$PWD:/src:ro" \
    -v "$PWD/.docker-data/deltabench-example:/demo:ro" \
    -v "$PQBENCH_DEMO_TARGET:/src/target" \
    -w /src rust:1.91.1-bookworm \
    target/debug/deltabench /demo
sleep 2
