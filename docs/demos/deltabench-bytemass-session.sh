#!/bin/sh
set -eu

run_deltabench() {
    docker run --rm --platform "$PQBENCH_DEMO_PLATFORM" \
        -v "$PWD:/src:ro" \
        -v "$PWD/.docker-data/stackexchange-delta:/demo:ro" \
        -v "$PQBENCH_DEMO_TARGET:/src/target" \
        -w /src rust:1.91.1-bookworm \
        target/debug/deltabench /demo "$@"
}

prompt() {
    printf '\033[1;32m$\033[0m %s\n' "$*"
    sleep 2
}

prompt deltabench --help
docker run --rm --platform "$PQBENCH_DEMO_PLATFORM" \
    -v "$PWD:/src:ro" \
    -v "$PQBENCH_DEMO_TARGET:/src/target" \
    -w /src rust:1.91.1-bookworm \
    target/debug/deltabench --help
sleep 4

prompt deltabench ./delta-table
run_deltabench
sleep 4

prompt deltabench ./delta-table --version 0 --json
run_deltabench --version 0 --json >.docker-data/deltabench-report.json
sed -n '1,20p' .docker-data/deltabench-report.json
printf '... JSON truncated for preview\n'
sleep 4

prompt "deltabench ./delta-table --d3 > treemap.html"
run_deltabench --d3 >.docker-data/deltabench-treemap.html
wc -c .docker-data/deltabench-treemap.html | awk '{ print "wrote treemap.html (" $1 " bytes)" }'
sleep 5
