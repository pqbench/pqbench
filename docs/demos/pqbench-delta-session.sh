#!/bin/sh
set -eu

run_pqbench() {
    docker run --rm --platform "$PQBENCH_DEMO_PLATFORM" \
        -v "$PWD:/src:ro" \
        -v "$PWD/.docker-data/stackexchange-delta:/demo:ro" \
        -v "$PQBENCH_DEMO_TARGET:/src/target" \
        -w /src rust:1.94.1-bookworm \
        target/debug/pqbench delta /demo "$@"
}

prompt() {
    printf '\033[1;32m$\033[0m %s\n' "$*"
    sleep 2
}

prompt pqbench delta --help
docker run --rm --platform "$PQBENCH_DEMO_PLATFORM" \
    -v "$PWD:/src:ro" \
    -v "$PQBENCH_DEMO_TARGET:/src/target" \
    -w /src rust:1.94.1-bookworm \
    target/debug/pqbench delta --help
sleep 4

prompt pqbench delta ./delta-table
run_pqbench
sleep 4

prompt pqbench delta ./delta-table --version 0 --json
run_pqbench --version 0 --json >.docker-data/pqbench-delta-report.json
sed -n '1,20p' .docker-data/pqbench-delta-report.json
printf '... JSON truncated for preview\n'
sleep 4

prompt "pqbench delta ./delta-table --d3 > treemap.html"
run_pqbench --d3 >.docker-data/pqbench-delta-treemap.html
wc -c .docker-data/pqbench-delta-treemap.html | awk '{ print "wrote treemap.html (" $1 " bytes)" }'
sleep 5
