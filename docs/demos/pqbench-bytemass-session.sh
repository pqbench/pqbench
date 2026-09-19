#!/bin/sh
set -eu

run_pqbench() {
    docker run --rm --platform "$PQBENCH_DEMO_PLATFORM" \
        -v "$PWD:/src:ro" \
        -v "$PWD/.docker-data/deltabench-example:/demo:ro" \
        -v "$PQBENCH_DEMO_TARGET:/src/target" \
        -w /src rust:1.91.1-bookworm \
        target/debug/pqbench "$@"
}

prompt() {
    printf '\033[1;32m$\033[0m %s\n' "$*"
    sleep 1
}

prompt pqbench --help
run_pqbench --help
sleep 2

prompt pqbench bytemass --help
run_pqbench bytemass --help
sleep 2

prompt pqbench bytemass alltypes_dictionary.parquet
run_pqbench bytemass /demo/alltypes_dictionary.parquet
sleep 2

prompt "pqbench bytemass 'part-*.parquet' --json"
run_pqbench bytemass '/demo/*.parquet' --json >.docker-data/pqbench-bytemass.json
sed -n '1,18p' .docker-data/pqbench-bytemass.json
printf '... JSON truncated for preview\n'
sleep 2

prompt "pqbench bytemass alltypes_dictionary.parquet --d3 > treemap.html"
run_pqbench bytemass /demo/alltypes_dictionary.parquet --d3 >.docker-data/pqbench-treemap.html
wc -c .docker-data/pqbench-treemap.html | awk '{ print "wrote treemap.html (" $1 " bytes)" }'
sleep 3
