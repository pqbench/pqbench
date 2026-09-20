#!/bin/sh
set -eu

run_pqbench() {
    docker run --rm --platform "$PQBENCH_DEMO_PLATFORM" \
        -v "$PWD:/src:ro" \
        -v "$PWD/.docker-data/nyc-taxi:/demo:ro" \
        -v "$PQBENCH_DEMO_TARGET:/src/target" \
        -w /src rust:1.94.1-bookworm \
        target/debug/pqbench "$@"
}

prompt() {
    printf '\033[1;32m$\033[0m %s\n' "$*"
    sleep 2
}

prompt pqbench --help
run_pqbench --help
sleep 4

prompt pqbench bytemass --help
run_pqbench bytemass --help
sleep 4

prompt pqbench bytemass yellow_tripdata_2024-01.parquet
run_pqbench bytemass /demo/yellow_tripdata_2024-01.parquet
sleep 4

prompt "pqbench bytemass 'yellow_tripdata_*.parquet' --json"
run_pqbench bytemass '/demo/yellow_tripdata_*.parquet' --json >.docker-data/pqbench-bytemass.json
sed -n '1,18p' .docker-data/pqbench-bytemass.json
printf '... JSON truncated for preview\n'
sleep 4

prompt "pqbench bytemass yellow_tripdata_2024-01.parquet --d3 > treemap.html"
run_pqbench bytemass /demo/yellow_tripdata_2024-01.parquet --d3 >.docker-data/pqbench-treemap.html
wc -c .docker-data/pqbench-treemap.html | awk '{ print "wrote treemap.html (" $1 " bytes)" }'
sleep 5
