#!/bin/sh
# Lists the committed Iceberg fixture. Measuring it needs rustfs + iceberg-s3
# because the manifests name s3://lakehouse/... — run `make lakehouse` first.
set -eu

pqbench() {
    "${PQBENCH:?set PQBENCH to the pqbench binary}" "$@"
}

prompt() {
    printf '\033[1;32m$\033[0m %s\n' "$*"
    sleep 1
}

prompt "pqbench table ls docker/e2e-lakehouse/iceberg -o /tmp/pqbench-demo-iceberg.ndjson.zst"
pqbench table ls docker/e2e-lakehouse/iceberg -o /tmp/pqbench-demo-iceberg.ndjson.zst
sleep 2

# The REST pipe needs the local stand (`make lakehouse`); run it when it answers.
if curl -sf http://localhost:8181/v1/config >/dev/null 2>&1; then
    prompt "pqbench schema ls CAT.SCHEMA < docs/demos/iceberg-rest.json | pqbench tablev2 info | pqbench table ls | pqbench partition ls | pqbench bytemass --json"
    pqbench schema ls CAT.SCHEMA < docs/demos/iceberg-rest.json | pqbench tablev2 info | pqbench table ls | pqbench partition ls | pqbench bytemass --json | cat
    sleep 2
fi
