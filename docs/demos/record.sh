#!/bin/sh
# Re-record the terminal GIFs against this branch's CLI.
# Uses the committed lakehouse fixtures and the small Parquet test file.
# Catalog pipes are not recorded here; they need `make lakehouse`.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

for command in asciinema agg cargo; do
    command -v "$command" >/dev/null 2>&1 || {
        echo "missing required command: $command" >&2
        exit 1
    }
done

test -f docker/e2e-lakehouse/table/_delta_log/00000000000000000000.json || {
    echo "missing committed Delta fixture under docker/e2e-lakehouse/table" >&2
    exit 1
}

CARGO=${CARGO:-cargo}
$CARGO build -p pqbench-cli --features delta
export PQBENCH="${CARGO_TARGET_DIR:-$root/target}/debug/pqbench"
test -x "$PQBENCH"

asciinema rec --headless --overwrite --return --window-size 100x28 \
    --command "sh docs/demos/pqbench-lake-session.sh" \
    /tmp/pqbench-lake.cast
asciinema rec --headless --overwrite --return --window-size 100x28 \
    --command "sh docs/demos/pqbench-bytemass-session.sh" \
    /tmp/pqbench-bytemass.cast
asciinema rec --headless --overwrite --return --window-size 100x28 \
    --command "sh docs/demos/pqbench-delta-session.sh" \
    /tmp/pqbench-delta.cast

agg --theme github-dark --font-size 20 --idle-time-limit 6 \
    /tmp/pqbench-lake.cast docs/images/pqbench-lake.gif
agg --theme github-dark --font-size 20 --idle-time-limit 6 \
    /tmp/pqbench-bytemass.cast docs/images/pqbench-bytemass.gif
agg --theme github-dark --font-size 20 --idle-time-limit 6 \
    /tmp/pqbench-delta.cast docs/images/pqbench-delta-bytemass.gif
