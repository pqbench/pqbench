#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

for command in asciinema agg docker; do
    command -v "$command" >/dev/null 2>&1 || {
        echo "missing required command: $command" >&2
        exit 1
    }
done

test -f .docker-data/nyc-taxi/yellow_tripdata_2024-01.parquet || {
    echo "stage the documented NYC Taxi Parquet example under .docker-data first" >&2
    exit 1
}
test -f .docker-data/stackexchange-delta/_delta_log/00000000000000000000.json || {
    echo "stage the documented Stack Exchange Delta example under .docker-data first" >&2
    exit 1
}

case $(docker info --format '{{.Architecture}}') in
    aarch64 | arm64)
        platform=linux/arm64
        target_volume=pqbench-cache-target-194
        registry_volume=pqbench-cache-registry-194
        git_volume=pqbench-cache-git-194
        ;;
    x86_64 | amd64)
        platform=linux/amd64
        target_volume=pqbench-cache-target-194-amd64
        registry_volume=pqbench-cache-registry-194-amd64
        git_volume=pqbench-cache-git-194-amd64
        ;;
    *)
        echo "unsupported Docker architecture" >&2
        exit 1
        ;;
esac

docker run --rm --platform "$platform" \
    -v "$root:/src:ro" \
    -v "$target_volume:/src/target" \
    -v "$registry_volume:/usr/local/cargo/registry" \
    -v "$git_volume:/usr/local/cargo/git" \
    -w /src rust:1.94.1-bookworm \
    cargo build --locked --package pqbench-cli --features delta

export PQBENCH_DEMO_PLATFORM=$platform
export PQBENCH_DEMO_TARGET=$target_volume
asciinema rec --headless --overwrite --return --window-size 100x28 \
    --command "sh docs/demos/pqbench-bytemass-session.sh" \
    /tmp/pqbench-bytemass.cast
asciinema rec --headless --overwrite --return --window-size 100x28 \
    --command "sh docs/demos/pqbench-delta-session.sh" \
    /tmp/pqbench-delta.cast

agg --theme github-dark --font-size 20 --idle-time-limit 6 \
    /tmp/pqbench-bytemass.cast docs/images/pqbench-bytemass.gif
agg --theme github-dark --font-size 20 --idle-time-limit 6 \
    /tmp/pqbench-delta.cast docs/images/pqbench-delta-bytemass.gif
