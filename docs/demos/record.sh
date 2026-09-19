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

test -f .docker-data/deltabench-example/alltypes_dictionary.parquet || {
    echo "stage the documented Delta example under .docker-data first" >&2
    exit 1
}

case $(docker info --format '{{.Architecture}}') in
    aarch64 | arm64)
        platform=linux/arm64
        target_volume=pqbench-cache-target-191
        registry_volume=pqbench-cache-registry-191
        git_volume=pqbench-cache-git-191
        ;;
    x86_64 | amd64)
        platform=linux/amd64
        target_volume=pqbench-cache-target-191-amd64
        registry_volume=pqbench-cache-registry-191-amd64
        git_volume=pqbench-cache-git-191-amd64
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
    -w /src rust:1.91.1-bookworm \
    cargo build --locked --package pqbench-cli --package deltabench-cli

export PQBENCH_DEMO_PLATFORM=$platform
export PQBENCH_DEMO_TARGET=$target_volume
asciinema rec --headless --overwrite --return --window-size 100x28 \
    --command "sh docs/demos/pqbench-bytemass-session.sh" \
    /tmp/pqbench-bytemass.cast
asciinema rec --headless --overwrite --return --window-size 100x28 \
    --command "sh docs/demos/deltabench-bytemass-session.sh" \
    /tmp/deltabench-bytemass.cast

agg --theme github-dark --font-size 20 --idle-time-limit 2 \
    /tmp/pqbench-bytemass.cast docs/images/pqbench-bytemass.gif
agg --theme github-dark --font-size 20 --idle-time-limit 2 \
    /tmp/deltabench-bytemass.cast docs/images/deltabench-bytemass.gif
