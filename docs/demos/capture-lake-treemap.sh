#!/bin/sh
# One still of `table | bytemass | viz`. This is not a lake click-through.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

chrome="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
test -x "$chrome" || {
    echo "Google Chrome is required to capture the table treemap" >&2
    exit 1
}
command -v ffmpeg >/dev/null 2>&1 || {
    echo "missing required command: ffmpeg" >&2
    exit 1
}

bin="${CARGO_TARGET_DIR:-$root/target}/debug/pqbench"
test -x "$bin" || cargo build -p pqbench-cli --features delta

html=.docker-data/pqbench-table.html
frames=.docker-data/table-frames
mkdir -p "$frames" .docker-data
"$bin" table docker/e2e-lakehouse/table |
    "$bin" bytemass |
    "$bin" viz -o .docker-data/pqbench-table

"$chrome" --headless=new --disable-gpu --hide-scrollbars \
    --screenshot="$frames/frame-0.png" --window-size=1440,900 \
    --virtual-time-budget=5000 \
    "file://$root/$html"

ffmpeg -y -framerate 1/2 -i "$frames/frame-%d.png" \
    -vf "fps=2,split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse" \
    docs/images/pqbench-lake-treemap.gif
