#!/bin/sh
# Fetch public open-dataset parquet files into data/samples/ for local testing.
# Run via `make samples`. Idempotent: skips files already present and valid.
set -eu

root="$(cd "$(dirname "$0")/.." && pwd)"
out="$root/data/samples"
mkdir -p "$out"

base=https://github.com/apache/parquet-testing/raw/master/data
squad=https://huggingface.co/api/datasets/rajpurkar/squad/parquet/plain_text/train/0.parquet

fetch() {
	name="$1"
	url="$2"
	if [ -f "$out/$name" ] && [ "$(head -c4 "$out/$name")" = "PAR1" ]; then
		echo "skip  $name"
		return
	fi
	echo "fetch $name"
	if curl -sL --fail -o "$out/$name" "$url" && [ "$(head -c4 "$out/$name")" = "PAR1" ]; then
		echo "  ok"
	else
		echo "  FAILED $name" >&2
		rm -f "$out/$name"
		exit 1
	fi
}

# Edge-case types and encodings, plus nested struct/list/map column paths.
fetch alltypes_plain.parquet "$base/alltypes_plain.parquet"
fetch alltypes_dictionary.parquet "$base/alltypes_dictionary.parquet"
fetch nested_lists.snappy.parquet "$base/nested_lists.snappy.parquet"
fetch nested_structs.rust.parquet "$base/nested_structs.rust.parquet"
fetch nested_maps.snappy.parquet "$base/nested_maps.snappy.parquet"
fetch delta_byte_array.parquet "$base/delta_byte_array.parquet"
# Real nested data (struct + list) from Hugging Face.
fetch squad.parquet "$squad"

echo "samples in $out"
