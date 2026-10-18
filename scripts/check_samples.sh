#!/bin/sh
# Verify the committed open-dataset parquet samples in data/samples/.
#
# The files are tracked by Git LFS (see .gitattributes). A clone that skipped
# LFS has the pointer, not the parquet, so this checks the magic bytes and
# tells you to run `git lfs pull`. Run via `make samples`.
set -eu

root="$(cd "$(dirname "$0")/.." && pwd)"
out="$root/data/samples"

status=0
for name in \
	alltypes_plain.parquet \
	alltypes_dictionary.parquet \
	nested_lists.snappy.parquet \
	nested_structs.rust.parquet \
	nested_maps.snappy.parquet \
	delta_byte_array.parquet \
	squad.parquet
do
	if [ ! -f "$out/$name" ]; then
		echo "missing   $name" >&2
		status=1
	elif [ "$(head -c4 "$out/$name")" != "PAR1" ]; then
		echo "pointer   $name" >&2
		status=1
	else
		echo "ok        $name"
	fi
done

if [ "$status" -ne 0 ]; then
	echo "samples incomplete: run \`git lfs pull\`" >&2
	exit 1
fi
echo "samples in $out"
