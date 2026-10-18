#!/bin/sh
# The wheel's version lives in python/Cargo.toml, which cannot inherit
# [workspace.package]: python/ is its own workspace root, so the local gate
# never compiles pyo3 (see AGENTS.md). This script is the one definition of that
# value — no argument prints the workspace version, `sync` writes it into the
# wheel's manifest, `check` fails when the two disagree.
set -eu

root=$(dirname "$0")/..
manifest="$root/python/Cargo.toml"

version() { sed -n 's/^version = "\(.*\)"/\1/p' "$1" | head -1; }

workspace=$(version "$root/Cargo.toml")
[ -n "$workspace" ] || { echo "no [workspace.package] version in Cargo.toml" >&2; exit 1; }

case ${1:-} in
"")
    echo "$workspace"
    ;;
check)
    carried=$(version "$manifest")
    [ "$carried" = "$workspace" ] || {
        echo "python/Cargo.toml carries $carried, but the workspace is $workspace" >&2
        echo "run: make sync-version" >&2
        exit 1
    }
    ;;
sync)
    tmp="$manifest.new"
    sed "s/^version = \".*\"/version = \"$workspace\"/" "$manifest" >"$tmp"
    mv "$tmp" "$manifest"
    echo "python/Cargo.toml: $workspace"
    ;;
*)
    echo "usage: $0 [check|sync]" >&2
    exit 2
    ;;
esac
