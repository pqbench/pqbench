#!/bin/sh
# Enforce the third-party isolation rule (see crates/pqbench/src/third_party).
#
# Each wrapper under crates/pqbench/src/third_party/<crate>/ splits into:
#   api.rs   the isolated surface. No `#[cfg(feature = ...)]` may appear here;
#            it always compiles, and feature-disabled behavior is a *runtime*
#            error returned by the private `impl` module.
#   impl.rs  the only place feature flags live. It dispatches to a private
#            backend module and, without the feature, returns an error naming
#            it. The third-party crate name is named only under impl/.
#
# Tests may enable features freely, so `#[cfg(test)]` / `#[test]` code is not
# checked.
#
# The check is textual and deterministic: any `cfg(feature = ...)` or
# `cfg!(feature = ...)` inside an `api.rs` is a violation.
#
# Output mirrors the aipnaming CLI:
#   default   file:line:col: error[isolation/feature-cfg]: message
#   --github  ::error file=...,line=...,col=...,title=...::message workflow
#             commands, which GitHub renders inline on the PR diff. Also
#             selected when $GITHUB_ACTIONS is set.
# A per-file count and a "leak" summary go to stderr; findings go to stdout
# (so `--github | tee annotations.txt` captures only annotations). The exit
# status is 0 when clean and 1 when a wrapper leaks a feature flag. Run it from
# the repo root.
#
# Usage: scripts/check_isolation.sh [--github]

set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
dir="$root/crates/pqbench/src/third_party"

format=text
for arg in "$@"; do
    case $arg in
        --github) format=github ;;
        -h | --help)
            sed -n '/^# Usage:/q; 2,$p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "usage: $0 [--github]" >&2
            exit 2
            ;;
    esac
done
[ -n "${GITHUB_ACTIONS:-}" ] && format=github

command -v grep >/dev/null 2>&1 || {
    echo "FAIL: grep is required" >&2
    exit 1
}

[ -d "$dir" ] || {
    echo "FAIL: no third_party directory at $dir" >&2
    exit 1
}

# The leak lines of one api.rs, as `line:col:text`.
#
# Matches that sit after a `//` comment marker are prose, not a leak. The 1-based
# column is the byte offset of the `cfg` token, matching rustc's diagnostic.
leaks_in() {
    grep -nE 'cfg(\(|!\().*feature[[:space:]]*=' "$1" \
        | while IFS= read -r hit; do
            line=${hit%%:*}
            text=${hit#*:}
            case $text in
                *//*) [ "${text%%//*}" = "$text" ] || continue ;;
            esac
            col=$(awk -v line="$text" 'BEGIN { print index(line, "cfg") }')
            printf '%s:%s:%s\n' "$line" "$col" "$text"
        done
}

# Workflow-command data escapes `%`, `\r`, and `\n`.
escape() {
    printf '%s' "$1" | sed -e 's/%/%25/g' -e 's/\r/%0D/g' -e 's/\n/%0A/g'
}

# One annotation for `line:col:text` about `rel`, in the chosen format.
emit() {
    rel=$1
    line=${2%%:*}
    rest=${2#*:}
    col=${rest%%:*}
    message="feature flag in the isolated api.rs; move it to impl.rs"
    case $format in
        github)
            printf '::error file=%s,line=%s,col=%s,title=isolation/feature-cfg::%s\n' \
                "$rel" "$line" "$col" "$(escape "$message")"
            ;;
        *)
            printf '%s:%s:%s: error[isolation/feature-cfg]: %s\n' \
                "$rel" "$line" "$col" "$message"
            ;;
    esac
}

count=0
leaks=0

for api in "$dir"/*/api.rs; do
    [ -e "$api" ] || continue
    count=$((count + 1))
    rel=${api#"$root"/}
    hits=$(leaks_in "$api")
    [ -n "$hits" ] || continue
    while IFS= read -r hit; do
        emit "$rel" "$hit"
        leaks=$((leaks + 1))
    done <<EOF
$hits
EOF
done

if [ "$count" -eq 0 ]; then
    echo "FAIL: found no third_party/<crate>/api.rs files under $dir" >&2
    exit 1
fi

if [ "$leaks" -ne 0 ]; then
    echo "isolation: $leaks feature flag(s) leaked into an api.rs" >&2
    exit 1
fi

echo "isolation: ok, $count api.rs files carry no feature flag" >&2
