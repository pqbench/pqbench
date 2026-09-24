#!/bin/sh
# Enforce the third-party isolation rule (see crates/pqbench/src/third_party).
#
# Feature flags live in exactly one place: the `impl` of a third_party wrapper.
#   third_party/<crate>/api.rs   the isolated surface. No `#[cfg(feature = ...)]`
#                                may appear here; it always compiles, and
#                                feature-disabled behavior is a *runtime* error.
#   third_party/<crate>/impl.rs  the only place feature flags live. It dispatches
#   third_party/<crate>/impl/*   to a private backend and, without the feature,
#                                returns an error naming it.
#
# So a `cfg(feature = ...)` anywhere else under a crate's `src/` is a violation:
# not in the library, not in the CLI, not in a wrapper root, not in an api.rs.
# Tests are exempt — they gate freely on features, and `#[cfg(test)]` code is not
# checked.
#
# The check is textual and deterministic: any `cfg(feature = ...)` or
# `cfg!(feature = ...)` outside an allowed impl file is a violation. A match that
# sits after a `//` comment marker is prose, not a leak.
#
# Output mirrors the aipnaming CLI:
#   default   file:line:col: error[isolation/feature-cfg]: message
#   --github  ::error file=...,line=...,col=...,title=...::message workflow
#             commands, which GitHub renders inline on the PR diff. Also
#             selected when $GITHUB_ACTIONS is set.
# A per-file count and a "leak" summary go to stderr; findings go to stdout
# (so `--github | tee annotations.txt` captures only annotations). The exit
# status is 0 when clean and 1 when a feature flag leaks. Run it from the repo
# root.
#
# Usage: scripts/check_isolation.sh [--github]

set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

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

[ -d "$root/crates/pqbench/src/third_party" ] || {
    echo "FAIL: no third_party directory under $root/crates/pqbench/src" >&2
    exit 1
}

# The feature-cfg lines of one file, as `line:col:text`.
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

# Whether a feature flag in `rel` (repo-relative) is allowed.
#
# Allowed only in a third_party wrapper's impl file:
#   crates/*/src/third_party/<crate>/impl.rs
#   crates/*/src/third_party/<crate>/impl/<file>.rs
allowed() {
    case $1 in
        */src/third_party/*/impl.rs) return 0 ;;
        */src/third_party/*/impl/*.rs) return 0 ;;
        *) return 1 ;;
    esac
}

# A reason for a rejected flag, keyed on where it was found.
reason() {
    case $1 in
        */third_party/*/api.rs) echo "feature flag in the isolated api.rs; move it to impl.rs" ;;
        */third_party/*) echo "feature flag in a third_party wrapper outside an impl file" ;;
        *) echo "feature flag outside third_party; move it into a wrapper's impl" ;;
    esac
}

# Workflow-command data escapes `%`, `\r`, and `\n`.
escape() {
    printf '%s' "$1" | sed -e 's/%/%25/g' -e 's/\r/%0D/g' -e 's/\n/%0A/g'
}

# One annotation for `line:col:text` about `rel` with `message`.
emit() {
    rel=$1
    message=$2
    line=${3%%:*}
    rest=${3#*:}
    col=${rest%%:*}
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

api_count=0
leaks=0

# Every crate source file, tests excluded.
for crate in "$root"/crates/*; do
    [ -d "$crate/src" ] || continue
    for src in $(find "$crate/src" -type f -name '*.rs'); do
        rel=${src#"$root"/}
        case $rel in */api.rs) api_count=$((api_count + 1)) ;; esac
        allowed "$rel" && continue
        hits=$(leaks_in "$src")
        [ -n "$hits" ] || continue
        message=$(reason "$rel")
        while IFS= read -r hit; do
            emit "$rel" "$message" "$hit"
            leaks=$((leaks + 1))
        done <<EOF
$hits
EOF
    done
done

if [ "$api_count" -eq 0 ]; then
    echo "FAIL: found no third_party api.rs files under $root/crates" >&2
    exit 1
fi

if [ "$leaks" -ne 0 ]; then
    echo "isolation: $leaks feature flag(s) outside a third_party impl file" >&2
    exit 1
fi

echo "isolation: ok, $api_count api.rs files and no feature flag outside a third_party impl" >&2
