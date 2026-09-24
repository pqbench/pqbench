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
# `cfg!(feature = ...)` inside an `api.rs` is a violation. Run it from the
# repository root.

set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
dir="$root/crates/pqbench/src/third_party"

command -v rg >/dev/null 2>&1 || {
    echo "FAIL: ripgrep (rg) is required" >&2
    exit 1
}

[ -d "$dir" ] || {
    echo "FAIL: no third_party directory at $dir" >&2
    exit 1
}

fail=0
count=0

for api in "$dir"/*/api.rs; do
    [ -e "$api" ] || continue
    count=$((count + 1))
    # Skip matches that sit after a `//` comment marker on the line; a doc or
    # prose mention of the rule is not a leak.
    hits=$(
        rg -n --no-heading 'cfg(\(|!\().*feature[[:space:]]*=' "$api" \
            | while IFS= read -r line; do
                code=${line#*:}
                case $code in
                    *//*) [ "${code%%//*}" = "$code" ] || continue ;;
                esac
                printf '%s\n' "$line"
            done
    )
    if [ -n "$hits" ]; then
        fail=1
        echo "FAIL: feature flag in an api.rs: ${api#"$root"/}" >&2
        echo "$hits" >&2
    fi
done

if [ "$count" -eq 0 ]; then
    echo "FAIL: found no third_party/<crate>/api.rs files under $dir" >&2
    exit 1
fi

if [ "$fail" -ne 0 ]; then
    echo "FAIL: feature flags leaked into an api.rs" >&2
    exit 1
fi

echo "ok: $count api.rs files carry no feature flag"
