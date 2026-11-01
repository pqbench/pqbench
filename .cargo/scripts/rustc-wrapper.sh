#!/bin/sh
set -eu

# Link with mold when it is installed: GNU ld takes ~9 s to link one
# all-features test binary, mold ~2 s. Only link steps (the invocations that
# emit a linked artifact) are touched, so compile/cache keys are unchanged and
# a machine without mold keeps the default linker.
link=0
for argument in "$@"; do
    case "$argument" in
        --emit=*link*) link=1 ;;
    esac
done
if [ "$link" = 1 ] && command -v mold >/dev/null 2>&1; then
    set -- "$@" -C link-arg=-fuse-ld=mold
fi

if command -v sccache >/dev/null 2>&1; then
    exec sccache "$@"
fi

# Non-interactive shells often have a minimal PATH (a flatpak app sees only
# /app/bin:/usr/bin), so a sccache installed by `cargo install` in the user's
# bin dir is missed and the wrapper silently falls back to plain rustc. Try
# the standard locations before giving up.
for candidate in "$HOME/.local/bin/sccache" "$HOME/.cargo/bin/sccache"; do
    if [ -x "$candidate" ]; then
        exec "$candidate" "$@"
    fi
done

exec "$@"
