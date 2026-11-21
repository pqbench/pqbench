#!/bin/sh
set -eu

# Link with mold when it is installed: GNU ld takes ~9 s to link one
# all-features test binary, mold ~2 s. Only link steps (the invocations that
# emit a linked artifact) are touched, so compile keys are unchanged and a
# machine without mold keeps the default linker.
link=0
for argument in "$@"; do
    case "$argument" in
        --emit=*link*) link=1 ;;
    esac
done
if [ "$link" = 1 ] && command -v mold >/dev/null 2>&1; then
    set -- "$@" -C link-arg=-fuse-ld=mold
fi

exec "$@"
