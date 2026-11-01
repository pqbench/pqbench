#!/bin/sh
set -eu

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
