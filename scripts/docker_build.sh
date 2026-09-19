#!/bin/sh
# Choose the CPU baseline and build the pqbench release binary.
#
# TARGETARCH is provided by buildx during multi-platform builds (amd64 / arm64 /
# ...). By default a portable per-arch baseline is used so the published image
# runs on any CPU; NATIVE=1 opts into exact-host tuning (local only). The C/C++
# codecs use -march on x86 but -mcpu on ARM, because an ARM baseline like
# neoverse-n1 is a CPU name, not an architecture. This overrides
# .cargo/config.toml's -march=native while retaining the other codec flags.
# Baseline numbers differ from a native build; see docs/docker.md.
set -eu

TARGETARCH=${TARGETARCH:-}
NATIVE=${NATIVE:-0}
CPU=${CPU:-}

case "$TARGETARCH" in
  amd64|x86_64)   cpuflag=-march ;;
  arm64|arm64/v8) cpuflag=-mcpu ;;
  *)              cpuflag=-march ;;
esac

if [ "$NATIVE" = "1" ]; then
    march=native
elif [ -n "$CPU" ]; then
    march="$CPU"
else
    case "$TARGETARCH" in
      arm64|arm64/v8) march=neoverse-n1 ;;
      amd64|x86_64)   march=x86-64-v3 ;;
      *)              march=generic ;;
    esac
fi

codec_flags="-O3 -DNDEBUG -fPIE -fomit-frame-pointer -fstrict-aliasing -ffast-math -DHAVE_BUILTIN_CTZ=1"
export CXXFLAGS="$codec_flags $cpuflag=$march"
export RUSTFLAGS="-C target-cpu=$march"

exec cargo build --locked --release --package pqbench-cli
