# syntax=docker/dockerfile:1
FROM rust:1.90-alpine3.22 AS build

# TARGETARCH is set automatically by buildx during multi-platform builds
# (amd64 / arm64 / ...). NATIVE=1 opts into exact-host tuning (local only).
ARG TARGETARCH
ARG CPU=""
ARG NATIVE=0

RUN apk add --no-cache build-base pkgconf zlib-dev zlib-static

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo/ .cargo/
COPY crates/ crates/

# Choose the CPU baseline: a portable per-arch baseline by default (so the
# published image runs on any CPU), or native when NATIVE=1. The C/C++ codecs
# use -march on x86 but -mcpu on ARM, because an ARM baseline like neoverse-n1
# is a CPU name, not an architecture. This overrides .cargo/config.toml's
# -march=native while retaining the other codec flags. Baseline numbers differ
# from a native build; see docs/docker.md.
RUN case "$TARGETARCH" in \
        amd64|x86_64)  cpuflag=-march ;; \
        arm64|arm64/v8) cpuflag=-mcpu ;; \
        *)             cpuflag=-march ;; \
      esac; \
      if [ "$NATIVE" = "1" ]; then \
          march=native; \
      elif [ -n "$CPU" ]; then \
          march="$CPU"; \
      else \
          case "$TARGETARCH" in \
            arm64|arm64/v8) march=neoverse-n1 ;; \
            amd64|x86_64)   march=x86-64-v3 ;; \
            *) march=generic ;; \
          esac; \
      fi; \
      export CXXFLAGS="-O3 -DNDEBUG -fPIE $cpuflag=$march -fomit-frame-pointer -fstrict-aliasing -ffast-math -DHAVE_BUILTIN_CTZ=1"; \
      export RUSTFLAGS="-C target-cpu=$march"; \
      cargo build --locked --release --package pqbench-cli

FROM alpine:3.22 AS runtime
RUN apk add --no-cache ca-certificates

LABEL org.opencontainers.image.title="pqbench" \
      org.opencontainers.image.description="lzbench for parquet" \
      org.opencontainers.image.source="https://github.com/pqbench/pqbench" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"

COPY --from=build /src/target/release/pqbench /usr/local/bin/pqbench
COPY LICENSE-MIT LICENSE-APACHE /usr/share/licenses/pqbench/
WORKDIR /data
USER 65532:65532
ENTRYPOINT ["pqbench"]
CMD ["--help"]
