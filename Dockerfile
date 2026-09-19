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

# Choose the CPU baseline and build the release binary; see scripts/docker_build.sh.
# (x86 codecs use -march, ARM uses -mcpu; the script overrides
# .cargo/config.toml's -march=native while retaining the other codec flags.)
COPY scripts/docker_build.sh /src/scripts/docker_build.sh
RUN sh /src/scripts/docker_build.sh

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
