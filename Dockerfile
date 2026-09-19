# syntax=docker/dockerfile:1
FROM rust:1.90-alpine3.22 AS build

RUN apk add --no-cache build-base pkgconf zlib-dev zlib-static

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo/ .cargo/
COPY crates/ crates/

# Published binaries must run on CPUs other than the build host. Override
# .cargo/config.toml's -march=native while retaining the other codec flags.
ENV CXXFLAGS="-O3 -DNDEBUG -fPIE -fomit-frame-pointer -fstrict-aliasing -ffast-math -DHAVE_BUILTIN_CTZ=1"
RUN cargo build --locked --release --package pqbench-cli

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
