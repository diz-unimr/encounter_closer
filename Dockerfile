# syntax=docker/dockerfile:1

# ---- gemeinsame Basis: Toolchain + Quellcode ----
FROM rust:1-bookworm AS base
# cmake/g++ werden von rdkafka (feature "cmake-build") für librdkafka benötigt
RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake build-essential \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml ./
COPY src ./src
COPY tests ./tests

# ---- Test-Image: kompiliert vorab, führt Unit- + Integrationstests aus ----
FROM base AS test
RUN cargo test --no-run --all-targets
CMD ["cargo", "test", "--all-targets", "--", "--include-ignored"]

# ---- Release-Build ----
FROM base AS builder
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release \
 && cp target/release/encounter-closer /usr/local/bin/encounter-closer

# ---- schlankes Laufzeit-Image ----
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/encounter-closer /usr/local/bin/encounter-closer
USER nobody
ENTRYPOINT ["encounter-closer"]