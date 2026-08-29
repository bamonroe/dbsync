# Build and run dbsync without a host Rust toolchain.
# The Rust version is pinned here so the build is identical everywhere.

FROM rust:1.90-slim-bookworm AS builder

WORKDIR /build

# Cache the dependency build: copy manifests first and compile a stub, so a
# source-only change does not re-download and rebuild the whole dependency tree.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
    && echo 'fn main() {}' > src/main.rs \
    && echo '' > src/lib.rs \
    && cargo build --release \
    && rm -rf src

COPY src ./src
# Cargo skips rebuilding when mtimes look unchanged; touch to force it.
RUN touch src/main.rs src/lib.rs && cargo build --release


FROM debian:bookworm-slim AS runtime

# rustls is used for TLS, but the CA bundle still has to come from the image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Run unprivileged; the sync directory is bind-mounted in with matching ownership.
RUN useradd --create-home --uid 1000 dbsync
USER dbsync
WORKDIR /home/dbsync

COPY --from=builder /build/target/release/dbsync /usr/local/bin/dbsync

ENTRYPOINT ["dbsync"]
CMD ["run"]
