# Single image; each service runs a different subcommand (spec §14).
# Builder and runtime must share a Debian release (glibc compatibility).
FROM rust:1.96-slim-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY .sqlx ./.sqlx
COPY migrations ./migrations
COPY src ./src
COPY templates ./templates
COPY static ./static
ENV SQLX_OFFLINE=true
RUN cargo build --release

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
# supercronic: containers-friendly cron for the worker service.
ARG SUPERCRONIC_VERSION=v0.2.34
ADD https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64 /usr/local/bin/supercronic
RUN chmod +x /usr/local/bin/supercronic
COPY --from=builder /build/target/release/arkivo /usr/local/bin/arkivo
COPY docker/crontab /etc/arkivo/crontab
RUN useradd --system --create-home arkivo
USER arkivo
ENTRYPOINT ["/usr/local/bin/arkivo"]
