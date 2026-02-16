# syntax=docker/dockerfile

# Build
FROM rust:slim AS cargo-build
# ARG APP_NAME
WORKDIR /code/

# Build the application.
RUN --mount=type=bind,source=src,target=src \
    --mount=type=bind,source=Cargo.toml,target=Cargo.toml \
    --mount=type=bind,source=Cargo.lock,target=Cargo.lock \
    --mount=type=cache,target=/code/target/ \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo build --release && \
    cp ./target/release/kafka_client /bin/kafka_client

# Run
FROM debian:bookworm-slim

RUN addgroup app
RUN useradd -g app app

COPY --from=cargo-build /bin/kafka_client /bin/

USER app

CMD ["/bin/kafka_client"]
