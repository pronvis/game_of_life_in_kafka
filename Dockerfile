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
    cargo build --release --bin life_cell && \
    cp ./target/release/life_cell /bin/life_cell

# Run
FROM debian:trixie-slim

RUN useradd app

COPY --from=cargo-build /bin/life_cell /bin/

USER app

CMD ["/bin/life_cell"]
