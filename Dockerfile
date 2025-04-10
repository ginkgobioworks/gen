FROM rust:bullseye

RUN apt-get update && apt-get install -y libclang-dev

WORKDIR gen

COPY Cargo.lock Cargo.toml LICENSE /gen
COPY migrations migrations
COPY src src

RUN cargo build --release
