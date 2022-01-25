#!/usr/bin/bash

set -exu


DEBIAN_FRONTEND=noninteractive sudo apt-get install musl-dev musl-tools musl

rustup component add rustfmt
rustup target add x86_64-unknown-linux-musl

cargo fmt -- --check
cargo clippy -- -D clippy::pedantic -A clippy::missing_errors_doc
cargo build --release --no-default-features --target x86_64-unknown-linux-musl --locked
cp target/x86_64-unknown-linux-musl/release/avml target/x86_64-unknown-linux-musl/release/avml-minimal
cargo build --release --target x86_64-unknown-linux-musl --locked
strip target/x86_64-unknown-linux-musl/release/avml
strip target/x86_64-unknown-linux-musl/release/avml-minimal
strip target/x86_64-unknown-linux-musl/release/avml-convert
