#!/usr/bin/bash

set -exu


DEBIAN_FRONTEND=noninteractive sudo apt-get install musl-dev musl-tools musl

rustup component add rustfmt
rustup target add x86_64-unknown-linux-musl
rustup update stable

cargo fmt -- --check
cargo build --release --no-default-features --target x86_64-unknown-linux-musl --locked
cp target/x86_64-unknown-linux-musl/release/avml target/x86_64-unknown-linux-musl/release/avml-minimal
cargo build --release --target x86_64-unknown-linux-musl --locked
cargo build --release --target x86_64-unknown-linux-musl --locked --bin avml-upload --features "put blobstore status"
cargo test --release --target x86_64-unknown-linux-musl --locked
cargo clippy --locked --all-targets --all-features -- -D warnings -D clippy::pedantic -A clippy::missing_errors_doc
strip target/x86_64-unknown-linux-musl/release/avml
strip target/x86_64-unknown-linux-musl/release/avml-minimal
strip target/x86_64-unknown-linux-musl/release/avml-convert
strip target/x86_64-unknown-linux-musl/release/avml-upload
