#!/bin/bash

set -ex

rustup update
cargo update
cargo test --target x86_64-unknown-linux-musl --release
cargo build --target x86_64-unknown-linux-musl --release  --locked --no-default-features
cp target/x86_64-unknown-linux-musl/release/avml target/x86_64-unknown-linux-musl/release/avml-minimal
cargo build --target x86_64-unknown-linux-musl --release --locked
./eng/run.sh
cargo package --locked
