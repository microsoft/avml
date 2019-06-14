#!/bin/bash

set -ex

rustup update
cargo update
cargo test --target x86_64-unknown-linux-musl --release
cargo build --target x86_64-unknown-linux-musl --release --locked
cargo build --target x86_64-unknown-linux-musl --release  --locked --no-default-features
./test/run.sh
cargo package
