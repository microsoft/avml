#!/usr/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -uvex -o pipefail

cd $(dirname ${BASH_SOURCE[0]})/../

cargo test --release --target x86_64-unknown-linux-musl --locked --all-targets --all-features
for FEATURE in $(cargo metadata --locked --format-version 1 | jq '.packages | [.[] | select(.name=="avml")][0].features | keys | @tsv' -r); do
    cargo check --release --target x86_64-unknown-linux-musl --locked --no-default-features --features ${FEATURE} --features native-tls
done
cargo build --release --no-default-features --target x86_64-unknown-linux-musl --locked
cp target/x86_64-unknown-linux-musl/release/avml target/x86_64-unknown-linux-musl/release/avml-minimal
cargo build --release --target x86_64-unknown-linux-musl --locked
cargo build --release --target x86_64-unknown-linux-musl --locked --bin avml-upload --features "put blobstore status"
strip target/x86_64-unknown-linux-musl/release/avml
strip target/x86_64-unknown-linux-musl/release/avml-minimal
strip target/x86_64-unknown-linux-musl/release/avml-convert
strip target/x86_64-unknown-linux-musl/release/avml-upload
