#!/usr/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -uvex -o pipefail

cd $(dirname ${BASH_SOURCE[0]})/../

ARCH=$(uname -m)

cargo test --release --target ${ARCH}-unknown-linux-musl --locked --all-targets --all-features
for FEATURE in $(cargo metadata --locked --format-version 1 | jq '.packages | [.[] | select(.name=="avml")][0].features | keys | @tsv' -r); do
    cargo check --release --target ${ARCH}-unknown-linux-musl --locked --no-default-features --features ${FEATURE} --features native-tls
done
cargo build --release --no-default-features --target ${ARCH}-unknown-linux-musl --locked
cp target/${ARCH}-unknown-linux-musl/release/avml target/${ARCH}-unknown-linux-musl/release/avml-minimal
cargo build --release --target ${ARCH}-unknown-linux-musl --locked
cargo build --release --target ${ARCH}-unknown-linux-musl --locked --bin avml-upload --features "put blobstore status"
strip target/${ARCH}-unknown-linux-musl/release/avml
strip target/${ARCH}-unknown-linux-musl/release/avml-minimal
strip target/${ARCH}-unknown-linux-musl/release/avml-convert
strip target/${ARCH}-unknown-linux-musl/release/avml-upload
