#!/usr/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -uvex -o pipefail

DEBIAN_FRONTEND=noninteractive sudo apt-get install musl-dev musl-tools musl

rustup component add rustfmt
rustup target add x86_64-unknown-linux-musl
rustup update stable
cargo install typos-cli cargo-semver-checks
