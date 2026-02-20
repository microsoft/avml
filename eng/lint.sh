#!/usr/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -uvex -o pipefail

cd $(dirname ${BASH_SOURCE[0]})/../

rustup +stable component add rustfmt
cargo fmt -- --check
cargo +stable clippy --locked --all-targets --all-features -- -D warnings -D clippy::pedantic -A clippy::missing_errors_doc

which typos || cargo install typos-cli
typos

which cargo-semver-checks || cargo install cargo-semver-checks --locked
cargo semver-checks check-release
