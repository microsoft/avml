#!/usr/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -uvex -o pipefail

cd $(dirname ${BASH_SOURCE[0]})/../

cargo fmt -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings -D clippy::pedantic -A clippy::missing_errors_doc

cargo install typos-cli
typos

cargo install cargo-semver-checks --locked
cargo semver-checks check-release