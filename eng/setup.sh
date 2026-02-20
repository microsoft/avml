#!/usr/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -uvex -o pipefail

DEBIAN_FRONTEND=noninteractive sudo apt-get install musl-dev musl-tools musl jq

rustup update stable
rustup +stable target add $(uname -m)-unknown-linux-musl
