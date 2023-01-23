#!/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -e

cd $(dirname ${BASH_SOURCE[0]})/../

IMAGES_TXT=${1:-eng/images.txt}
CONVERT=${2:-target/x86_64-unknown-linux-musl/release/avml-convert}

for SKU in $(cat ${IMAGES_TXT}); do
    echo testing conversion ${SKU}.lime
    ${CONVERT} ${SKU}.lime ${SKU}.uncompressed.lime
    ${CONVERT} --source-format lime --format lime_compressed ${SKU}.uncompressed.lime ${SKU}.recompressed.lime
    diff -q ${SKU}.lime ${SKU}.recompressed.lime
    rm ${SKU}.recompressed.lime ${SKU}.uncompressed.lime
done
