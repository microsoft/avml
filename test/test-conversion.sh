#!/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -e

IMAGES_TXT=${1:-test/images.txt}
CONVERT=${2:-target/x86_64-unknown-linux-musl/release/avml-convert}

for SKU in $(cat ${IMAGES_TXT}); do
    ${CONVERT} ${SKU}.lime ${SKU}.uncompressed.lime
    ${CONVERT} --format lime_compressed ${SKU}.uncompressed.lime ${SKU}.recompressed.lime
    ${CONVERT} --format lime_compressed ${SKU}.lime ${SKU}.compressed.lime
    diff -q ${SKU}.lime ${SKU}.compressed.lime
    diff -q ${SKU}.lime ${SKU}.recompressed.lime
done