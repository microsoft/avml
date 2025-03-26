#!/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -ueo pipefail

cd $(dirname ${BASH_SOURCE[0]})/../

IMAGES_TXT=${1:-eng/images.txt}
FILE=${2:-target/x86_64-unknown-linux-musl/release/avml}

function cleanup {
    for group in $(az group list --query '[].name' -o tsv |grep vm-capture-test); do
        az group delete -y --no-wait --name $group || echo delete failed...
    done
}
trap cleanup EXIT

xargs -P 20 -a ${IMAGES_TXT} -I test-image-name eng/test-azure-image.sh ${FILE} test-image-name

eng/test-conversion.sh
