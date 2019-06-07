#!/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -ex

IMAGES_TXT=${1:-test/images.txt}
FILE=${2:-target/x86_64-unknown-linux-musl/release/avml}
GROUP=vm-capture-test-`date '+%Y-%m-%d-%H-%M-%S'`-$$
REGION=eastus
STORAGE=$(dd if=/dev/random | tr -dc 'a-z0-9' | fold -w 24 | head -n 1)
DST_PATH=$(dd if=/dev/random | tr -dc 'a-z0-9' | fold -w 24 | head -n 1)/avml
CONTAINER=tools
URL=https://${STORAGE}.blob.core.windows.net/${CONTAINER}/${DST_PATH}

function cleanup {
    for group in $(az group list --query '[].name' -o tsv |grep vm-capture-test); do
        az group delete -y --no-wait --name $group || echo delete failed...
    done
}
trap cleanup EXIT

az group create -l ${REGION} -n ${GROUP} --query "properties.provisioningState" -o tsv
az storage account create --location ${REGION} --resource-group ${GROUP} --name ${STORAGE}
az storage container create --account-name ${STORAGE} --name ${CONTAINER}
az storage container set-permission --account-name ${STORAGE} -n ${CONTAINER} --public-access blob
az storage blob upload --account-name ${STORAGE} --container ${CONTAINER} --name ${DST_PATH} --file ${FILE}
xargs -P 20 -a ${IMAGES_TXT} -I test-image-name test/test-azure-image.sh ${URL} test-image-name
