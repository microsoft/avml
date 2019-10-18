#!/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -e

LOG=/tmp/.log_$$.txt

IMAGES_TXT=${1:-test/images.txt}
FILE=${2:-target/x86_64-unknown-linux-musl/release/avml}
GROUP=vm-capture-test-`date '+%Y-%m-%d-%H-%M-%S'`-$$
REGION=eastus
STORAGE=$(dd if=/dev/urandom | tr -dc 'a-z0-9' | fold -w 24 | head -n 1)
DST_PATH=$(dd if=/dev/urandom | tr -dc 'a-z0-9' | fold -w 24 | head -n 1)/avml
CONTAINER=tools
URL=https://${STORAGE}.blob.core.windows.net/${CONTAINER}/${DST_PATH}

LOG=/tmp/$(dd if=/dev/urandom | tr -dc 'a-z0-9' | fold -w 24 | head -n 1).log
function fail {
    echo ERROR
    cat "${LOG}"
    exit 1
}

function quiet {
    rm -f ${LOG}
    $* 2>> ${LOG} >> ${LOG} && rm ${LOG} || fail
}

function cleanup {
    for group in $(az group list --query '[].name' -o tsv |grep vm-capture-test); do
        az group delete -y --no-wait --name $group || echo delete failed...
    done
    rm -f ${LOG}
}
trap cleanup EXIT

quiet az group create -l ${REGION} -n ${GROUP}
quiet az storage account create --location ${REGION} --resource-group ${GROUP} --name ${STORAGE}
quiet az storage container create --account-name ${STORAGE} --name ${CONTAINER}
quiet az storage container set-permission --account-name ${STORAGE} -n ${CONTAINER} --public-access blob
quiet az storage blob upload --account-name ${STORAGE} --container ${CONTAINER} --name ${DST_PATH} --file ${FILE}
xargs -P 20 -a ${IMAGES_TXT} -I test-image-name test/test-azure-image.sh ${URL} test-image-name
