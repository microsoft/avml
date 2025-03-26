#!/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -ue -o pipefail

VM=$(uuidgen)
LOG=/tmp/avml-test-${VM}.log
ERR=/tmp/avml-test-${VM}.err
GROUP=vm-capture-test-${VM}
REGION=eastus2
EXE=${1-target/x86_64-unknown-linux-musl/release/avml}
SKU=${2:-OpenLogic:CentOS:8_5:latest}
SIZE=${3:-Standard_B1ls}

function fail {
    echo ERROR
    if [ -f ${ERR} ]; then
    ec
        cat "${ERR}"
    fi
    if [ -f ${LOG} ]; then
        cat "${LOG}"
    fi
    exit 1
}

function quiet {
    rm -f ${ERR}
    rm -f ${LOG}
    $* 2>> ${ERR} >> ${LOG} || fail
}

function cleanup {
    az group delete -y --no-wait --name ${GROUP} || echo already removed
    rm -f ${LOG}
    rm -f ${ERR}
}
trap cleanup EXIT

echo testing ${SKU}
quiet az group create -l ${REGION} -n ${GROUP}
IP=$(az vm create -g ${GROUP} --size ${SIZE} -n ${VM} --image ${SKU} --public-ip-sku Standard --security-type Standard --query publicIpAddress -o tsv)
ssh-keygen -R ${IP} 2>/dev/null > /dev/null
quiet scp -oStrictHostKeyChecking=no ${EXE} ${IP}:./avml
quiet ssh -oStrictHostKeyChecking=no ${IP} sudo chmod +x avml
quiet ssh -oStrictHostKeyChecking=no ${IP} sudo ./avml --compress /mnt/image.lime
quiet ssh -oStrictHostKeyChecking=no ${IP} sudo chmod a+r /mnt/image.lime
quiet scp -oStrictHostKeyChecking=no ${IP}:/mnt/image.lime ./${SKU}.lime
