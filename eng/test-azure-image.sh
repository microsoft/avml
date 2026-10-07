#!/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -ueo pipefail

VM=$(uuidgen)
LOG=/tmp/avml-test-${VM}.log
ERR=/tmp/avml-test-${VM}.err
KEY=/tmp/avml-test-${VM}
GROUP=vm-capture-test-${VM}
REGION=eastus2
EXE=${1-target/x86_64-unknown-linux-musl/release/avml}
SKU=${2:-OpenLogic:CentOS:8_5:latest}
SIZE=${3:-Standard_B1ls}
SOURCE=${4:-}

function fail {
    echo ERROR
    if [ -f ${ERR} ]; then
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
    "$@" 2>> ${ERR} >> ${LOG} || fail
}

function cleanup {
    az group delete -y --no-wait --name ${GROUP} || echo already removed
    rm -f ${LOG}
    rm -f ${ERR}
    rm -f ${KEY}
    rm -f ${KEY}.pub
}
trap cleanup EXIT

echo testing ${SKU}
quiet az group create -l ${REGION} -n ${GROUP}
quiet ssh-keygen -q -t rsa -b 3072 -N '' -f ${KEY}
IP=$(az vm create -g ${GROUP} --size ${SIZE} -n ${VM} --image ${SKU} --admin-username avml --ssh-key-values ${KEY}.pub --public-ip-sku Standard --security-type Standard --patch-mode ImageDefault --query publicIpAddress -o tsv | tr -d '\r')
ssh-keygen -R ${IP} 2>/dev/null > /dev/null
rm -f ${ERR}
rm -f ${LOG}
READY=
for _ in $(seq 1 30); do
    if ssh -i ${KEY} -oConnectTimeout=5 -oStrictHostKeyChecking=no -oPubkeyAcceptedAlgorithms=+ssh-rsa -oHostKeyAlgorithms=+ssh-rsa avml@${IP} true 2>> ${ERR} >> ${LOG}; then
        READY=1
        break
    fi
    sleep 5
done
[ -n "${READY}" ] || fail
quiet scp -i ${KEY} -oStrictHostKeyChecking=no -oPubkeyAcceptedAlgorithms=+ssh-rsa -oHostKeyAlgorithms=+ssh-rsa ${EXE} avml@${IP}:./avml
quiet ssh -i ${KEY} -oStrictHostKeyChecking=no -oPubkeyAcceptedAlgorithms=+ssh-rsa -oHostKeyAlgorithms=+ssh-rsa avml@${IP} sudo chmod +x avml
ACQUIRE=(sudo ./avml acquire --compress)
if [ -n "${SOURCE}" ]; then
    ACQUIRE+=(--source "${SOURCE}")
fi
ACQUIRE+=(/mnt/image.lime)
quiet ssh -i ${KEY} -oStrictHostKeyChecking=no -oPubkeyAcceptedAlgorithms=+ssh-rsa -oHostKeyAlgorithms=+ssh-rsa avml@${IP} "${ACQUIRE[@]}"
quiet ssh -i ${KEY} -oStrictHostKeyChecking=no -oPubkeyAcceptedAlgorithms=+ssh-rsa -oHostKeyAlgorithms=+ssh-rsa avml@${IP} sudo chmod a+r /mnt/image.lime
quiet scp -i ${KEY} -oStrictHostKeyChecking=no -oPubkeyAcceptedAlgorithms=+ssh-rsa -oHostKeyAlgorithms=+ssh-rsa avml@${IP}:/mnt/image.lime ./${SKU}.lime
