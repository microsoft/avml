#!/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -e

INSTANCE=$(uuidgen)
LOG=/tmp/avml-test-${INSTANCE}.log
CONFIG=/tmp/test-config.json.${INSTANCE}
GROUP=vm-capture-test-${INSTANCE}
VM=${INSTANCE}
REGION=eastus
TOOL_URL=${1}
SKU=${2:-RedHat:RHEL:8:8.0.2019050711}
SIZE=${3:-Standard_B1ls}

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
    rm -f ${CONFIG}
    az group delete -y --no-wait --name ${GROUP} || echo already removed
    rm -f ${LOG}
}
trap cleanup EXIT

echo -n '{"commandToExecute": "./avml --compress /mnt/image.lime", "fileUris": ["' > ${CONFIG}
echo -n ${TOOL_URL} >> ${CONFIG}
echo  '"]}' >> ${CONFIG}

echo testing ${SKU}
quiet az group create -l ${REGION} -n ${GROUP}
IP=$( az vm create -g ${GROUP} --size ${SIZE} -n ${VM} --image ${SKU} --query publicIpAddress -o tsv --public-ip-sku Standard )
quiet az vm extension set -g ${GROUP} --vm-name ${VM} --publisher Microsoft.Azure.Extensions -n customScript --settings ${CONFIG}
ssh-keygen -R ${IP} 2>/dev/null > /dev/null
quiet ssh -oStrictHostKeyChecking=no ${IP} sudo chmod a+r /mnt/image.lime
quiet scp -oStrictHostKeyChecking=no ${IP}:/mnt/image.lime ./${SKU}.lime
