#!/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -ex

CONFIG=/tmp/test-config.json.`date '+%Y-%m-%d-%H-%M-%S'`.$$
TOOL_URL=${1}
SKU=${2:-RedHat:RHEL:8:8.0.2019050711}
SIZE=${3:-Standard_B1ls}
REGION=eastus
GROUP=vm-capture-test-`date '+%Y-%m-%d-%H-%M-%S'`-$$
VM=$(uuidgen)

function cleanup {
    rm -f ${CONFIG}
    az group delete -y --no-wait --name ${GROUP} || echo already removed
}
trap cleanup EXIT

echo -n '{"commandToExecute": "./avml --compress /tmp/image.lime", "fileUris": ["' > ${CONFIG}
echo -n ${TOOL_URL} >> ${CONFIG}
echo  '"]}' >> ${CONFIG}

az group create -l ${REGION} -n ${GROUP}
IP=$( az vm create -g ${GROUP} --size ${SIZE} -n ${VM} --image ${SKU} --query publicIpAddress -o tsv )
az vm extension set -g ${GROUP} --vm-name ${VM} --publisher Microsoft.Azure.Extensions -n customScript --settings ${CONFIG}
ssh-keygen -R ${IP} || echo no existing host key
scp -oStrictHostKeyChecking=no ${IP}:/tmp/image.lime ./${SKU}.lime
