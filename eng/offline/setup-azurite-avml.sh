#!/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -euo pipefail

usage() {
    echo "Usage: $(basename "$0") <azurite-host> [port]"
}

# Print usage when the first argument asks for help.
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

# Require the host argument and reject unexpected extra arguments.
if (( $# < 1 || $# > 2 )); then
    usage >&2
    exit 1
fi

HOST=$1
PORT=${2:-10000}

# Azurite's known public credentials for development
ACCOUNT=devstoreaccount1
KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='

CONTAINER=avml
BLOB="snapshot-$(date -u '+%Y%m%dT%H%M%SZ').lime"
ENDPOINT="http://${HOST}:${PORT}/${ACCOUNT}"
CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=${ACCOUNT};AccountKey=${KEY};BlobEndpoint=${ENDPOINT};"

# Create the avml container in azurite
az storage container create \
    -n "${CONTAINER}" \
    --connection-string "${CONNECTION_STRING}" \
    -o none

# Generate the sas url to give to avml
SAS=$(
    az storage blob generate-sas \
        -c "${CONTAINER}" \
        -n "${BLOB}" \
        --permissions cw \
        --expiry "$(date -u -d '1 day' '+%Y-%m-%dT%H:%MZ')" \
        --connection-string "${CONNECTION_STRING}" \
        -o tsv
)

# Print the avml command to stream the snapshot to azurite
printf "sudo ./avml stream blob --compress '%s/%s/%s?%s'\n" \
    "${ENDPOINT}" "${CONTAINER}" "${BLOB}" "${SAS}"
