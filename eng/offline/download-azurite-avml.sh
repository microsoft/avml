#!/bin/bash
set -e

usage() {
    echo "Usage: $(basename "$0") <azurite-host> [port] [destination]"
}

# Print usage when the first argument asks for help.
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

# Require the host argument and reject unexpected extra arguments.
if (( $# < 1 || $# > 3 )); then
    usage >&2
    exit 1
fi

HOST=$1
PORT=${2:-10000}
DESTINATION=${3:-./avml-blobs}

# Azurite's known public credentials for development
ACCOUNT=devstoreaccount1
KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='

CONTAINER=avml
ENDPOINT="http://${HOST}:${PORT}/${ACCOUNT}"
CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=${ACCOUNT};AccountKey=${KEY};BlobEndpoint=${ENDPOINT};"

# List the blobs
az storage blob list \
    -c "${CONTAINER}" \
    --connection-string "${CONNECTION_STRING}" \
    --query '[].name' \
    -o tsv

mkdir -p "${DESTINATION}"
az storage blob download-batch \
    -s "${CONTAINER}" \
    -d "${DESTINATION}" \
    --connection-string "${CONNECTION_STRING}"

# Download one blob instead:
# 
# BLOB=snapshot-20260813T180000Z.lime
# az storage blob download \
#     -c "${CONTAINER}" \
#     -n "${BLOB}" \
#     -f "${DESTINATION}/${BLOB}" \
#     --connection-string "${CONNECTION_STRING}"
