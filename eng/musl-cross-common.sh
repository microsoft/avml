#!/usr/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

: "${MUSL_CROSS_MAKE_REV:=227df8b99103f9c59f6570babf892978e293082f}"
: "${MUSL_CROSS_MAKE_URL:=https://github.com/richfelker/musl-cross-make/archive/${MUSL_CROSS_MAKE_REV}.tar.gz}"
: "${MUSL_CROSS_MAKE_SHA256:=bb3fc7851088e1e5e1274ee56a0ab6ae176043d160fdf0b71027934b091f208a}"
: "${MUSL_CROSS_MAKE_ARCHIVE:=}"
: "${MUSL_VERSION:=1.2.6}"
: "${MUSL_SOURCE_URL:=https://sources.buildroot.net/musl/musl-${MUSL_VERSION}.tar.gz}"
: "${MUSL_SOURCE_SHA256:=d585fd3b613c66151fc3249e8ed44f77020cb5e6c1e635a616d3f9f82460512a}"
: "${MUSL_CROSS_MAKE_DL_CMD:=curl --fail --show-error --location --continue-at - --retry 5 --retry-delay 10 --output}"

avml_verify_sha256() {
    local archive="$1"
    local expected_sha256="$2"

    if ! echo "${expected_sha256}  ${archive}" | sha256sum --check --status; then
        echo "${archive}: SHA-256 verification failed; expected ${expected_sha256}" >&2
        exit 1
    fi
}

avml_download_verified() {
    local name="$1"
    local url="$2"
    local archive="$3"
    local expected_sha256="$4"
    local temporary_archive="${archive}.download"

    log "Downloading ${name}"
    curl --fail --show-error --location --retry 5 --retry-delay 10 \
        --output "$temporary_archive" "$url"
    avml_verify_sha256 "$temporary_archive" "$expected_sha256"
    mv "$temporary_archive" "$archive"
}

avml_ensure_musl_cross_make_source() {
    local build_tools_dir="$1"
    local source_dir="$2"
    local archive="${build_tools_dir}/musl-cross-make-${MUSL_CROSS_MAKE_REV}.tar.gz"

    mkdir -p "$build_tools_dir" "$source_dir"
    if [[ -n "$MUSL_CROSS_MAKE_ARCHIVE" ]]; then
        archive="$MUSL_CROSS_MAKE_ARCHIVE"
        avml_verify_sha256 "$archive" "$MUSL_CROSS_MAKE_SHA256"
    elif [[ -f "$archive" ]]; then
        avml_verify_sha256 "$archive" "$MUSL_CROSS_MAKE_SHA256"
    else
        avml_download_verified \
            "musl-cross-make" \
            "$MUSL_CROSS_MAKE_URL" \
            "$archive" \
            "$MUSL_CROSS_MAKE_SHA256"
    fi

    if [[ ! -f "${source_dir}/Makefile" ]]; then
        log "Extracting musl-cross-make"
        rm -rf "$source_dir"
        mkdir -p "$source_dir"
        tar --extract --gzip --directory "$source_dir" --strip-components=1 \
            --file "$archive"
    fi
}

avml_ensure_musl_source() {
    local source_dir="${1}/sources"
    local archive="${source_dir}/musl-${MUSL_VERSION}.tar.gz"

    mkdir -p "$source_dir"
    if [[ -f "$archive" ]]; then
        avml_verify_sha256 "$archive" "$MUSL_SOURCE_SHA256"
        return
    fi

    avml_download_verified \
        "musl ${MUSL_VERSION}" \
        "$MUSL_SOURCE_URL" \
        "$archive" \
        "$MUSL_SOURCE_SHA256"
}
