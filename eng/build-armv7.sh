#!/usr/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../"

source eng/musl-cross-common.sh

TARGET_NAME="${TARGET_NAME:-armv7-unknown-linux-musleabihf}"
CROSS_COMPILE="${CROSS_COMPILE:-arm-linux-musleabihf-}"
MUSL_TOOLCHAIN_DIR="${MUSL_TOOLCHAIN_DIR:-${HOME}/.cache/avml/arm-linux-musleabihf-cross}"
MUSL_CROSS_MAKE_DIR="${MUSL_CROSS_MAKE_DIR:-${HOME}/.cache/avml/musl-cross-make-armv7-${MUSL_CROSS_MAKE_REV}}"
BUILD_TOOLS_DIR="${BUILD_TOOLS_DIR:-target/armv7-build-tools}"
TARGET_ENV="${TARGET_NAME//-/_}"
TARGET_ENV="${TARGET_ENV^^}"

log() {
    echo "==> $*"
}

require_command() {
    local command_name="$1"

    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "Missing required command: $command_name" >&2
        exit 1
    fi
}

install_apt_packages() {
    local missing_packages=("$@")

    if [[ "${#missing_packages[@]}" -eq 0 ]]; then
        return
    fi

    require_command sudo
    log "Installing missing host packages: ${missing_packages[*]}"
    sudo apt-get update
    sudo apt-get install --no-install-recommends --yes "${missing_packages[@]}"
}

ensure_host_tools() {
    local packages=()

    command -v curl >/dev/null 2>&1 || packages+=("curl")
    command -v make >/dev/null 2>&1 || packages+=("make")
    command -v perl >/dev/null 2>&1 || packages+=("perl")
    command -v sha256sum >/dev/null 2>&1 || packages+=("coreutils")
    command -v gcc >/dev/null 2>&1 || packages+=("build-essential")
    command -v bzip2 >/dev/null 2>&1 || packages+=("bzip2")
    command -v xz >/dev/null 2>&1 || packages+=("xz-utils")

    if [[ "${#packages[@]}" -gt 0 ]]; then
        if command -v apt-get >/dev/null 2>&1; then
            install_apt_packages "${packages[@]}"
        else
            echo "Install these host tools before running $0: ${packages[*]}" >&2
            exit 1
        fi
    fi
}

ensure_musl_toolchain() {
    if command -v "${CROSS_COMPILE}gcc" >/dev/null 2>&1; then
        return
    fi
    if [[ -x "${MUSL_TOOLCHAIN_DIR}/bin/arm-linux-musleabihf-gcc" ]]; then
        CROSS_COMPILE="${MUSL_TOOLCHAIN_DIR}/bin/arm-linux-musleabihf-"
        return
    fi

    avml_ensure_musl_cross_make_source "$BUILD_TOOLS_DIR" "$MUSL_CROSS_MAKE_DIR"

    cat > "${MUSL_CROSS_MAKE_DIR}/config.mak" <<EOF
TARGET = arm-linux-musleabihf
OUTPUT = ${MUSL_TOOLCHAIN_DIR}
DL_CMD = ${MUSL_CROSS_MAKE_DL_CMD}
COMMON_CONFIG += CFLAGS="-g0 -Os" CXXFLAGS="-g0 -Os" LDFLAGS="-s"
COMMON_CONFIG += --disable-nls --with-debug-prefix-map=\$(CURDIR)=
GCC_CONFIG += --with-arch=armv7-a --with-fpu=vfpv3-d16 --with-float=hard
GCC_CONFIG += --disable-libquadmath --disable-decimal-float --disable-libitm
GCC_CONFIG += --disable-lto --enable-languages=c,c++
EOF

    avml_ensure_musl_source "$MUSL_CROSS_MAKE_DIR"
    log "Building ARMv7 musl cross toolchain"
    make -C "$MUSL_CROSS_MAKE_DIR"
    make -C "$MUSL_CROSS_MAKE_DIR" install
    CROSS_COMPILE="${MUSL_TOOLCHAIN_DIR}/bin/arm-linux-musleabihf-"
}

verify_binary() {
    local binary="$1"
    local readelf="${CROSS_COMPILE}readelf"

    "$readelf" -h "$binary" | grep -q "Class:.*ELF32"
    "$readelf" -h "$binary" | grep -q "Data:.*little endian"
    "$readelf" -h "$binary" | grep -q "Machine:.*ARM"
    "$readelf" -h "$binary" | grep -q "Flags:.*hard-float ABI"
    "$readelf" -A "$binary" | grep -q "Tag_CPU_arch: v7"
    ! "$readelf" -l "$binary" | grep -q "INTERP"
    ! "$readelf" -d "$binary" | grep -q "(NEEDED)"
}

ensure_host_tools
require_command rustup
log "Ensuring Rust target ${TARGET_NAME} is installed"
rustup +stable target add "$TARGET_NAME"
ensure_musl_toolchain

LINKER="${CROSS_COMPILE}gcc"
STRIP="${CROSS_COMPILE}strip"
TARGET_VARIABLE="${TARGET_NAME//-/_}"

export "CARGO_TARGET_${TARGET_ENV}_LINKER=${LINKER}"
export "CC_${TARGET_VARIABLE}=${LINKER}"
export "AR_${TARGET_VARIABLE}=${CROSS_COMPILE}ar"
export "RANLIB_${TARGET_VARIABLE}=${CROSS_COMPILE}ranlib"
export "CFLAGS_${TARGET_VARIABLE}=-march=armv7-a -mfpu=vfpv3-d16 -mfloat-abi=hard"
export OPENSSL_STATIC="${OPENSSL_STATIC:-1}"

log "Building minimal AVML for ${TARGET_NAME}"
cargo +stable build --release --no-default-features --target "$TARGET_NAME" --locked
cp "target/${TARGET_NAME}/release/avml" "target/${TARGET_NAME}/release/avml-minimal"

log "Building default AVML for ${TARGET_NAME}"
cargo +stable build --release --target "$TARGET_NAME" --locked

for binary in \
    "target/${TARGET_NAME}/release/avml" \
    "target/${TARGET_NAME}/release/avml-minimal"; do
    log "Verifying ${binary}"
    "$STRIP" "$binary"
    verify_binary "$binary"
done
