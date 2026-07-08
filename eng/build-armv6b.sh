#!/usr/bin/bash
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
#

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../"

VERBOSE="${VERBOSE:-0}"

TARGET_NAME="${TARGET_NAME:-armv5te-unknown-linux-musleabi}"
TARGET="${TARGET:-eng/targets/${TARGET_NAME}.json}"
BUILD_OUTPUT_DIR="${BUILD_OUTPUT_DIR:-target/${TARGET_NAME}/release}"
ARTIFACT_DIR="${ARTIFACT_DIR:-target/armeb-unknown-linux-musleabi/release}"
CARGO_TOOLCHAIN="${CARGO_TOOLCHAIN:-nightly}"
CROSS_COMPILE="${CROSS_COMPILE:-armeb-linux-musleabi-}"
MUSL_TOOLCHAIN_DIR="${MUSL_TOOLCHAIN_DIR:-${HOME}/.cache/avml/armeb-linux-musleabi-cross}"
MUSL_CROSS_MAKE_REV="${MUSL_CROSS_MAKE_REV:-227df8b99103f9c59f6570babf892978e293082f}"
MUSL_CROSS_MAKE_URL="${MUSL_CROSS_MAKE_URL:-https://github.com/richfelker/musl-cross-make/archive/${MUSL_CROSS_MAKE_REV}.tar.gz}"
MUSL_CROSS_MAKE_SHA256="${MUSL_CROSS_MAKE_SHA256:-bb3fc7851088e1e5e1274ee56a0ab6ae176043d160fdf0b71027934b091f208a}"
MUSL_CROSS_MAKE_ARCHIVE="${MUSL_CROSS_MAKE_ARCHIVE:-}"
MUSL_CROSS_MAKE_DIR="${MUSL_CROSS_MAKE_DIR:-${HOME}/.cache/avml/musl-cross-make-${MUSL_CROSS_MAKE_REV}}"
BUILD_TOOLS_DIR="${BUILD_TOOLS_DIR:-target/armv6b-build-tools}"
if [[ "$BUILD_TOOLS_DIR" != /* ]]; then
    BUILD_TOOLS_DIR="$(pwd)/${BUILD_TOOLS_DIR}"
fi
BUILD_LOG="${BUILD_LOG:-${BUILD_TOOLS_DIR}/build.log}"
TARGET_ENV="${TARGET_NAME//-/_}"
TARGET_ENV="${TARGET_ENV^^}"
MUSL_CROSS_MAKE_DL_CMD="curl --fail --show-error --location --continue-at - --output"

mkdir -p "$(dirname "$BUILD_LOG")"
: > "$BUILD_LOG"
exec 3>&2

on_exit() {
    local status=$?

    if [[ "$status" -ne 0 ]]; then
        echo "==> Build failed; full log follows: ${BUILD_LOG}" >&3
        cat "$BUILD_LOG" >&3
    fi
}
trap on_exit EXIT
exec >"$BUILD_LOG" 2>&1

if [[ "$VERBOSE" == "1" ]]; then
    set -x
fi

log() {
    echo "==> $*" >&3
    echo "==> $*" >&2
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
    command -v python3 >/dev/null 2>&1 || packages+=("python3")
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

ensure_rust_toolchain() {
    require_command rustup
    log "Ensuring Rust toolchain ${CARGO_TOOLCHAIN} is installed"
    rustup toolchain install "$CARGO_TOOLCHAIN" --profile minimal --component rust-src
}

verify_sha256() {
    local archive="$1"
    local expected_sha256="$2"

    if ! echo "${expected_sha256}  ${archive}" | sha256sum --check --status; then
        echo "${archive}: SHA-256 verification failed; expected ${expected_sha256}" >&2
        exit 1
    fi
}

download_verified() {
    local url="$1"
    local archive="$2"
    local expected_sha256="$3"

    log "Downloading musl-cross-make"
    curl --fail --show-error --location --retry 5 --retry-delay 10 \
        --output "$archive" "$url"
    verify_sha256 "$archive" "$expected_sha256"
}

ensure_source_musl_toolchain() {
    local archive="${BUILD_TOOLS_DIR}/musl-cross-make-${MUSL_CROSS_MAKE_REV}.tar.gz"

    mkdir -p "$BUILD_TOOLS_DIR" "$MUSL_CROSS_MAKE_DIR"
    if [[ -n "$MUSL_CROSS_MAKE_ARCHIVE" ]]; then
        archive="$MUSL_CROSS_MAKE_ARCHIVE"
        verify_sha256 "$archive" "$MUSL_CROSS_MAKE_SHA256"
    elif [[ ! -f "$archive" ]]; then
        download_verified "$MUSL_CROSS_MAKE_URL" "$archive" "$MUSL_CROSS_MAKE_SHA256"
    else
        verify_sha256 "$archive" "$MUSL_CROSS_MAKE_SHA256"
    fi

    if [[ ! -f "${MUSL_CROSS_MAKE_DIR}/Makefile" ]]; then
        log "Extracting musl-cross-make"
        rm -rf "$MUSL_CROSS_MAKE_DIR"
        mkdir -p "$MUSL_CROSS_MAKE_DIR"
        tar --extract --gzip --directory "$MUSL_CROSS_MAKE_DIR" --strip-components=1 \
            --file "$archive"
    fi

    cat > "${MUSL_CROSS_MAKE_DIR}/config.mak" <<EOF
TARGET = armeb-linux-musleabi
OUTPUT = ${MUSL_TOOLCHAIN_DIR}
DL_CMD = ${MUSL_CROSS_MAKE_DL_CMD}
COMMON_CONFIG += CFLAGS="-g0 -Os" CXXFLAGS="-g0 -Os" LDFLAGS="-s"
COMMON_CONFIG += --disable-nls --with-debug-prefix-map=\$(CURDIR)=
GCC_CONFIG += --with-arch=armv5te --with-float=soft
GCC_CONFIG += --disable-libquadmath --disable-decimal-float --disable-libitm
GCC_CONFIG += --disable-fixed-point --disable-lto --enable-languages=c,c++
EOF

    log "Building musl cross toolchain"
    make -C "$MUSL_CROSS_MAKE_DIR"
    make -C "$MUSL_CROSS_MAKE_DIR" install
}

ensure_musl_toolchain() {
    if command -v "${CROSS_COMPILE}gcc" >/dev/null 2>&1; then
        return
    fi

    if [[ ! -x "${MUSL_TOOLCHAIN_DIR}/bin/armeb-linux-musleabi-gcc" ]]; then
        ensure_source_musl_toolchain
    fi

    CROSS_COMPILE="${MUSL_TOOLCHAIN_DIR}/bin/armeb-linux-musleabi-"
}

prepare_static_musl_toolchain() {
    local musl_lib_dir="${MUSL_TOOLCHAIN_DIR}/armeb-linux-musleabi/lib"
    local gcc_lib_dir

    gcc_lib_dir="$(dirname "$("${CROSS_COMPILE}gcc" -print-libgcc-file-name)")"

    if [[ -e "${musl_lib_dir}/libgcc_s.so.1" && ! -e "${musl_lib_dir}/libgcc_s.so.1.bak" ]]; then
        mv "${musl_lib_dir}/libgcc_s.so.1" "${musl_lib_dir}/libgcc_s.so.1.bak"
    fi
    if [[ -e "${musl_lib_dir}/libgcc_s.so" && ! -e "${musl_lib_dir}/libgcc_s.so.bak" ]]; then
        mv "${musl_lib_dir}/libgcc_s.so" "${musl_lib_dir}/libgcc_s.so.bak"
    fi
    if [[ ! -e "${musl_lib_dir}/libgcc_s.a" ]]; then
        ln -s "${gcc_lib_dir}/libgcc.a" "${musl_lib_dir}/libgcc_s.a"
    fi

    if [[ -e "${musl_lib_dir}/libc.so" && ! -e "${musl_lib_dir}/libc.so.bak" ]]; then
        mv "${musl_lib_dir}/libc.so" "${musl_lib_dir}/libc.so.bak"
    fi
    if [[ ! -e "${musl_lib_dir}/libc.so" ]]; then
        ln -s "${musl_lib_dir}/libc.a" "${musl_lib_dir}/libc.so"
    fi
}

build_musl_compat() {
    mkdir -p "$BUILD_TOOLS_DIR"

    cat > "${BUILD_TOOLS_DIR}/musl_compat.c" <<'EOF'
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/statfs.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/sendfile.h>
#include <stdarg.h>

int open64(const char *path, int flags, ...) {
    va_list ap;
    va_start(ap, flags);
    mode_t mode = va_arg(ap, mode_t);
    va_end(ap);
    return open(path, flags, mode);
}

int openat64(int dirfd, const char *path, int flags, ...) {
    va_list ap;
    va_start(ap, flags);
    mode_t mode = va_arg(ap, mode_t);
    va_end(ap);
    return openat(dirfd, path, flags, mode);
}

void *mmap64(void *addr, size_t len, int prot, int flags, int fd, off_t offset) {
    return mmap(addr, len, prot, flags, fd, offset);
}

off_t lseek64(int fd, off_t offset, int whence) {
    return lseek(fd, offset, whence);
}

int statfs64(const char *path, struct statfs *buf) {
    return statfs(path, buf);
}

ssize_t sendfile64(int out, int in, off_t *off, size_t len) {
    return sendfile(out, in, off, len);
}

int stat64(const char *path, struct stat *buf) {
    return stat(path, buf);
}

int fstat64(int fd, struct stat *buf) {
    return fstat(fd, buf);
}

int lstat64(const char *path, struct stat *buf) {
    return lstat(path, buf);
}
EOF

    "${CROSS_COMPILE}gcc" -c "${BUILD_TOOLS_DIR}/musl_compat.c" -o "${BUILD_TOOLS_DIR}/musl_compat.o" -Os
}

write_linker_wrapper() {
    local gcc_lib_dir
    local libgcc_eh

    gcc_lib_dir="$(dirname "$("${CROSS_COMPILE}gcc" -print-libgcc-file-name)")"
    libgcc_eh="${gcc_lib_dir}/libgcc_eh.a"

    cat > "${BUILD_TOOLS_DIR}/armeb-ld-wrapper.sh" <<EOF
#!/usr/bin/bash
set -euo pipefail

compat_object="${BUILD_TOOLS_DIR}/musl_compat.o"
libgcc_eh="${libgcc_eh}"
args=()
compile_only=0
for arg in "\$@"; do
    case "\$arg" in
        "\$compat_object") ;;
        -c|-S|-E) compile_only=1; args+=("\$arg") ;;
        -fuse-ld=lld) ;;
        -B*/gcc-ld) ;;
        -Wl,-Bdynamic) args+=("-Wl,-Bstatic") ;;
        -Wl,--as-needed) ;;
        *) args+=("\$arg") ;;
    esac
done

if [[ "\$compile_only" -eq 1 ]]; then
    exec "${CROSS_COMPILE}gcc" "\${args[@]}"
fi

if [[ -f "\$libgcc_eh" ]]; then
    exec "${CROSS_COMPILE}gcc" "\$compat_object" "\${args[@]}" "\$libgcc_eh"
fi

exec "${CROSS_COMPILE}gcc" "\$compat_object" "\${args[@]}"
EOF
    chmod +x "${BUILD_TOOLS_DIR}/armeb-ld-wrapper.sh"
}

LEGACY_ARMV6B_RUSTFLAGS=(
    "-C" "relocation-model=static"
    "-C" "link-arg=-static"
    "-C" "link-arg=-no-pie"
)

if [[ -n "${RUSTFLAGS:-}" ]]; then
    export RUSTFLAGS="${RUSTFLAGS} ${LEGACY_ARMV6B_RUSTFLAGS[*]}"
else
    export RUSTFLAGS="${LEGACY_ARMV6B_RUSTFLAGS[*]}"
fi

CARGO=(cargo "+${CARGO_TOOLCHAIN}")
BUILD_STD_ARGS=()
if [[ "${BUILD_STD:-1}" != "0" ]]; then
    BUILD_STD_ARGS=(
        "-Z" "build-std=std,panic_abort"
        "-Z" "json-target-spec"
    )
fi

patch_arm_eabi4() {
    local binary="$1"

    python3 - "$binary" <<'PY'
import struct
import sys
from pathlib import Path

path = Path(sys.argv[1])
data = bytearray(path.read_bytes())
if data[:4] != b"\x7fELF" or data[4] != 1:
    raise SystemExit(f"{path}: expected a 32-bit ELF file")

endian = ">" if data[5] == 2 else "<"
flags_offset = 36
(flags,) = struct.unpack_from(f"{endian}I", data, flags_offset)

# Legacy 2.6.18 ARM EABI4 kernels use the ELF flags while configuring
# syscall dispatch. Keep architecture flags below EF_ARM_BE8, but force
# EABI4 and clear EF_ARM_BE8 so BE32 kernels do not decode BE8 opcodes.
flags = (flags & 0x007f_ffff) | 0x0400_0000
struct.pack_into(f"{endian}I", data, flags_offset, flags)
path.write_bytes(data)
PY
}

verify_legacy_armv6b() {
    local binary="$1"

    "${READELF}" -h "$binary" | grep -q "Data:.*big endian"
    "${READELF}" -h "$binary" | grep -q "Type:.*EXEC"
    "${READELF}" -h "$binary" | grep -q "Flags:.*Version4 EABI"
    ! "${READELF}" -h "$binary" | grep -q "BE8"
    ! "${READELF}" -l "$binary" | grep -q "INTERP"
    "${READELF}" -A "$binary" | grep -q "Tag_CPU_arch: v5TE"
}

ensure_host_tools
ensure_rust_toolchain
ensure_musl_toolchain
prepare_static_musl_toolchain
build_musl_compat
write_linker_wrapper

LINKER="${LINKER:-${BUILD_TOOLS_DIR}/armeb-ld-wrapper.sh}"
READELF="${READELF:-${CROSS_COMPILE}readelf}"
STRIP="${STRIP:-${CROSS_COMPILE}strip}"
OBJDUMP="${OBJDUMP:-${CROSS_COMPILE}objdump}"

export "CARGO_TARGET_${TARGET_ENV}_LINKER=${LINKER}"
export "CC_${TARGET_NAME//-/_}=${LINKER}"
export "AR_${TARGET_NAME//-/_}=${CROSS_COMPILE}ar"
export "RANLIB_${TARGET_NAME//-/_}=${CROSS_COMPILE}ranlib"
export OPENSSL_STATIC="${OPENSSL_STATIC:-1}"
export RUSTFLAGS="${RUSTFLAGS} -C linker=${LINKER}"

log "Building minimal avml for ${TARGET_NAME}"
"${CARGO[@]}" build "${BUILD_STD_ARGS[@]}" --release --no-default-features --target "$TARGET" --locked
mkdir -p "$ARTIFACT_DIR"
cp "${BUILD_OUTPUT_DIR}/avml" "${ARTIFACT_DIR}/avml-minimal"
log "Building default avml for ${TARGET_NAME}"
"${CARGO[@]}" build "${BUILD_STD_ARGS[@]}" --release --target "$TARGET" --locked
cp "${BUILD_OUTPUT_DIR}/avml" "${ARTIFACT_DIR}/avml"

for binary in "${ARTIFACT_DIR}/avml" "${ARTIFACT_DIR}/avml-minimal"; do
    log "Verifying ${binary}"
    "${STRIP}" "$binary"
    patch_arm_eabi4 "$binary"
    verify_legacy_armv6b "$binary"
    ! "${OBJDUMP}" -d "$binary" | grep -qE '\bmovw\b|\bmovt\b'
done
