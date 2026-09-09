#!/usr/bin/env python3
#
# Licensed under the MIT License.
#

"""
Build Volatility 3 ISF (Intermediate Symbol File) for Linux kernels
on Fedora and Red Hat-based distributions (RHEL, CentOS, Rocky Linux, AlmaLinux).

Given a `uname -r`, `uname -a`, or `/proc/version` banner string:
1. Parses the kernel version, release, architecture, and distribution.
2. Locates and downloads the corresponding `kernel-debuginfo` (for vmlinux)
   and `kernel-core` / `kernel` (for System.map) RPM packages.
3. Extracts `vmlinux` and `System.map` without needing root privileges.
4. Generates the Volatility 3 `linux-kernel.json` ISF table via `dwarf2json`.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
import xmlrpc.client
from pathlib import Path
from typing import Dict, List, Optional, Tuple

USER_AGENT = "curl/8.10.1"


def log(msg: str, prefix: str = "[*]") -> None:
    print(f"{prefix} {msg}", file=sys.stderr)


def err(msg: str) -> None:
    print(f"[-] ERROR: {msg}", file=sys.stderr)


def parse_kernel_string(input_str: str) -> Dict[str, Optional[str]]:
    """
    Parses a kernel identifier from:
    - uname -r: e.g. '6.6.13-200.fc39.x86_64'
    - uname -a: e.g. 'Linux host 6.6.13-200.fc39.x86_64 #1 SMP ...'
    - /proc/version: e.g. 'Linux version 6.6.13-200.fc39.x86_64 (mockbuild@...)'
    """
    s = input_str.strip()

    # If it's a /proc/version banner: "Linux version <release> ..."
    m_proc = re.search(r"Linux version ([^\s]+)", s)
    if m_proc:
        s = m_proc.group(1)

    # In case of uname -a, find the token containing release patterns
    tokens = s.split()
    target_token = tokens[0]
    for token in tokens:
        if re.search(r"\.(fc|el)\d+", token):
            target_token = token
            break

    # Strip any trailing architecture or packaging suffixes if needed
    # Standard format: <version>-<release>.<arch>
    m = re.match(r"^(\d+\.[\d\.\-_]+?)-([a-zA-Z0-9_\.\-]+)\.([a-zA-Z0-9_]+)$", target_token)
    if not m:
        parts = target_token.rsplit(".", 1)
        if len(parts) == 2 and "-" in parts[0]:
            arch = parts[1]
            version, release = parts[0].split("-", 1)
        else:
            raise ValueError(f"Could not parse kernel string '{input_str}'. Expected format like '6.6.13-200.fc39.x86_64'")
    else:
        version = m.group(1)
        release = m.group(2)
        arch = m.group(3)

    # Detect distribution from release tag
    distro = None
    major = None
    minor = None

    fc_match = re.search(r"\.fc(\d+)", release)
    el_match = re.search(r"\.el(\d+)(?:_(\d+))?", release)

    if fc_match:
        distro = "fedora"
        major = fc_match.group(1)
    elif el_match:
        distro = "rhel"
        major = el_match.group(1)
        minor = el_match.group(2)

    return {
        "full_release": target_token,
        "version": version,
        "release": release,
        "arch": arch,
        "detected_distro": distro,
        "major": major,
        "minor": minor,
    }


def check_url_exists(url: str) -> Optional[int]:
    """Checks if a URL returns HTTP 200 via HEAD request. Returns file size in bytes if found."""
    req = urllib.request.Request(url, method="HEAD")
    req.add_header("User-Agent", USER_AGENT)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status == 200:
                length = resp.headers.get("Content-Length")
                return int(length) if length else 0
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        pass
    return None


def download_file(url: str, dest_path: Path, expected_size: Optional[int] = None) -> None:
    """Downloads a file using curl (preferred) or urllib with streaming progress, verifying RPM magic."""
    if dest_path.exists():
        if dest_path.stat().st_size > 1024:
            try:
                with open(dest_path, "rb") as f_check:
                    if f_check.read(4) == b"\xed\xab\xee\xdb":
                        if expected_size and dest_path.stat().st_size == expected_size:
                            log(f"Cached file matches size ({expected_size / (1024*1024):.1f} MB): {dest_path.name}")
                            return
                        elif not expected_size:
                            log(f"Using existing cached RPM ({dest_path.stat().st_size / (1024*1024):.1f} MB): {dest_path.name}")
                            return
            except OSError:
                pass
        # Corrupted or non-RPM file in cache: clean up
        dest_path.unlink()

    log(f"Downloading: {url}")
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = dest_path.with_suffix(f"{dest_path.suffix}.tmp")
    if temp_path.exists():
        temp_path.unlink()

    curl_bin = shutil.which("curl")
    if curl_bin:
        cmd = [
            curl_bin,
            "-f",
            "-L",
            "--progress-bar",
            "-A", USER_AGENT,
            "-o", str(temp_path),
            url,
        ]
        res = subprocess.run(cmd)
        if res.returncode != 0:
            if temp_path.exists():
                temp_path.unlink()
            raise RuntimeError(f"curl failed to download {url} (exit code {res.returncode})")
    else:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        try:
            with urllib.request.urlopen(req, timeout=120) as resp, open(temp_path, "wb") as f_out:
                total_bytes = int(resp.headers.get("Content-Length") or 0)
                downloaded = 0
                chunk_size = 2 * 1024 * 1024  # 2 MB chunks
                last_pct = -1

                while True:
                    chunk = resp.read(chunk_size)
                    if not chunk:
                        break
                    f_out.write(chunk)
                    downloaded += len(chunk)
                    if total_bytes > 0:
                        pct = int(downloaded * 100 / total_bytes)
                        if pct != last_pct and pct % 10 == 0:
                            sys.stderr.write(f"\r    Progress: {pct}% ({downloaded / (1024*1024):.1f}/{total_bytes / (1024*1024):.1f} MB)")
                            sys.stderr.flush()
                            last_pct = pct

                if total_bytes > 0:
                    sys.stderr.write("\n")
                    sys.stderr.flush()
        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            raise e

    # Verify that the downloaded file is a valid RPM (magic \xed\xab\xee\xdb)
    with open(temp_path, "rb") as f_check:
        magic = f_check.read(4)
        if magic != b"\xed\xab\xee\xdb":
            f_check.seek(0)
            preview = f_check.read(256).decode("utf-8", errors="replace")
            temp_path.unlink()
            raise RuntimeError(
                f"Downloaded file from {url} is not a valid RPM package (magic={magic!r}).\n"
                f"Response preview: {preview[:150]}"
            )

    temp_path.rename(dest_path)


class FedoraResolver:
    """Resolves Fedora kernel packages using Fedora Koji."""

    KOJI_HUB = "https://koji.fedoraproject.org/kojihub"
    KOJIPKGS_BASE = "https://kojipkgs.fedoraproject.org/packages"

    @classmethod
    def resolve(cls, info: Dict[str, Optional[str]]) -> Tuple[str, str]:
        version = info["version"]
        release = info["release"]
        arch = info["arch"]

        build_nvr = f"kernel-{version}-{release}"
        log(f"Querying Fedora Koji build system for '{build_nvr}'...")

        debuginfo_url = None
        core_url = None
        fallback_core = None

        try:
            server = xmlrpc.client.ServerProxy(cls.KOJI_HUB)
            build = server.getBuild(build_nvr)
            if build and build.get("build_id"):
                build_id = build["build_id"]
                rpms = server.listRPMs(build_id)
                for r in rpms:
                    r_name = r.get("name", "")
                    r_arch = r.get("arch", "")
                    r_nvr = r.get("nvr", "")

                    if r_arch == arch:
                        if r_name == "kernel-debuginfo":
                            debuginfo_url = f"{cls.KOJIPKGS_BASE}/kernel/{version}/{release}/{arch}/{r_nvr}.{arch}.rpm"
                        elif r_name == "kernel-core":
                            core_url = f"{cls.KOJIPKGS_BASE}/kernel/{version}/{release}/{arch}/{r_nvr}.{arch}.rpm"
                        elif r_name == "kernel" and not fallback_core:
                            fallback_core = f"{cls.KOJIPKGS_BASE}/kernel/{version}/{release}/{arch}/{r_nvr}.{arch}.rpm"
        except Exception as e:
            log(f"Koji query encountered an error ({e}); trying predictable URL format...", "[!]")

        if not core_url and fallback_core:
            core_url = fallback_core

        if not debuginfo_url:
            debuginfo_url = f"{cls.KOJIPKGS_BASE}/kernel/{version}/{release}/{arch}/kernel-debuginfo-{version}-{release}.{arch}.rpm"
        if not core_url:
            core_url = f"{cls.KOJIPKGS_BASE}/kernel/{version}/{release}/{arch}/kernel-core-{version}-{release}.{arch}.rpm"

        # Verify package URLs exist
        log(f"Verifying Fedora package URLs...")
        if not check_url_exists(debuginfo_url):
            log(f"Warning: Debuginfo URL did not respond to HEAD: {debuginfo_url}", "[!]")
        if not check_url_exists(core_url):
            alt_core = f"{cls.KOJIPKGS_BASE}/kernel/{version}/{release}/{arch}/kernel-{version}-{release}.{arch}.rpm"
            if check_url_exists(alt_core):
                core_url = alt_core

        return debuginfo_url, core_url


class EnterpriseLinuxResolver:
    """
    Resolves RHEL, CentOS, Rocky Linux, and AlmaLinux packages
    by searching AlmaLinux Vault, Rocky Linux Vault, and CentOS Vault.
    """

    @classmethod
    def resolve(cls, info: Dict[str, Optional[str]], distro_hint: Optional[str] = None) -> Tuple[str, str]:
        version = info["version"]
        release = info["release"]
        arch = info["arch"]
        major = info.get("major")
        minor = info.get("minor")

        log(f"Searching Enterprise Linux mirrors for kernel {version}-{release}.{arch}...")

        # Determine minor versions to probe
        minor_candidates = []
        if minor:
            minor_candidates.append(f"{major}.{minor}")
        if major == "9":
            minor_candidates.extend(["9.5", "9.4", "9.3", "9.2", "9.1", "9.0"])
        elif major == "8":
            minor_candidates.extend(["8.10", "8.9", "8.8", "8.7", "8.6", "8.5", "8.4", "8.3", "8.2", "8.1", "8.0"])
        elif major == "7":
            minor_candidates.extend(["7.9.2009", "7.8.2003", "7.7.1908", "7.6.1810"])

        # Deduplicate while preserving order
        seen = set()
        minors = [m for m in minor_candidates if not (m in seen or seen.add(m))]

        debuginfo_rpm = f"kernel-debuginfo-{version}-{release}.{arch}.rpm"
        core_rpm = f"kernel-core-{version}-{release}.{arch}.rpm"
        kernel_rpm = f"kernel-{version}-{release}.{arch}.rpm"

        # 1. Search AlmaLinux Vault
        for m in minors:
            base_debug = f"https://repo.almalinux.org/vault/{m}/BaseOS/debug/{arch}/Packages"
            base_core = f"https://repo.almalinux.org/vault/{m}/BaseOS/{arch}/os/Packages"
            test_debug = f"{base_debug}/{debuginfo_rpm}"

            if check_url_exists(test_debug):
                log(f"Found packages in AlmaLinux Vault ({m})")
                test_core = f"{base_core}/{core_rpm}"
                if not check_url_exists(test_core):
                    test_core = f"{base_core}/{kernel_rpm}"
                return test_debug, test_core

        # 2. Search Rocky Linux Vault
        for m in minors:
            base_debug = f"https://download.rockylinux.org/vault/rocky/{m}/BaseOS/{arch}/debug/tree/Packages/k"
            base_core = f"https://download.rockylinux.org/vault/rocky/{m}/BaseOS/{arch}/os/Packages/k"
            test_debug = f"{base_debug}/{debuginfo_rpm}"

            if check_url_exists(test_debug):
                log(f"Found packages in Rocky Linux Vault ({m})")
                test_core = f"{base_core}/{core_rpm}"
                if not check_url_exists(test_core):
                    test_core = f"{base_core}/{kernel_rpm}"
                return test_debug, test_core

        # 3. Search CentOS Vault (particularly CentOS 7 and CentOS Stream)
        for m in minors:
            base_debug = f"https://vault.centos.org/{m}/BaseOS/{arch}/debug/tree/Packages"
            base_core = f"https://vault.centos.org/{m}/BaseOS/{arch}/os/Packages"
            test_debug = f"{base_debug}/{debuginfo_rpm}"

            if check_url_exists(test_debug):
                log(f"Found packages in CentOS Vault ({m})")
                test_core = f"{base_core}/{core_rpm}"
                if not check_url_exists(test_core):
                    test_core = f"{base_core}/{kernel_rpm}"
                return test_debug, test_core

        # If not found in vaults, construct best candidate URLs from AlmaLinux
        primary_ver = minors[0] if minors else f"{major}.0"
        fallback_debug = f"https://repo.almalinux.org/vault/{primary_ver}/BaseOS/debug/{arch}/Packages/{debuginfo_rpm}"
        fallback_core = f"https://repo.almalinux.org/vault/{primary_ver}/BaseOS/{arch}/os/Packages/{core_rpm}"
        return fallback_debug, fallback_core


def extract_file_from_rpm(rpm_path: Path, pattern: str, dest_dir: Path) -> List[Path]:
    """
    Extracts files matching a glob pattern from an RPM package using rpm2cpio and cpio.
    Works entirely in user space without requiring root privileges.
    """
    log(f"Extracting '{pattern}' from {rpm_path.name}...")
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Check for rpm2cpio and cpio
    if not shutil.which("rpm2cpio") or not shutil.which("cpio"):
        raise RuntimeError("Missing 'rpm2cpio' or 'cpio'. Please install them (e.g. `sudo dnf install rpm cpio`).")

    cmd = f'rpm2cpio "{rpm_path.resolve()}" | cpio -idmu --quiet "{pattern}"'
    res = subprocess.run(cmd, shell=True, cwd=dest_dir, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Failed to extract {pattern} from {rpm_path.name}: {res.stderr}")

    matched = []
    for root, _, files in os.walk(dest_dir):
        for f in files:
            p = Path(root) / f
            # Match vmlinux (uncompressed kernel ELF) or System.map
            if "vmlinux" in pattern:
                if (f == "vmlinux" or f.startswith("vmlinux-")) and not p.is_symlink():
                    if p.stat().st_size > 10_000_000:  # Kernel ELF is typically > 20 MB
                        matched.append(p)
            elif "System.map" in pattern:
                if f.startswith("System.map") and not p.is_symlink():
                    matched.append(p)

    return matched


def find_or_install_dwarf2json(custom_path: Optional[str] = None) -> Path:
    """Finds dwarf2json binary or installs it via go if available."""
    if custom_path:
        p = Path(custom_path)
        if p.exists() and os.access(p, os.X_OK):
            return p
        raise FileNotFoundError(f"Specified dwarf2json executable not found: {custom_path}")

    # Check standard PATH
    which_d2j = shutil.which("dwarf2json")
    if which_d2j:
        return Path(which_d2j)

    # Check Go bin directory
    home_go = Path.home() / "go" / "bin" / "dwarf2json"
    if home_go.exists() and os.access(home_go, os.X_OK):
        return home_go

    # Check if Go is installed to build it automatically
    if shutil.which("go"):
        log("dwarf2json not found in PATH; compiling via 'go install github.com/volatilityfoundation/dwarf2json@latest'...")
        res = subprocess.run(
            ["go", "install", "github.com/volatilityfoundation/dwarf2json@latest"],
            capture_output=True,
            text=True,
        )
        if res.returncode == 0 and home_go.exists():
            log(f"Successfully compiled dwarf2json to {home_go}")
            return home_go
        else:
            log(f"go install failed: {res.stderr}", "[!]")

    # Check for prebuilt release download from GitHub
    release_url = "https://github.com/volatilityfoundation/dwarf2json/releases/download/v0.8.0/dwarf2json-linux-amd64"
    d2j_cache = Path.home() / ".cache" / "volatility-symbols" / "bin" / "dwarf2json"
    if d2j_cache.exists() and os.access(d2j_cache, os.X_OK):
        return d2j_cache

    log(f"Attempting to download precompiled dwarf2json from GitHub releases...")
    try:
        download_file(release_url, d2j_cache)
        d2j_cache.chmod(0o755)
        return d2j_cache
    except Exception as e:
        raise RuntimeError(
            f"Could not find or install 'dwarf2json'. Please install Go or install dwarf2json manually:\n"
            f"  go install github.com/volatilityfoundation/dwarf2json@latest\n"
            f"Error details: {e}"
        )


def run_dwarf2json(dwarf2json_bin: Path, vmlinux_path: Path, system_map_path: Optional[Path], output_path: Path) -> None:
    """Executes dwarf2json to generate the Volatility 3 ISF table."""
    cmd = [str(dwarf2json_bin), "linux", "--elf", str(vmlinux_path)]
    if system_map_path and system_map_path.exists():
        cmd.extend(["--system-map", str(system_map_path)])

    log(f"Running dwarf2json to build Volatility 3 ISF table...")
    log(f"Command: {' '.join(cmd)} > {output_path.name}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f_out:
        res = subprocess.run(cmd, stdout=f_out, stderr=subprocess.PIPE, text=True)

    if res.returncode != 0:
        if output_path.exists():
            output_path.unlink()
        raise RuntimeError(f"dwarf2json failed with exit code {res.returncode}:\n{res.stderr}")

    # Validate output
    file_size = output_path.stat().st_size
    if file_size < 1024:
        raise RuntimeError(f"dwarf2json output is suspiciously small ({file_size} bytes). Output may be corrupted.")

    # Volatility 3 compatibility fix:
    # In Linux >= 6.10 / 7.x, struct module_sect_attr was replaced by struct bin_attribute.
    # However, Volatility 3's Linux symbol loader strictly requires module_sect_attr in user_types.
    try:
        with open(output_path, "r", encoding="utf-8") as f_in:
            data = json.load(f_in)
        if "user_types" in data and "module_sect_attr" not in data["user_types"]:
            data["user_types"]["module_sect_attr"] = {
                "size": 32,
                "fields": {
                    "address": {"type": {"kind": "base", "name": "unsigned long"}, "offset": 16},
                    "name": {"type": {"kind": "pointer", "subtype": {"kind": "base", "name": "char"}}, "offset": 8},
                },
                "kind": "struct",
            }
            with open(output_path, "w", encoding="utf-8") as f_out:
                json.dump(data, f_out)
    except Exception as e:
        log(f"Warning during post-processing symbol table: {e}", "[!]")

    log(f"Successfully generated ISF symbol table: {output_path} ({file_size / (1024*1024):.2f} MB)", "[+]")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download kernel debug packages and build Volatility 3 ISF symbol JSON for Fedora & Red Hat distros."
    )
    parser.add_argument(
        "kernel_input",
        nargs="?",
        help="Kernel version string, uname -r, uname -a, or /proc/version (can also be piped via stdin)",
    )
    parser.add_argument(
        "-k", "--kernel",
        dest="kernel_flag",
        help="Explicit kernel version string (e.g. '6.6.13-200.fc39.x86_64')",
    )
    parser.add_argument(
        "-d", "--distro",
        choices=["auto", "fedora", "rhel", "centos", "rocky", "alma"],
        default="auto",
        help="Target distribution family (default: auto-detect from version tag)",
    )
    parser.add_argument(
        "-o", "--output",
        help="Output path for the generated JSON symbol file (default: ./linux-kernel-<release>.json)",
    )
    parser.add_argument(
        "--install",
        action="store_true",
        help="Automatically copy the symbol file into Volatility 3's user symbol directory (~/.local/share/volatility3/symbols/linux/)",
    )
    parser.add_argument(
        "--cache-dir",
        default=os.path.expanduser("~/.cache/volatility-symbols"),
        help="Directory to cache downloaded RPMs (default: ~/.cache/volatility-symbols)",
    )
    parser.add_argument(
        "--dwarf2json",
        help="Path to dwarf2json executable (auto-detected if omitted)",
    )
    parser.add_argument(
        "--debuginfo-url",
        help="Direct URL to kernel-debuginfo RPM (overrides automatic repository resolution)",
    )
    parser.add_argument(
        "--core-url",
        help="Direct URL to kernel-core / kernel RPM (overrides automatic repository resolution)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse input, resolve download URLs, and exit without downloading or extracting",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete downloaded RPMs and extracted files after symbol generation",
    )

    args = parser.parse_args()

    # Determine input string
    raw_input = args.kernel_flag or args.kernel_input
    if not raw_input:
        if not sys.stdin.isatty():
            raw_input = sys.stdin.read().strip()
        else:
            parser.print_help()
            return 1

    try:
        info = parse_kernel_string(raw_input)
    except Exception as e:
        err(f"Parsing error: {e}")
        return 1

    log(f"Parsed Kernel Information:")
    log(f"  Target Release : {info['full_release']}")
    log(f"  Version        : {info['version']}")
    log(f"  Release        : {info['release']}")
    log(f"  Architecture   : {info['arch']}")
    log(f"  Detected Distro: {info['detected_distro'] or 'unknown'}")

    distro = args.distro
    if distro == "auto":
        distro = info["detected_distro"] or "fedora"

    log(f"Resolving packages for distribution family: {distro.upper()}...")

    # Resolve URLs
    if args.debuginfo_url and args.core_url:
        debuginfo_url = args.debuginfo_url
        core_url = args.core_url
    elif distro == "fedora":
        debuginfo_url, core_url = FedoraResolver.resolve(info)
    else:
        debuginfo_url, core_url = EnterpriseLinuxResolver.resolve(info, distro)

    log(f"Debuginfo Package URL: {debuginfo_url}")
    log(f"Core/Map Package URL : {core_url}")

    if args.dry_run:
        log("Dry-run complete. Exiting.", "[+]")
        return 0

    # Locate dwarf2json before doing large downloads
    d2j_bin = find_or_install_dwarf2json(args.dwarf2json)
    log(f"Using dwarf2json at: {d2j_bin}")

    # Set up cache and work directories
    cache_dir = Path(args.cache_dir)
    rpm_cache_dir = cache_dir / "rpms"
    rpm_cache_dir.mkdir(parents=True, exist_ok=True)

    debuginfo_rpm_name = debuginfo_url.split("/")[-1]
    core_rpm_name = core_url.split("/")[-1]

    debuginfo_rpm_path = rpm_cache_dir / debuginfo_rpm_name
    core_rpm_path = rpm_cache_dir / core_rpm_name

    # Download RPMs
    try:
        download_file(debuginfo_url, debuginfo_rpm_path)
    except Exception as e:
        err(f"Failed to download kernel-debuginfo from {debuginfo_url}: {e}")
        err("Tip: You can supply a direct link using --debuginfo-url <URL>.")
        return 1

    try:
        download_file(core_url, core_rpm_path)
    except Exception as e:
        log(f"Could not download kernel-core ({e}); continuing with debuginfo only (System.map may be omitted)...", "[!]")
        core_rpm_path = None

    # Extraction in temporary directory
    temp_extract_dir = Path(tempfile.mkdtemp(prefix="vol_extract_"))
    try:
        # Extract vmlinux
        vmlinux_matches = extract_file_from_rpm(debuginfo_rpm_path, "*vmlinux*", temp_extract_dir)
        if not vmlinux_matches:
            err(f"Could not find uncompressed 'vmlinux' inside {debuginfo_rpm_name}.")
            return 1

        vmlinux_path = vmlinux_matches[0]
        log(f"Located vmlinux: {vmlinux_path} ({vmlinux_path.stat().st_size / (1024*1024):.1f} MB)")

        # Extract System.map
        system_map_path = None
        if core_rpm_path and core_rpm_path.exists():
            map_matches = extract_file_from_rpm(core_rpm_path, "*System.map*", temp_extract_dir)
            if map_matches:
                system_map_path = map_matches[0]
                log(f"Located System.map: {system_map_path}")

        # Determine output file path
        if args.output:
            out_file = Path(args.output).resolve()
        else:
            out_file = Path(f"linux-kernel-{info['full_release']}.json").resolve()

        run_dwarf2json(d2j_bin, vmlinux_path, system_map_path, out_file)

        # Install into Volatility 3 symbols directory if requested
        if args.install:
            vol3_dir = Path.home() / ".local" / "share" / "volatility3" / "symbols" / "linux"
            vol3_dir.mkdir(parents=True, exist_ok=True)
            installed_path = vol3_dir / out_file.name
            shutil.copy2(out_file, installed_path)
            log(f"Installed symbol table into Volatility 3: {installed_path}", "[+]")

        if args.clean:
            log("Cleaning up cache and extracted files...")
            if debuginfo_rpm_path.exists():
                debuginfo_rpm_path.unlink()
            if core_rpm_path and core_rpm_path.exists():
                core_rpm_path.unlink()

    finally:
        if temp_extract_dir.exists():
            shutil.rmtree(temp_extract_dir, ignore_errors=True)

    log("Symbol generation complete!", "[+]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
