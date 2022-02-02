// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use anyhow::{anyhow, bail, Context, Result};
use argh::FromArgs;
use avml::image::Block;
use std::{
    fs::{metadata, OpenOptions},
    ops::Range,
    path::{Path, PathBuf},
    str::FromStr,
};
#[cfg(any(feature = "blobstore", feature = "put"))]
use tokio::{fs::remove_file, runtime::Runtime};
#[cfg(any(feature = "blobstore", feature = "put"))]
use url::Url;

#[derive(FromArgs)]
/// A portable volatile memory acquisition tool for Linux
struct Config {
    /// compress via snappy
    #[argh(switch)]
    compress: bool,

    /// specify input source [possible values: /proc/kcore, /dev/crash, /dev/mem]
    #[argh(option)]
    source: Option<Source>,

    /// upload via HTTP PUT upon acquisition
    #[cfg(feature = "put")]
    #[argh(option)]
    url: Option<Url>,

    /// delete upon successful upload
    #[cfg(any(feature = "blobstore", feature = "put"))]
    #[argh(switch)]
    delete: bool,

    /// upload via Azure Blob Store upon acquisition
    #[cfg(feature = "blobstore")]
    #[argh(option)]
    sas_url: Option<Url>,

    /// specify maximum block size in MiB
    #[cfg(feature = "blobstore")]
    #[argh(option)]
    sas_block_size: Option<usize>,

    /// specify blob upload concurrency
    #[cfg(feature = "blobstore")]
    #[argh(option)]
    sas_block_concurrency: Option<usize>,

    /// name of the file to write to on local system
    #[argh(positional)]
    filename: PathBuf,
}

enum Source {
    DevCrash,
    DevMem,
    ProcKcore,
}

impl FromStr for Source {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let x = match s {
            "/dev/crash" => Self::DevCrash,
            "/dev/mem" => Self::DevMem,
            "/proc/kcore" => Self::ProcKcore,
            _ => bail!("unsupported format"),
        };
        Ok(x)
    }
}

fn can_open(src: &Path) -> bool {
    OpenOptions::new().read(true).open(src).is_ok()
}

// The file /proc/kcore is a psuedo-file in ELF core format that is 4KB+physical
// memory in size.
//
// If LOCKDOWN_KCORE is set in the kernel, then /proc/kcore may exist but is
// either inaccessible or doesn't allow access to all of the kernel memory.
//
// /dev/mem and /dev/crash, if available, are devices, rather than virtual
// files.  As such, we don't check those for size.
fn is_kcore_ok() -> bool {
    let path = Path::new("/proc/kcore");
    metadata(path).map(|x| x.len() > 0x2000).unwrap_or(false) && can_open(path)
}

fn kcore(ranges: &[Range<u64>], filename: &Path, version: u32) -> Result<()> {
    if !is_kcore_ok() {
        bail!("locked down kcore");
    }

    let mut image = avml::image::Image::new(version, Path::new("/proc/kcore"), filename)
        .with_context(|| {
            format!(
                "unable to create image. source: /proc/kcore destination: {}",
                filename.display()
            )
        })?;

    let mut file = elf::File::open_stream(&mut image.src)
        .map_err(|e| anyhow!("unable to parse ELF structures from /proc/kcore: {:?}", e))?;
    file.phdrs.retain(|&x| x.progtype == elf::types::PT_LOAD);
    file.phdrs.sort_by(|a, b| a.vaddr.cmp(&b.vaddr));
    let start = file.phdrs[0].vaddr - ranges[0].start;

    let mut blocks = vec![];
    'outer: for range in ranges {
        for phdr in &file.phdrs {
            if range.start == phdr.vaddr - start {
                let size = u64::min(phdr.memsz, range.end - range.start);
                blocks.push(Block {
                    offset: phdr.offset,
                    range: range.start..range.start + size,
                });
                continue 'outer;
            }
        }
        bail!(
            "unable to find memory range: {:016x}:{:016x}",
            range.start,
            range.end
        );
    }

    image.write_blocks(&blocks)?;
    Ok(())
}

fn phys(ranges: &[Range<u64>], filename: &Path, mem: &Path, version: u32) -> Result<()> {
    let is_crash = mem == Path::new("/dev/crash");
    let blocks = ranges
        .iter()
        .map(|x| Block {
            offset: x.start,
            range: if is_crash {
                x.start..((x.end >> 12) << 12)
            } else {
                x.start..x.end
            },
        })
        .collect::<Vec<_>>();

    let mut image = avml::image::Image::new(version, mem, filename).with_context(|| {
        format!(
            "unable to create image. source:{} destination:{}",
            mem.display(),
            filename.display()
        )
    })?;

    image.write_blocks(&blocks)?;

    Ok(())
}

macro_rules! try_method {
    ($func:expr) => {{
        match $func {
            Ok(x) => return Ok(x),
            Err(err) => err,
        }
    }};
}

fn read_src(ranges: &[Range<u64>], src: &Source, dst: &Path, version: u32) -> Result<()> {
    match src {
        Source::ProcKcore => kcore(ranges, dst, version),
        Source::DevCrash => phys(ranges, dst, Path::new("/dev/crash"), version),
        Source::DevMem => phys(ranges, dst, Path::new("/dev/mem"), version),
    }
}

fn get_mem(src: Option<&Source>, dst: &Path, version: u32) -> Result<()> {
    let ranges = avml::iomem::parse().context("unable to parse /proc/iomem")?;

    if let Some(src) = src {
        read_src(&ranges, src, dst, version)
    } else if dst == Path::new("/dev/stdout") {
        // If we're writing to stdout, we can't start over if reading from a
        // source fails.  As such, we need to do more work to pick a source
        // rather than just trying all available options.
        if is_kcore_ok() {
            read_src(&ranges, &Source::ProcKcore, dst, version)
                .context("reading /proc/kcore failed")
        } else if can_open(Path::new("/dev/crash")) {
            read_src(&ranges, &Source::DevCrash, dst, version).context("reading /dev/crash failed")
        } else if can_open(Path::new("/dev/mem")) {
            read_src(&ranges, &Source::DevMem, dst, version).context("reading /dev/mem failed")
        } else {
            bail!("unable to read memory");
        }
    } else {
        let crash_err = try_method!(read_src(&ranges, &Source::DevCrash, dst, version));
        let kcore_err = try_method!(read_src(&ranges, &Source::ProcKcore, dst, version));
        let devmem_err = try_method!(read_src(&ranges, &Source::DevMem, dst, version));

        eprintln!("/dev/crash failed: {:?}", crash_err);
        eprintln!("/proc/kcore failed: {:?}", kcore_err);
        eprintln!("/dev/mem failed: {:?}", devmem_err);
        bail!("unable to read memory");
    }
}

#[cfg(any(feature = "blobstore", feature = "put"))]
async fn upload(config: &Config) -> Result<()> {
    let mut delete = false;

    #[cfg(feature = "put")]
    {
        if let Some(url) = &config.url {
            avml::upload::put(&config.filename, url)
                .await
                .context("unable to upload via PUT")?;
            delete = true;
        }
    }

    #[cfg(feature = "blobstore")]
    {
        if let Some(sas_url) = &config.sas_url {
            let uploader = avml::BlobUploader::new(sas_url)?
                .block_size(config.sas_block_size)
                .concurrency(config.sas_block_concurrency);
            uploader
                .upload_file(&config.filename)
                .await
                .context("upload via SAS URL failed")?;
            delete = true;
        }
    }

    if delete && config.delete {
        remove_file(&config.filename)
            .await
            .context("unable to remove file after PUT")?;
    }

    Ok(())
}

fn main() -> Result<()> {
    let config: Config = argh::from_env();

    let version = if config.compress { 2 } else { 1 };
    get_mem(config.source.as_ref(), &config.filename, version)
        .context("unable to acquire memory")?;

    #[cfg(any(feature = "blobstore", feature = "put"))]
    {
        let rt = Runtime::new()?;
        rt.block_on(upload(&config))?;
    }

    Ok(())
}
