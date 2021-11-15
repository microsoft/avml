// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use anyhow::{anyhow, bail, Context, Result};
use argh::FromArgs;
#[cfg(feature = "blobstore")]
use avml::ONE_MB;
use std::{
    fs::metadata,
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
    #[argh(option, default = "100")]
    sas_block_size: usize,

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

fn kcore(ranges: &[Range<u64>], filename: &Path, version: u32) -> Result<()> {
    if metadata("/proc/kcore")?.len() < 0x2000 {
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

    for range in ranges {
        for phdr in &file.phdrs {
            if range.start == phdr.vaddr - start {
                image.write_block(
                    phdr.offset,
                    Range {
                        start: range.start,
                        end: range.start + phdr.memsz,
                    },
                )?;
            }
        }
    }
    Ok(())
}

fn phys(ranges: &[Range<u64>], filename: &Path, mem: &Path, version: u32) -> Result<()> {
    let mut image = avml::image::Image::new(version, mem, filename).with_context(|| {
        format!(
            "unable to create image. source:{} destination:{}",
            mem.display(),
            filename.display()
        )
    })?;
    for range in ranges {
        let end = if mem == Path::new("/dev/crash") {
            (range.end >> 12) << 12
        } else {
            range.end
        };

        image
            .write_block(
                range.start,
                Range {
                    start: range.start,
                    end,
                },
            )
            .with_context(|| format!("unable to write block: {}:{}", range.start, end))?;
    }

    Ok(())
}

macro_rules! try_method {
    ($func:expr) => {{
        match $func {
            Ok(_) => return Ok(()),
            Err(err) => err,
        }
    }};
}

fn get_mem(src: Option<&Source>, dst: &Path, version: u32) -> Result<()> {
    let ranges =
        avml::iomem::parse(Path::new("/proc/iomem")).context("parsing /proc/iomem failed")?;

    if let Some(source) = src {
        match source {
            Source::ProcKcore => kcore(&ranges, dst, version)?,
            Source::DevCrash => phys(&ranges, dst, Path::new("/dev/crash"), version)?,
            Source::DevMem => phys(&ranges, dst, Path::new("/dev/mem"), version)?,
        };
    }

    let crash_err = try_method!(phys(&ranges, dst, Path::new("/dev/crash"), version));
    let kcore_err = try_method!(kcore(&ranges, dst, version));
    let devmem_err = try_method!(phys(&ranges, dst, Path::new("/dev/mem"), version));

    eprintln!("unable to read memory");
    eprintln!("/dev/crash failed: {:?}", crash_err);
    eprintln!("/proc/kcore failed: {:?}", kcore_err);
    eprintln!("/dev/mem failed: {:?}", devmem_err);

    bail!("unable to read physical memory")
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
        let sas_block_size = config.sas_block_size * ONE_MB;

        if let Some(sas_url) = &config.sas_url {
            avml::blobstore::upload_sas(&config.filename, sas_url, sas_block_size)
                .await
                .context("upload via sas URL failed")?;
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
