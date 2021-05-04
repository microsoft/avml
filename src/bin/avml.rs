// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#[macro_use]
extern crate clap;

use anyhow::{anyhow, bail, Context, Result};
#[cfg(feature = "blobstore")]
use avml::ONE_MB;
use clap::{App, Arg};
#[cfg(any(feature = "blobstore", feature = "put"))]
use std::fs::remove_file;
use std::{fs::metadata, ops::Range};

fn kcore(ranges: &[std::ops::Range<u64>], filename: &str, version: u32) -> Result<()> {
    if metadata("/proc/kcore")?.len() < 0x2000 {
        bail!("locked down kcore");
    }

    let mut image =
        avml::image::Image::new(version, "/proc/kcore", filename).with_context(|| {
            format!(
                "unable to create image. source: /proc/kcore destination: {}",
                filename
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

fn phys(ranges: &[std::ops::Range<u64>], filename: &str, mem: &str, version: u32) -> Result<()> {
    let mut image = avml::image::Image::new(version, mem, filename).with_context(|| {
        format!(
            "unable to create image. source:{} destination:{}",
            mem, filename
        )
    })?;
    for range in ranges {
        let end = if mem == "/dev/crash" {
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

fn get_mem(src: Option<&str>, dst: &str, version: u32) -> Result<()> {
    let ranges = avml::iomem::parse("/proc/iomem").context("parsing /proc/iomem failed")?;

    if let Some(source) = src {
        match source {
            "/proc/kcore" => kcore(&ranges, dst, version)?,
            "/dev/crash" => phys(&ranges, dst, "/dev/crash", version)?,
            "/dev/mem" => phys(&ranges, dst, "/dev/mem", version)?,
            _ => unimplemented!(),
        };
    }

    let crash_err = try_method!(phys(&ranges, dst, "/dev/crash", version));
    let kcore_err = try_method!(kcore(&ranges, dst, version));
    let devmem_err = try_method!(phys(&ranges, dst, "/dev/mem", version));

    eprintln!("unable to read memory");
    eprintln!("/dev/crash failed: {:?}", crash_err);
    eprintln!("/proc/kcore failed: {:?}", kcore_err);
    eprintln!("/dev/mem failed: {:?}", devmem_err);

    bail!("unable to read physical memory")
}

fn main() -> Result<()> {
    let sources = vec!["/proc/kcore", "/dev/crash", "/dev/mem"];
    let args = App::new(crate_name!())
        .author(crate_authors!())
        .about(crate_description!())
        .version(crate_version!())
        .args(&[
            Arg::with_name("compress")
                .long("compress")
                .help("compress pages via snappy"),
            Arg::with_name("filename")
                .help("name of the file to write to on local system")
                .required(true),
            Arg::with_name("source")
                .long("source")
                .takes_value(true)
                .possible_values(&sources)
                .help("specify input source"),
            #[cfg(feature = "blobstore")]
            Arg::with_name("sas_url")
                .long("sas_url")
                .takes_value(true)
                .help("Upload via Azure Blob Store upon acquisition")
                .conflicts_with("url"),
            #[cfg(feature = "blobstore")]
            Arg::with_name("sas_block_size")
                .long("sas_block_size")
                .takes_value(true)
                .help("specify maximum block size in MiB"),
            #[cfg(feature = "put")]
            Arg::with_name("url")
                .long("url")
                .takes_value(true)
                .help("Upload via HTTP PUT upon acquisition.")
                .required(false),
            #[cfg(any(feature = "blobstore", feature = "put"))]
            Arg::with_name("delete")
                .long("delete")
                .help("delete upon successful upload"),
        ])
        .get_matches();

    let src = args.value_of("source");
    let dst = value_t!(args.value_of("filename"), String)?;
    let version = if args.is_present("compress") { 2 } else { 1 };

    get_mem(src, &dst, version).context("unable to collect memory")?;

    #[cfg(any(feature = "blobstore", feature = "put"))]
    let mut delete = false;

    #[cfg(feature = "put")]
    {
        let url = args.value_of("url");
        if let Some(url) = url {
            avml::upload::put(&dst, url).context("unable to upload via PUT")?;
            delete = true;
        }
    }

    #[cfg(feature = "blobstore")]
    {
        let sas_url = args.value_of("sas_url");
        let sas_block_size = if args.is_present("sas_block_size") {
            value_t!(args.value_of("sas_block_size"), usize)?
        } else {
            100
        } * ONE_MB;

        if let Some(sas_url) = sas_url {
            avml::blobstore::upload_sas(&dst, sas_url, sas_block_size)
                .context("upload via sas URL failed")?;
            delete = true;
        }
    }

    #[cfg(any(feature = "blobstore", feature = "put"))]
    {
        if delete && args.is_present("delete") {
            remove_file(&dst).context("unable to remove file after PUT")?;
        }
    }

    Ok(())
}
