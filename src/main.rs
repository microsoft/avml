// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#[macro_use]
extern crate clap;
extern crate byteorder;
extern crate elf;
extern crate snap;

#[cfg(feature = "put")]
extern crate reqwest;

#[cfg(feature = "blobstore")]
extern crate azure;
#[cfg(feature = "blobstore")]
extern crate retry;
#[cfg(feature = "blobstore")]
extern crate tokio_core;
#[cfg(feature = "blobstore")]
extern crate url;

use clap::{App, Arg};
use std::error::Error;
#[cfg(any(feature = "blobstore", feature = "put"))]
use std::fs;
use std::fs::metadata;

#[cfg(feature = "blobstore")]
mod blobstore;
mod image;
mod iomem;
#[cfg(feature = "put")]
mod upload;

fn kcore(
    ranges: &[std::ops::Range<u64>],
    filename: &str,
    version: u32,
) -> Result<(), Box<dyn Error>> {
    if metadata("/proc/kcore")?.len() < 0x2000 {
        return Err(From::from("locked down kcore"));
    }

    let mut image = image::Image::new(version, "/proc/kcore", filename)?;
    let mut file = elf::File::open_stream(&mut image.src).expect("unable to analyze /proc/kcore");
    file.phdrs.retain(|&x| x.progtype == elf::types::PT_LOAD);
    file.phdrs.sort_by(|a, b| a.vaddr.cmp(&b.vaddr));
    let start = file.phdrs[0].vaddr - ranges[0].start;

    for range in ranges {
        for phdr in &file.phdrs {
            if range.start == phdr.vaddr - start {
                image.handle_block(phdr.offset, range.start, phdr.memsz)?;
            }
        }
    }
    Ok(())
}

fn phys(
    ranges: &[std::ops::Range<u64>],
    filename: &str,
    mem: &str,
    version: u32,
) -> Result<(), Box<dyn Error>> {
    let mut image = image::Image::new(version, mem, filename)?;
    for range in ranges {
        image.handle_block(range.start, range.start, 1 + range.end - range.start)?;
    }

    Ok(())
}

macro_rules! try_method {
    ($func:expr, $src:expr) => {{
        eprintln!("trying {}", $src);
        if let Err(err) = $func {
            eprintln!("failed {}: {}", $src, err);
        } else {
            eprintln!("succeeded {}", $src);
            return Ok(());
        }
    }};
}

fn get_mem(src: Option<&str>, dst: &str, version: u32) -> Result<(), Box<dyn Error>> {
    let ranges = iomem::parse("/proc/iomem")?;

    if let Some(source) = src {
        let result = match source {
            "/proc/kcore" => kcore(&ranges, dst, version),
            "/dev/crash" => phys(&ranges, dst, "/dev/crash", version),
            "/dev/mem" => phys(&ranges, dst, "/dev/mem", version),
            _ => unimplemented!(),
        };
        if result.is_err() {
            eprintln!("failed: {}", source);
        }
        return result;
    }

    try_method!(phys(&ranges, dst, "/dev/crash", version), "/dev/crash");
    try_method!(kcore(&ranges, dst, version), "/proc/kcore");
    try_method!(phys(&ranges, dst, "/dev/mem", version), "/dev/mem");

    Err(From::from("unable to read physical memory"))
}

fn run_app() -> Result<(), Box<dyn Error>> {
    let sources = vec!["/proc/kcore", "/dev/crash", "/dev/mem"];
    let args = App::new("avml")
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

    get_mem(src, &dst, version)?;

    #[cfg(any(feature = "blobstore", feature = "put"))]
    let mut delete = false;

    #[cfg(feature = "put")]
    {
        let url = args.value_of("url");
        if let Some(url) = url {
            upload::put(&dst, url)?;
            if args.is_present("delete") {
                fs::remove_file(&dst)?;
            }
        }
    }

    #[cfg(feature = "blobstore")]
    {
        let sas_url = args.value_of("sas_url");
        let sas_block_size = if args.is_present("sas_block_size") {
            value_t!(args.value_of("sas_block_size"), usize)?
        } else {
            100
        } * 1024
            * 1024;
        if let Some(sas_url) = sas_url {
            blobstore::upload_sas(&dst, sas_url, sas_block_size)?;
            delete = true;
        }
    }

    #[cfg(any(feature = "blobstore", feature = "put"))]
    {
        if delete && args.is_present("delete") {
            fs::remove_file(&dst)?;
        }
    }

    Ok(())
}

fn main() {
    ::std::process::exit(match run_app() {
        Ok(_) => 0,
        Err(err) => {
            eprintln!("error: {:?}", err);
            1
        }
    });
}
