// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use anyhow::{bail, Result};
use argh::FromArgs;
use avml::ONE_MB;
use snap::Reader;
use std::{convert::TryFrom, fs::metadata, io::prelude::*, io::SeekFrom};

const LIME: &str = "lime";
const LIME_COMPRESSED: &str = "lime_compressed";
const RAW: &str = "raw";

fn convert(src: String, dst: String, compress: bool) -> Result<()> {
    let src_len = metadata(&src)?.len();
    let mut image = avml::image::Image::new(1, &src, &dst)?;

    loop {
        let current = image.src.seek(SeekFrom::Current(0))?;
        if current >= src_len {
            break;
        }

        let header = avml::image::Header::read(&image.src)?;
        let mut new_header = header.clone();
        new_header.version = if compress { 2 } else { 1 };

        match header.version {
            1 => {
                avml::image::copy_block(new_header, &mut image.src, &mut image.dst)?;
            }
            2 => {
                let mut reader = Reader::new(&image.src);
                avml::image::copy_block(new_header, &mut reader, &mut image.dst)?;
                image.src.seek(SeekFrom::Current(8))?;
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}

fn convert_to_raw(src: String, dst: String) -> Result<()> {
    let src_len = metadata(&src)?.len();
    let mut image = avml::image::Image::new(1, &src, &dst)?;

    loop {
        let current = image.src.seek(SeekFrom::Current(0))?;
        if current >= src_len {
            break;
        }
        let current_dst = image.dst.seek(SeekFrom::Current(0))?;

        let header = avml::image::Header::read(&image.src)?;
        let mut zeros = vec![0; ONE_MB];

        let mut unmapped = usize::try_from(header.range.start - current_dst)?;
        while unmapped > ONE_MB {
            image.dst.write_all(&zeros)?;
            unmapped -= ONE_MB;
        }
        if unmapped > 0 {
            zeros.resize(unmapped, 0);
            image.dst.write_all(&zeros)?;
        }

        let size = usize::try_from(header.range.end - header.range.start)?;

        match header.version {
            1 => {
                avml::image::copy(size, &mut image.src, &mut image.dst)?;
            }
            2 => {
                let mut reader = Reader::new(&image.src);
                avml::image::copy(size, &mut reader, &mut image.dst)?;
                image.src.seek(SeekFrom::Current(8))?;
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}

#[derive(FromArgs, Debug)]
/// AVML compress/decompress tool
struct Config {
    /// compress via snappy
    #[argh(switch)]
    compress: bool,

    /// output format
    #[argh(option, default = "LIME.to_string()")]
    format: String,

    /// upload via HTTP PUT upon acquisition
    #[cfg(feature = "put")]
    #[argh(option)]
    url: Option<String>,

    /// delete upon successful upload
    #[argh(switch)]
    delete: bool,

    /// upload via Azure Blob Store upon acquisition
    #[cfg(feature = "blobstore")]
    #[argh(option)]
    sas_url: Option<String>,

    /// specify maximum block size in MiB
    #[cfg(feature = "blobstore")]
    #[argh(option, default = "100")]
    sas_block_size: usize,

    /// name of the source file to read to on local system
    #[argh(positional)]
    source: String,

    /// name of the destination file to write to on local system
    #[argh(positional)]
    destination: String,
}

fn main() -> Result<()> {
    let config: Config = argh::from_env();

    match config.format.as_ref() {
        RAW => convert_to_raw(config.source, config.destination)?,
        LIME => convert(config.source, config.destination, false)?,
        LIME_COMPRESSED => convert(config.source, config.destination, true)?,
        _ => bail!(
            "unsupported format: {}.  Supported formats {}",
            config.format,
            &[RAW, LIME, LIME_COMPRESSED].join(", ")
        ),
    }

    Ok(())
}
