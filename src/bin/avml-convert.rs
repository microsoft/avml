// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use anyhow::{bail, Error, Result};
use argh::FromArgs;
use avml::ONE_MB;
use snap::read::FrameDecoder;
use std::{
    convert::TryFrom,
    fs::metadata,
    io::prelude::*,
    io::SeekFrom,
    path::{Path, PathBuf},
    str::FromStr,
};

fn convert(src: &Path, dst: &Path, compress: bool) -> Result<()> {
    let src_len = metadata(src)?.len();
    let mut image = avml::image::Image::new(1, src, dst)?;

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
                let mut decoder = FrameDecoder::new(&image.src);
                avml::image::copy_block(new_header, &mut decoder, &mut image.dst)?;
                image.src.seek(SeekFrom::Current(8))?;
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}

fn convert_to_raw(src: &Path, dst: &Path) -> Result<()> {
    let src_len = metadata(src)?.len();
    let mut image = avml::image::Image::new(1, src, dst)?;

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
                let mut decoder = FrameDecoder::new(&image.src);
                avml::image::copy(size, &mut decoder, &mut image.dst)?;
                image.src.seek(SeekFrom::Current(8))?;
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}

#[derive(FromArgs)]
/// AVML compress/decompress tool
struct Config {
    /// specify output format [possible values: raw, lime, lime_compressed.  Default: lime_compressed]
    #[argh(option, default = "Format::LimeCompressed")]
    source_format: Format,

    /// specify output format [possible values: raw, lime, lime_compressed.  Default: lime]
    #[argh(option, default = "Format::Lime")]
    format: Format,

    /// name of the source file to read to on local system
    #[argh(positional)]
    src: PathBuf,

    /// name of the destination file to write to on local system
    #[argh(positional)]
    dst: PathBuf,
}

enum Format {
    Raw,
    Lime,
    LimeCompressed,
}

impl FromStr for Format {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let x = match s {
            "raw" => Self::Raw,
            "lime" => Self::Lime,
            "lime_compressed" => Self::LimeCompressed,
            _ => bail!("unsupported format"),
        };
        Ok(x)
    }
}

fn main() -> Result<()> {
    let config: Config = argh::from_env();

    match (config.source_format, config.format) {
        (Format::Lime | Format::LimeCompressed, Format::Raw) => {
            convert_to_raw(&config.src, &config.dst)
        }
        (Format::Lime, Format::LimeCompressed) => convert(&config.src, &config.dst, true),
        (Format::LimeCompressed, Format::Lime) => convert(&config.src, &config.dst, false),
        (Format::Lime, Format::Lime)
        | (Format::LimeCompressed, Format::LimeCompressed)
        | (Format::Raw, Format::Raw) => bail!("no conversion required"),
        (Format::Raw, Format::Lime | Format::LimeCompressed) => {
            bail!("converting from raw not supported")
        }
    }
}
