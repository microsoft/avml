// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::manual_assert)]
#![deny(clippy::indexing_slicing)]

use avml::{image, iomem::split_ranges, Error, Result, Snapshot, Source, ONE_MB};
use clap::{Parser, ValueEnum};
use snap::read::FrameDecoder;
use std::{
    convert::TryFrom,
    fs::metadata,
    io::{prelude::*, SeekFrom},
    path::{Path, PathBuf},
};

fn convert(src: &Path, dst: &Path, compress: bool) -> Result<()> {
    let src_len = metadata(src).map_err(image::Error::Read)?.len();
    let mut image = image::Image::new(1, src, dst)?;

    loop {
        let current = image.src.stream_position().map_err(image::Error::Read)?;
        if current >= src_len {
            break;
        }

        let header = image::Header::read(&image.src)?;
        let mut new_header = header.clone();
        new_header.version = if compress { 2 } else { 1 };

        match header.version {
            1 => {
                image::copy_block(new_header, &mut image.src, &mut image.dst)?;
            }
            2 => {
                let mut decoder = FrameDecoder::new(&image.src);
                image::copy_block(new_header, &mut decoder, &mut image.dst)?;
                image
                    .src
                    .seek(SeekFrom::Current(8))
                    .map_err(image::Error::Read)?;
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}

fn convert_to_raw(src: &Path, dst: &Path) -> Result<()> {
    let src_len = metadata(src).map_err(image::Error::Read)?.len();
    let mut image = image::Image::new(1, src, dst)?;

    loop {
        let current = image.src.stream_position().map_err(image::Error::Read)?;
        if current >= src_len {
            break;
        }
        let current_dst = image.dst.stream_position().map_err(image::Error::Read)?;

        let header = image::Header::read(&image.src)?;
        let mut zeros = vec![0; ONE_MB];

        let mut unmapped = usize::try_from(header.range.start - current_dst)
            .map_err(|_| image::Error::SizeConversion)?;
        while unmapped > ONE_MB {
            image.dst.write_all(&zeros).map_err(image::Error::Write)?;
            unmapped -= ONE_MB;
        }
        if unmapped > 0 {
            zeros.resize(unmapped, 0);
            image.dst.write_all(&zeros).map_err(image::Error::Write)?;
        }

        let size = usize::try_from(header.range.end - header.range.start)
            .map_err(|_| image::Error::SizeConversion)?;

        match header.version {
            1 => {
                image::copy(size, &mut image.src, &mut image.dst)?;
            }
            2 => {
                let mut decoder = FrameDecoder::new(&image.src);
                image::copy(size, &mut decoder, &mut image.dst)?;
                image
                    .src
                    .seek(SeekFrom::Current(8))
                    .map_err(image::Error::Read)?;
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}

fn convert_from_raw(src: &Path, dst: &Path, compress: bool) -> Result<()> {
    let src_len = metadata(src).map_err(image::Error::Read)?.len();
    let ranges = split_ranges(vec![0..src_len], image::MAX_BLOCK_SIZE);

    let version = if compress { 2 } else { 1 };

    let source = Source::Raw(src.to_owned());

    Snapshot::new(dst, ranges)
        .version(version)
        .source(Some(&source))
        .create()?;

    Ok(())
}

#[derive(Parser)]
/// AVML compress/decompress tool
#[command(version)]
struct Config {
    /// specify output format
    #[arg(long, value_enum, default_value_t = Format::LimeCompressed)]
    source_format: Format,

    /// specify output format
    #[arg(long, value_enum, default_value_t = Format::Lime)]
    format: Format,

    /// name of the source file to read to on local system
    src: PathBuf,

    /// name of the destination file to write to on local system
    dst: PathBuf,
}

#[derive(ValueEnum, Clone)]
enum Format {
    Raw,
    Lime,
    #[value(rename_all = "snake_case")]
    LimeCompressed,
}

fn main() -> Result<()> {
    let config = Config::parse();

    match (config.source_format, config.format) {
        (Format::Lime | Format::LimeCompressed, Format::Raw) => {
            convert_to_raw(&config.src, &config.dst)
        }
        (Format::Lime, Format::LimeCompressed) => convert(&config.src, &config.dst, true),
        (Format::LimeCompressed, Format::Lime) => convert(&config.src, &config.dst, false),
        (Format::Raw, Format::Lime) => convert_from_raw(&config.src, &config.dst, false),
        (Format::Raw, Format::LimeCompressed) => convert_from_raw(&config.src, &config.dst, true),
        (Format::Lime, Format::Lime)
        | (Format::LimeCompressed, Format::LimeCompressed)
        | (Format::Raw, Format::Raw) => Err(Error::NoConversionRequired),
    }
}
