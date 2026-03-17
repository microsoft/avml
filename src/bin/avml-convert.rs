// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::manual_assert)]
#![deny(clippy::indexing_slicing)]

use avml::{Error, ONE_MB, Result, image};
use clap::{Parser, ValueEnum};
use snap::read::FrameDecoder;
use std::{
    fs::{File, metadata},
    io::{Read, Seek, SeekFrom, Write, copy},
    path::{Path, PathBuf},
};

fn convert(src: &Path, dst: &Path, compress: bool) -> Result<()> {
    let src_len = metadata(src)
        .map_err(|e| image::Error::Io(e, "unable to read source size"))?
        .len();
    let mut image = image::Image::<File, File>::new(1, src, dst)?;
    convert_image(&mut image, src_len, compress)
}

fn convert_image<R, W>(image: &mut image::Image<R, W>, src_len: u64, compress: bool) -> Result<()>
where
    R: Read + Seek,
    W: Write,
{
    image.version = if compress { 2 } else { 1 };
    loop {
        let current = image.src.stream_position().map_err(|e| {
            image::Error::Io(e, "unable to get current offset into the memory source")
        })?;
        if current >= src_len {
            break;
        }

        image.convert_block()?;
    }

    Ok(())
}

fn convert_to_raw_image<R, W>(image: &mut image::Image<R, W>, src_len: u64) -> Result<()>
where
    R: Read + Seek,
    W: Write + Seek,
{
    image.version = 1;
    loop {
        let current = image.src.stream_position().map_err(|e| {
            image::Error::Io(e, "unable to get the current offset into the memory source")
        })?;
        if current >= src_len {
            break;
        }
        let current_dst = image.dst.stream_position().map_err(|e| {
            image::Error::Io(
                e,
                "unable to get the current offset into the destination stream",
            )
        })?;

        let header = image.read_header()?;
        let mut zeros = vec![0; ONE_MB];

        let mut unmapped = usize::try_from(header.range.start - current_dst)
            .map_err(image::Error::IntConversion)?;
        while unmapped > ONE_MB {
            image
                .dst
                .write_all(&zeros)
                .map_err(|e| image::Error::Io(e, "unable to write padding bytes"))?;
            unmapped -= ONE_MB;
        }
        if unmapped > 0 {
            zeros.resize(unmapped, 0);
            image
                .dst
                .write_all(&zeros)
                .map_err(|e| image::Error::Io(e, "unable to write padding bytes"))?;
        }

        let size = header.size()?;

        match header.version {
            1 => {
                let mut handle =
                    (&mut image.src).take(size.try_into().map_err(image::Error::IntConversion)?);
                copy(&mut handle, &mut image.dst)
                    .map_err(|e| image::Error::Io(e, "unable to copy image data"))?;
            }
            2 => {
                let mut decoder = FrameDecoder::new(&mut image.src)
                    .take(size.try_into().map_err(image::Error::IntConversion)?);
                copy(&mut decoder, &mut image.dst)
                    .map_err(|e| image::Error::Io(e, "unable to copy image data"))?;
                image
                    .src
                    .seek(SeekFrom::Current(8))
                    .map_err(|e| image::Error::Io(e, "unable to seek past the compressed size"))?;
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}

fn convert_to_raw(src: &Path, dst: &Path) -> Result<()> {
    let src_len = metadata(src)
        .map_err(|e| image::Error::Io(e, "unable to get source file size"))?
        .len();
    let mut image = image::Image::<File, File>::new(1, src, dst)?;
    convert_to_raw_image(&mut image, src_len)
}

fn encode_raw_image<R, W>(
    image: &mut image::Image<R, W>,
    raw_len: u64,
    compress: bool,
) -> Result<()>
where
    R: Read + Seek,
    W: Write,
{
    image.version = if compress { 2 } else { 1 };

    let mut start = 0_u64;
    while start < raw_len {
        let end = raw_len.min(start.saturating_add(image::MAX_BLOCK_SIZE));
        image.copy_block(start..end)?;
        start = end;
    }

    Ok(())
}

fn convert_from_raw(src: &Path, dst: &Path, compress: bool) -> Result<()> {
    let src_len = metadata(src)
        .map_err(|e| image::Error::Io(e, "unable to read source size"))?
        .len();
    let mut image = image::Image::<File, File>::new(1, src, dst)?;
    encode_raw_image(&mut image, src_len, compress)
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

#[cfg(test)]
mod tests {
    use crate::{convert_image, convert_to_raw_image, encode_raw_image};
    use avml::{Result, image};
    use rand::{RngCore, SeedableRng, rngs::SmallRng};
    use std::io::Cursor;

    fn memory_image(src: &[u8]) -> image::Image<Cursor<&[u8]>, Cursor<Vec<u8>>> {
        image::Image {
            version: 1,
            align_src: false,
            src: Cursor::new(src),
            dst: Cursor::new(Vec::new()),
        }
    }

    fn block_size() -> Result<usize> {
        usize::try_from(image::MAX_BLOCK_SIZE)
            .map_err(image::Error::IntConversion)
            .map_err(Into::into)
    }

    fn random_bytes(rng: &mut SmallRng, len: usize) -> Vec<u8> {
        let mut bytes = vec![0; len];
        rng.fill_bytes(&mut bytes);
        bytes
    }

    fn build_sparse_raw() -> Result<Vec<u8>> {
        let mut rng = SmallRng::seed_from_u64(0);

        let block_size = block_size()?;
        let partial_head_len = block_size / 4;
        let partial_tail_len = block_size.saturating_sub(partial_head_len);
        let chunks = vec![
            vec![0; block_size],
            random_bytes(&mut rng, block_size),
            vec![0; block_size],
            random_bytes(&mut rng, partial_head_len),
            vec![0; block_size],
            random_bytes(&mut rng, partial_tail_len),
            random_bytes(&mut rng, block_size),
            vec![0; block_size],
            random_bytes(&mut rng, block_size),
        ];

        Ok(chunks.concat())
    }

    fn encode_raw(raw: &[u8], version: u32) -> Result<Vec<u8>> {
        let mut image = memory_image(raw);
        let total = u64::try_from(raw.len()).map_err(image::Error::IntConversion)?;
        encode_raw_image(&mut image, total, version == 2)?;
        Ok(image.dst.into_inner())
    }

    fn convert_encoded(encoded: &[u8], compress: bool) -> Result<Vec<u8>> {
        let encoded_len = u64::try_from(encoded.len()).map_err(image::Error::IntConversion)?;
        let mut image = memory_image(encoded);
        convert_image(&mut image, encoded_len, compress)?;
        Ok(image.dst.into_inner())
    }

    fn decode_to_raw(encoded: &[u8]) -> Result<Vec<u8>> {
        let encoded_len = u64::try_from(encoded.len()).map_err(image::Error::IntConversion)?;
        let mut image = memory_image(encoded);
        convert_to_raw_image(&mut image, encoded_len)?;
        Ok(image.dst.into_inner())
    }

    fn header_version(encoded: &[u8]) -> Result<u32> {
        Ok(image::Header::read(Cursor::new(encoded))?.version)
    }

    #[test]
    fn convert_sparse_raw_between_lime_and_compressed_formats() -> Result<()> {
        let raw = build_sparse_raw()?;
        let lime = encode_raw(&raw, 1)?;
        assert_eq!(header_version(&lime)?, 1);
        assert_eq!(decode_to_raw(&lime)?, raw);

        let compressed = convert_encoded(&lime, true)?;
        assert_eq!(header_version(&compressed)?, 2);

        let lime_roundtrip = convert_encoded(&compressed, false)?;
        assert_eq!(header_version(&lime_roundtrip)?, 1);
        assert_eq!(lime_roundtrip, lime);

        assert_eq!(decode_to_raw(&compressed)?, raw);
        assert_eq!(decode_to_raw(&lime_roundtrip)?, raw);

        Ok(())
    }

    #[test]
    fn trailing_zero_block_is_dropped_from_raw_roundtrip() -> Result<()> {
        let mut raw = build_sparse_raw()?;
        let expected_raw = raw.clone();
        let block_size = block_size()?;
        raw.extend(vec![0; block_size]);

        let lime = encode_raw(&raw, 1)?;
        assert_eq!(decode_to_raw(&lime)?, expected_raw);

        let compressed = convert_encoded(&lime, true)?;
        let lime_roundtrip = convert_encoded(&compressed, false)?;
        assert_eq!(lime_roundtrip, lime);
        assert_eq!(decode_to_raw(&compressed)?, expected_raw);
        assert_eq!(decode_to_raw(&lime_roundtrip)?, expected_raw);

        Ok(())
    }
}
