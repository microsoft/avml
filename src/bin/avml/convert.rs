// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use avml::{Error, Format, Result, image};
use clap::{Parser, ValueEnum};
use snap::read::FrameDecoder;
use std::{
    fs::{File, metadata},
    io::{Read, Seek, SeekFrom, Write, copy, repeat},
    path::{Path, PathBuf},
};

#[derive(Parser)]
pub struct Args {
    /// specify input format
    #[arg(long, value_enum, default_value_t = CliFormat::LimeCompressed)]
    source_format: CliFormat,

    /// specify output format
    #[arg(long, value_enum, default_value_t = CliFormat::Lime)]
    format: CliFormat,

    /// name of the source file to read from on local system
    src: PathBuf,

    /// name of the destination file to write to on local system
    dst: PathBuf,
}

#[derive(ValueEnum, Clone, Copy, PartialEq, Eq)]
enum CliFormat {
    Raw,
    Lime,
    #[value(rename_all = "snake_case")]
    LimeCompressed,
}

pub fn run(args: &Args) -> Result<()> {
    match (args.source_format, args.format) {
        (CliFormat::Lime | CliFormat::LimeCompressed, CliFormat::Raw) => {
            convert_to_raw(&args.src, &args.dst)
        }
        (CliFormat::Lime, CliFormat::LimeCompressed) => {
            convert(&args.src, &args.dst, Format::AvmlCompressed)
        }
        (CliFormat::LimeCompressed, CliFormat::Lime) => convert(&args.src, &args.dst, Format::Lime),
        (CliFormat::Raw, CliFormat::Lime) => convert_from_raw(&args.src, &args.dst, Format::Lime),
        (CliFormat::Raw, CliFormat::LimeCompressed) => {
            convert_from_raw(&args.src, &args.dst, Format::AvmlCompressed)
        }
        (CliFormat::Lime, CliFormat::Lime)
        | (CliFormat::LimeCompressed, CliFormat::LimeCompressed)
        | (CliFormat::Raw, CliFormat::Raw) => Err(Error::NoConversionRequired),
    }
}

fn convert(src: &Path, dst: &Path, format: Format) -> Result<()> {
    let src_len = metadata(src)
        .map_err(|source| image::Error::Io {
            context: "unable to read source size",
            source,
        })?
        .len();
    let mut image = image::Image::<File, File>::new(format, src, dst)?;
    convert_image(&mut image, src_len)
}

fn convert_image<R, W>(image: &mut image::Image<R, W>, src_len: u64) -> Result<()>
where
    R: Read + Seek,
    W: Write,
{
    loop {
        let current = image
            .src
            .stream_position()
            .map_err(|source| image::Error::Io {
                context: "unable to get current offset into the memory source",
                source,
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
    loop {
        let current = image
            .src
            .stream_position()
            .map_err(|source| image::Error::Io {
                context: "unable to get the current offset into the memory source",
                source,
            })?;
        if current >= src_len {
            break;
        }
        let current_dst = image
            .dst
            .stream_position()
            .map_err(|source| image::Error::Io {
                context: "unable to get the current offset into the destination stream",
                source,
            })?;

        let header = image.read_header()?;

        let pad = header.range.start.saturating_sub(current_dst);
        if pad > 0 {
            copy(&mut repeat(0).take(pad), &mut image.dst).map_err(|source| image::Error::Io {
                context: "unable to write padding bytes",
                source,
            })?;
        }

        let size = header.size()?;

        match header.format {
            Format::Lime => {
                let mut handle =
                    (&mut image.src).take(size.try_into().map_err(image::Error::IntConversion)?);
                copy(&mut handle, &mut image.dst).map_err(|source| image::Error::Io {
                    context: "unable to copy image data",
                    source,
                })?;
            }
            Format::AvmlCompressed => {
                let mut decoder = FrameDecoder::new(&mut image.src)
                    .take(size.try_into().map_err(image::Error::IntConversion)?);
                copy(&mut decoder, &mut image.dst).map_err(|source| image::Error::Io {
                    context: "unable to copy image data",
                    source,
                })?;
                image
                    .src
                    .seek(SeekFrom::Current(8))
                    .map_err(|source| image::Error::Io {
                        context: "unable to seek past the compressed size",
                        source,
                    })?;
            }
        }
    }

    Ok(())
}

fn convert_to_raw(src: &Path, dst: &Path) -> Result<()> {
    let src_len = metadata(src)
        .map_err(|source| image::Error::Io {
            context: "unable to get source file size",
            source,
        })?
        .len();
    let mut image = image::Image::<File, File>::new(Format::Lime, src, dst)?;
    convert_to_raw_image(&mut image, src_len)
}

fn encode_raw_image<R, W>(image: &mut image::Image<R, W>, raw_len: u64) -> Result<()>
where
    R: Read + Seek,
    W: Write,
{
    let mut start = 0_u64;
    while start < raw_len {
        let end = raw_len.min(start.saturating_add(image::MAX_BLOCK_SIZE));
        image.copy_block(start..end)?;
        start = end;
    }

    Ok(())
}

fn convert_from_raw(src: &Path, dst: &Path, format: Format) -> Result<()> {
    let src_len = metadata(src)
        .map_err(|source| image::Error::Io {
            context: "unable to read source size",
            source,
        })?
        .len();
    let mut image = image::Image::<File, File>::new(format, src, dst)?;
    encode_raw_image(&mut image, src_len)
}

#[cfg(test)]
mod tests {
    use super::{convert_image, convert_to_raw_image, encode_raw_image};
    use avml::{Format, Result, image};
    use rand::{Rng as _, SeedableRng as _, rngs::SmallRng};
    use std::io::Cursor;

    fn memory_image(format: Format, src: &[u8]) -> image::Image<Cursor<&[u8]>, Cursor<Vec<u8>>> {
        image::Image::from_streams(format, Cursor::new(src), Cursor::new(Vec::new()))
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

    fn encode_raw(raw: &[u8], format: Format) -> Result<Vec<u8>> {
        let mut image = memory_image(format, raw);
        let total = u64::try_from(raw.len()).map_err(image::Error::IntConversion)?;
        encode_raw_image(&mut image, total)?;
        Ok(image.dst.into_inner())
    }

    fn convert_encoded(encoded: &[u8], format: Format) -> Result<Vec<u8>> {
        let encoded_len = u64::try_from(encoded.len()).map_err(image::Error::IntConversion)?;
        let mut image = memory_image(format, encoded);
        convert_image(&mut image, encoded_len)?;
        Ok(image.dst.into_inner())
    }

    fn decode_to_raw(encoded: &[u8]) -> Result<Vec<u8>> {
        let encoded_len = u64::try_from(encoded.len()).map_err(image::Error::IntConversion)?;
        let mut image = memory_image(Format::Lime, encoded);
        convert_to_raw_image(&mut image, encoded_len)?;
        Ok(image.dst.into_inner())
    }

    fn header_format(encoded: &[u8]) -> Result<Format> {
        Ok(image::Header::read(Cursor::new(encoded))?.format)
    }

    #[test]
    fn convert_sparse_raw_between_lime_and_compressed_formats() -> Result<()> {
        let raw = build_sparse_raw()?;
        let lime = encode_raw(&raw, Format::Lime)?;
        assert_eq!(header_format(&lime)?, Format::Lime);
        assert_eq!(decode_to_raw(&lime)?, raw);

        let compressed = convert_encoded(&lime, Format::AvmlCompressed)?;
        assert_eq!(header_format(&compressed)?, Format::AvmlCompressed);

        let lime_roundtrip = convert_encoded(&compressed, Format::Lime)?;
        assert_eq!(header_format(&lime_roundtrip)?, Format::Lime);
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

        let lime = encode_raw(&raw, Format::Lime)?;
        assert_eq!(decode_to_raw(&lime)?, expected_raw);

        let compressed = convert_encoded(&lime, Format::AvmlCompressed)?;
        let lime_roundtrip = convert_encoded(&compressed, Format::Lime)?;
        assert_eq!(lime_roundtrip, lime);
        assert_eq!(decode_to_raw(&compressed)?, expected_raw);
        assert_eq!(decode_to_raw(&lime_roundtrip)?, expected_raw);

        Ok(())
    }
}
