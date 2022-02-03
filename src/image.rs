// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use snap::write::FrameEncoder;
use std::{
    convert::TryFrom,
    fs::{File, OpenOptions},
    io::{prelude::*, Cursor, Seek, SeekFrom},
    ops::Range,
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unable to write image")]
    Write(#[source] std::io::Error),

    #[error("unable to read memory")]
    Read(#[source] std::io::Error),

    #[error("unable to read header: {1}")]
    ReadHeader(#[source] std::io::Error, &'static str),

    #[error("invalid padding")]
    InvalidPadding,

    #[error("file is too large")]
    TooLarge,

    #[error("unimplemented version")]
    UnimplementedVersion,

    #[error("unsupported format")]
    UnsupportedFormat,

    #[error("write block failed: {0:?}")]
    WriteBlock(Range<u64>),

    #[error("size conversion error")]
    SizeConversion,
}

type Result<T> = std::result::Result<T, Error>;

pub const MAX_BLOCK_SIZE: u64 = 0x1000 * 0x1000;
const PAGE_SIZE: usize = 0x1000;
const LIME_MAGIC: u32 = 0x4c69_4d45; // EMiL as u32le
const AVML_MAGIC: u32 = 0x4c4d_5641; // AVML as u32le

#[derive(Debug, Clone)]
pub struct Header {
    pub range: Range<u64>,
    pub version: u32,
}

pub struct Block {
    pub offset: u64,
    pub range: Range<u64>,
}

impl Header {
    pub fn read(mut src: &File) -> Result<Self> {
        let magic = src
            .read_u32::<LittleEndian>()
            .map_err(|e| Error::ReadHeader(e, "magic"))?;
        let version = src
            .read_u32::<LittleEndian>()
            .map_err(|e| Error::ReadHeader(e, "version"))?;
        let start = src
            .read_u64::<LittleEndian>()
            .map_err(|e| Error::ReadHeader(e, "start offset"))?;
        let end = src
            .read_u64::<LittleEndian>()
            .map_err(|e| Error::ReadHeader(e, "end offset"))?
            + 1;
        let padding = src
            .read_u64::<LittleEndian>()
            .map_err(|e| Error::ReadHeader(e, "padding"))?;
        if padding != 0 {
            return Err(Error::InvalidPadding);
        }
        if !(magic == LIME_MAGIC && version == 1 || magic == AVML_MAGIC && version == 2) {
            return Err(Error::UnsupportedFormat);
        };

        Ok(Self {
            range: Range { start, end },
            version,
        })
    }

    fn encode(&self) -> Result<[u8; 32]> {
        let magic = match self.version {
            1 => LIME_MAGIC,
            2 => AVML_MAGIC,
            _ => return Err(Error::UnimplementedVersion),
        };
        let mut bytes = [0; 32];
        LittleEndian::write_u32_into(&[magic, self.version], &mut bytes[..8]);
        LittleEndian::write_u64_into(&[self.range.start, self.range.end - 1, 0], &mut bytes[8..]);
        Ok(bytes)
    }

    pub fn write<W>(&self, dst: &mut W) -> Result<()>
    where
        W: Write,
    {
        let bytes = self.encode()?;
        dst.write_all(&bytes).map_err(Error::Write)?;
        Ok(())
    }
}

pub fn copy<R, W>(mut size: usize, src: &mut R, dst: &mut W) -> Result<()>
where
    R: Read,
    W: Write,
{
    let mut buf = vec![0; PAGE_SIZE];
    while size >= PAGE_SIZE {
        src.read_exact(&mut buf).map_err(Error::Read)?;
        dst.write_all(&buf).map_err(Error::Write)?;
        size -= PAGE_SIZE;
    }
    if size > 0 {
        buf.resize(size, 0);
        src.read_exact(&mut buf).map_err(Error::Read)?;
        dst.write_all(&buf).map_err(Error::Write)?;
    }
    Ok(())
}

// read the entire block into memory, and only write it if it's not empty
fn copy_if_nonzero<R, W>(header: &Header, src: &mut R, mut dst: &mut W) -> Result<()>
where
    R: Read,
    W: Write + Seek,
{
    let size = usize::try_from(header.range.end - header.range.start)
        .map_err(|_| Error::SizeConversion)?;

    // read the entire block into memory, but still read page by page
    let mut buf = Cursor::new(vec![0; size]);
    copy(size, src, &mut buf)?;
    let buf = buf.into_inner();

    // if the entire block is zero, we can skip it
    if buf.iter().all(|x| x == &0) {
        return Ok(());
    }

    header.write(dst)?;
    if header.version == 1 {
        dst.write_all(&buf).map_err(Error::Write)?;
    } else {
        let begin = dst.seek(SeekFrom::Current(0)).map_err(Error::Write)?;
        {
            let mut snap_fh = FrameEncoder::new(&mut dst);
            snap_fh.write_all(&buf).map_err(Error::Write)?;
        }
        let end = dst.seek(SeekFrom::Current(0)).map_err(Error::Write)?;
        let mut size_bytes = [0; 8];
        LittleEndian::write_u64_into(&[end - begin], &mut size_bytes);
        dst.write_all(&size_bytes).map_err(Error::Write)?;
    }
    Ok(())
}

fn copy_large_block<R, W>(header: &Header, src: &mut R, mut dst: &mut W) -> Result<()>
where
    R: Read,
    W: Write + Seek,
{
    header.write(dst)?;
    let size = usize::try_from(header.range.end - header.range.start)
        .map_err(|_| Error::SizeConversion)?;

    if header.version == 1 {
        copy(size, src, dst)?;
    } else {
        let begin = dst.seek(SeekFrom::Current(0)).map_err(Error::Write)?;
        {
            let mut snap_fh = FrameEncoder::new(&mut dst);
            copy(size, src, &mut snap_fh)?;
        }
        let end = dst.seek(SeekFrom::Current(0)).map_err(Error::Write)?;
        let mut size_bytes = [0; 8];
        LittleEndian::write_u64_into(&[end - begin], &mut size_bytes);
        dst.write_all(&size_bytes).map_err(Error::Write)?;
    }
    Ok(())
}

fn copy_block_impl<R, W>(header: &Header, src: &mut R, dst: &mut W) -> Result<()>
where
    R: Read,
    W: Write + Seek,
{
    if header.range.end - header.range.start > MAX_BLOCK_SIZE {
        copy_large_block(header, src, dst)
    } else {
        copy_if_nonzero(header, src, dst)
    }
}

pub fn copy_block<R, W>(mut header: Header, src: &mut R, dst: &mut W) -> Result<()>
where
    R: Read,
    W: Write + Seek,
{
    if header.version == 2 {
        while header.range.end - header.range.start > MAX_BLOCK_SIZE {
            let range = Range {
                start: header.range.start,
                end: header.range.start + MAX_BLOCK_SIZE,
            };
            copy_block_impl(
                &Header {
                    range: range.clone(),
                    version: header.version,
                },
                src,
                dst,
            )?;
            header.range.start += MAX_BLOCK_SIZE;
        }
    }
    if header.range.end > header.range.start {
        copy_block_impl(&header, src, dst)?;
    }

    Ok(())
}

pub struct Image {
    pub version: u32,
    pub src: File,
    pub dst: File,
}

impl Image {
    pub fn new(version: u32, src_filename: &Path, dst_filename: &Path) -> Result<Self> {
        let src = OpenOptions::new()
            .read(true)
            .open(src_filename)
            .map_err(Error::Read)?;
        let dst = OpenOptions::new()
            .mode(0o600)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dst_filename)
            .map_err(Error::Write)?;

        Ok(Self { version, src, dst })
    }

    pub fn write_blocks(&mut self, blocks: &[Block]) -> Result<()> {
        for block in blocks {
            self.write_block(block)
                .map_err(|_| Error::WriteBlock(block.range.clone()))?;
        }
        Ok(())
    }

    fn write_block(&mut self, block: &Block) -> Result<()> {
        let header = Header {
            range: block.range.clone(),
            version: self.version,
        };

        if block.offset > 0 {
            self.src
                .seek(SeekFrom::Start(block.offset))
                .map_err(Error::Read)?;
        }

        copy_block(header, &mut self.src, &mut self.dst)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    #[test]
    fn encode_header() {
        let expected = b"\x45\x4d\x69\x4c\x01\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\
                         \x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let header = super::Header {
            range: Range {
                start: 0x1000,
                end: 0x20001,
            },
            version: 1,
        };
        assert_eq!(header.encode().unwrap(), *expected);

        let expected = b"\x41\x56\x4d\x4c\x02\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\
                         \x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let header = super::Header {
            range: Range {
                start: 0x1000,
                end: 0x20001,
            },
            version: 2,
        };
        assert_eq!(header.encode().unwrap(), *expected);
    }
}
