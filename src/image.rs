// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use anyhow::{bail, Context, Result};
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use snap::write::FrameEncoder;
use std::{
    convert::TryFrom,
    fs::{File, OpenOptions},
    io::{prelude::*, Seek, SeekFrom},
    ops::Range,
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

const PAGE_SIZE: usize = 0x1000;
const LIME_MAGIC: u32 = 0x4c69_4d45; // EMiL as u32le
const AVML_MAGIC: u32 = 0x4c4d_5641; // AVML as u32le

#[derive(Debug, Clone)]
pub struct Header {
    pub range: Range<u64>,
    pub version: u32,
}

impl Header {
    pub fn read(mut src: &File) -> Result<Self> {
        let magic = src
            .read_u32::<LittleEndian>()
            .context("unable to read magic")?;
        let version = src
            .read_u32::<LittleEndian>()
            .context("unable to read version")?;
        let start = src
            .read_u64::<LittleEndian>()
            .context("unable to read start")?;
        let end = src
            .read_u64::<LittleEndian>()
            .context("unable to read end")?
            + 1;
        let padding = src
            .read_u64::<LittleEndian>()
            .context("unable to read padding")?;
        if padding != 0 {
            bail!("invalid padding: {}", padding);
        }
        if !(magic == LIME_MAGIC && version == 1 || magic == AVML_MAGIC && version == 2) {
            bail!("unknown format");
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
            _ => bail!("unimplemented version"),
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
        dst.write_all(&bytes)?;
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
        src.read_exact(&mut buf)?;
        dst.write_all(&buf)?;
        size -= PAGE_SIZE;
    }
    if size > 0 {
        buf.resize(size, 0);
        src.read_exact(&mut buf)?;
        dst.write_all(&buf)?;
    }
    Ok(())
}

fn copy_block_impl<R, W>(header: &Header, src: &mut R, mut dst: &mut W) -> Result<()>
where
    R: Read,
    W: Write + Seek,
{
    header.write(dst)?;
    let size = usize::try_from(header.range.end - header.range.start)
        .context("unable to create image range size")?;
    if header.version == 1 {
        copy(size, src, dst)?;
    } else {
        let begin = dst
            .seek(SeekFrom::Current(0))
            .context("unable to seek to location")?;
        {
            let mut snap_fh = FrameEncoder::new(&mut dst);
            copy(size, src, &mut snap_fh).context("copy failed")?;
        }
        let end = dst.seek(SeekFrom::Current(0)).context("seek failed")?;
        let mut size_bytes = [0; 8];
        LittleEndian::write_u64_into(&[end - begin], &mut size_bytes);
        dst.write_all(&size_bytes)
            .context("write_all of size failed")?;
    }
    Ok(())
}

pub fn copy_block<R, W>(mut header: Header, src: &mut R, dst: &mut W) -> Result<()>
where
    R: Read,
    W: Write + Seek,
{
    if header.version == 2 {
        let max_size =
            u64::try_from(100 * 256 * PAGE_SIZE).context("unable to create image range size")?;
        while header.range.end - header.range.start > max_size {
            let range = Range {
                start: header.range.start,
                end: header.range.start + max_size,
            };
            copy_block_impl(
                &Header {
                    range: range.clone(),
                    version: header.version,
                },
                src,
                dst,
            )
            .with_context(|| format!("unable to copy block: {:?}", range))?;
            header.range.start += max_size;
        }
    }
    if header.range.end > header.range.start {
        copy_block_impl(&header, src, dst)
            .with_context(|| format!("unable to copy block: {:?}", header.range))?;
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
            .with_context(|| format!("unable to open source file: {}", src_filename.display()))?;
        let dst = OpenOptions::new()
            .mode(0o600)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dst_filename)
            .with_context(|| format!("unable to destination image: {}", dst_filename.display()))?;
        Ok(Self { version, src, dst })
    }

    pub fn write_block(&mut self, offset: u64, range: Range<u64>) -> Result<()> {
        let header = Header {
            range: range.clone(),
            version: self.version,
        };

        if offset > 0 {
            self.src
                .seek(SeekFrom::Start(offset))
                .with_context(|| format!("unable to seek to block: {}", offset))?;
        }

        copy_block(header, &mut self.src, &mut self.dst)
            .with_context(|| format!("unable to copy block: {:?}", range))?;
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
