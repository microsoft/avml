// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::io::snappy::SnapCountWriter;
use byteorder::{ByteOrder as _, LittleEndian, ReadBytesExt as _};
use core::ops::Range;
use snap::read::FrameDecoder;
#[cfg(target_family = "unix")]
use std::os::unix::fs::OpenOptionsExt as _;
use std::{
    fs::{File, OpenOptions, canonicalize},
    io::{Cursor, Read, Seek, SeekFrom, Write},
    path::Path,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io error: {1}")]
    Io(#[source] std::io::Error, &'static str),

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

    #[error(transparent)]
    IntConversion(#[from] core::num::TryFromIntError),
}

type Result<T> = core::result::Result<T, Error>;

pub const MAX_BLOCK_SIZE: u64 = 0x1000 * 0x1000;
const PAGE_SIZE: usize = 0x1000;
const LIME_MAGIC: u32 = 0x4c69_4d45; // EMiL as u32le
const AVML_MAGIC: u32 = 0x4c4d_5641; // AVML as u32le

#[derive(Debug, Clone)]
pub struct Header {
    pub range: Range<u64>,
    pub version: u32,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Block {
    pub offset: u64,
    pub range: Range<u64>,
}

impl Header {
    /// Reads a header from the provided file.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The header cannot be read from the file
    /// - The magic number or version is invalid
    /// - The padding value is not zero
    pub fn read<R: Read>(mut src: R) -> Result<Self> {
        let magic = src
            .read_u32::<LittleEndian>()
            .map_err(|e| Error::Io(e, "unable to read header magic"))?;
        let version = src
            .read_u32::<LittleEndian>()
            .map_err(|e| Error::Io(e, "unable to read header version"))?;
        let start = src
            .read_u64::<LittleEndian>()
            .map_err(|e| Error::Io(e, "unable to read header start offset"))?;
        let end = src
            .read_u64::<LittleEndian>()
            .map_err(|e| Error::Io(e, "unable to read header end offset"))?
            .checked_add(1)
            .ok_or(Error::TooLarge)?;
        let padding = src
            .read_u64::<LittleEndian>()
            .map_err(|e| Error::Io(e, "unable to read header padding"))?;
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
        LittleEndian::write_u64_into(
            &[self.range.start, self.range.end.saturating_sub(1), 0],
            &mut bytes[8..],
        );
        Ok(bytes)
    }

    /// Writes the header to the destination writer.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The version is not supported
    /// - The header cannot be written to the destination
    pub fn write<W>(&self, mut dst: W) -> Result<()>
    where
        W: Write,
    {
        let bytes = self.encode()?;
        dst.write_all(&bytes)
            .map_err(|e| Error::Io(e, "unable to write header"))?;
        Ok(())
    }

    pub fn size(&self) -> Result<usize> {
        Ok(usize::try_from(
            self.range.end.saturating_sub(self.range.start),
        )?)
    }
}

/// Copies data from a source reader to a destination writer.
///
/// # Errors
/// Returns an error if:
/// - Reading from the source fails
/// - Writing to the destination fails
#[inline]
fn copy<R, W>(mut size: usize, align_src: bool, mut src: R, mut dst: W) -> Result<()>
where
    R: Read,
    W: Write,
{
    if align_src {
        let mut buf = vec![0; PAGE_SIZE];
        while size >= PAGE_SIZE {
            src.read_exact(&mut buf)
                .map_err(|e| Error::Io(e, "unable to read memory page"))?;
            dst.write_all(&buf)
                .map_err(|e| Error::Io(e, "unable to write memory page"))?;
            size = size.saturating_sub(PAGE_SIZE);
        }
        if size > 0 {
            buf.resize(size, 0);
            src.read_exact(&mut buf)
                .map_err(|e| Error::Io(e, "unable to read memory page"))?;
            dst.write_all(&buf)
                .map_err(|e| Error::Io(e, "unable to write memory page"))?;
        }
    } else {
        let mut src = src.take(size.try_into()?);
        std::io::copy(&mut src, &mut dst)
            .map_err(|e| Error::Io(e, "unable to copy memory pages"))?;
    }
    Ok(())
}

pub struct Image<R: Read + Seek, W: Write> {
    pub version: u32,
    pub align_src: bool,
    pub src: R,
    pub dst: W,
}

impl<R: Read + Seek, W: Write> Image<R, W> {
    #[cfg(target_family = "windows")]
    fn open_dst(path: &Path) -> Result<File> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| Error::Io(e, "unable to create snapshot file"))
    }

    #[cfg(target_family = "unix")]
    fn open_dst(path: &Path) -> Result<File> {
        OpenOptions::new()
            .mode(0o600)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| Error::Io(e, "unable to create snapshot file"))
    }

    /// Creates a new Image with the specified version, source filename, and destination filename.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The source file cannot be opened for reading
    /// - The destination file cannot be created or opened for writing
    pub fn new(
        version: u32,
        src_filename: &Path,
        dst_filename: &Path,
    ) -> Result<Image<File, File>> {
        let src_filename =
            canonicalize(src_filename).map_err(|e| Error::Io(e, "unable to canonicalize path"))?;
        let align_src = [
            Path::new("/dev/crash"),
            Path::new("/dev/mem"),
            Path::new("/dev/kcore"),
        ]
        .contains(&src_filename.as_path());

        let src = OpenOptions::new()
            .read(true)
            .open(&src_filename)
            .map_err(|e| Error::Io(e, "unable to open memory source"))?;

        let dst = Self::open_dst(dst_filename)?;

        Ok(Image::<File, File> {
            version,
            align_src,
            src,
            dst,
        })
    }

    /// Writes multiple memory blocks to the destination file.
    ///
    /// # Errors
    /// Returns an error if writing any block fails
    pub fn write_blocks(&mut self, blocks: &[Block]) -> Result<()> {
        for block in blocks {
            self.write_block(block)
                .map_err(|_| Error::WriteBlock(block.range.clone()))?;
        }
        Ok(())
    }

    fn write_block(&mut self, block: &Block) -> Result<()> {
        if block.offset > 0 {
            self.src
                .seek(SeekFrom::Start(block.offset))
                .map_err(|e| Error::Io(e, "unable to see to page"))?;
        }

        self.copy_block(block.range.clone())?;
        Ok(())
    }

    pub fn read_header(&mut self) -> Result<Header> {
        Header::read(&mut self.src)
    }

    fn write_header(&mut self, range: Range<u64>) -> Result<()> {
        Header {
            range,
            version: self.version,
        }
        .write(&mut self.dst)
    }

    /// Copies a memory block from the source reader to the destination writer.
    ///
    /// # Errors
    /// Returns an error if:
    /// - Reading from the source fails
    /// - Writing to the destination fails
    /// - Size conversion from u64 to usize fails
    pub fn copy_block(&mut self, mut range: Range<u64>) -> Result<()>
    where
        R: Read,
        W: Write,
    {
        if self.version == 2 {
            while range.end.saturating_sub(range.start) > MAX_BLOCK_SIZE {
                let new_range = Range {
                    start: range.start,
                    end: range
                        .start
                        .checked_add(MAX_BLOCK_SIZE)
                        .ok_or(Error::TooLarge)?,
                };
                self.copy_block_impl(new_range)?;
                range.start = range.start.saturating_add(MAX_BLOCK_SIZE);
            }
        }
        if range.end > range.start {
            self.copy_block_impl(range)?;
        }

        Ok(())
    }

    fn copy_block_impl(&mut self, range: Range<u64>) -> Result<()> {
        if range_len(range.clone()) > MAX_BLOCK_SIZE {
            self.copy_large_block(range)
        } else {
            self.copy_if_nonzero(range)
        }
    }

    fn copy_large_block(&mut self, range: Range<u64>) -> Result<()> {
        self.write_header(range.clone())?;
        let size = range_usize(range.clone())?;

        if self.version == 1 {
            copy(size, self.align_src, &mut self.src, &mut self.dst)?;
        } else {
            let mut encoder = SnapCountWriter::new(&mut self.dst);
            copy(size, self.align_src, &mut self.src, &mut encoder)?;
            encoder
                .finalize()
                .map_err(|e| Error::Io(e, "unable to finalize compressed block"))?;
        }
        Ok(())
    }

    // read the entire block into memory, and only write it if it's not empty
    fn copy_if_nonzero(&mut self, range: Range<u64>) -> Result<()> {
        self.write_header(range.clone())?;
        let size = range_usize(range.clone())?;

        // read the entire block into memory, but still read page by page
        let mut buf = Cursor::new(vec![0; size]);
        copy(size, self.align_src, &mut self.src, &mut buf)?;
        let buf = buf.into_inner();

        // if the entire block is zero, we can skip it
        if buf.iter().all(|x| x == &0) {
            return Ok(());
        }

        if self.version == 1 {
            self.dst
                .write_all(&buf)
                .map_err(|e| Error::Io(e, "unable to write non-zero block"))?;
        } else {
            let mut encoder = SnapCountWriter::new(&mut self.dst);
            encoder
                .write_all(&buf)
                .map_err(|e| Error::Io(e, "unable to write compressed block"))?;
            encoder
                .finalize()
                .map_err(|e| Error::Io(e, "unable to finalize compressed block"))?;
        }
        Ok(())
    }

    pub fn convert_block(&mut self) -> Result<()> {
        let header = self.read_header()?;
        let mut new_header = header.clone();
        new_header.version = if header.version == 1 { 2 } else { 1 };
        match header.version {
            1 => {
                self.copy_block(header.range)?;
            }
            2 => {
                self.write_header(new_header.range.clone())?;
                {
                    let size = range_len(new_header.range.clone());
                    let mut decoder = FrameDecoder::new(&mut self.src).take(size);
                    std::io::copy(&mut decoder, &mut self.dst)
                        .map_err(|e| Error::Io(e, "unable to copy compressed data"))?;
                }
                self.src
                    .seek(SeekFrom::Current(8))
                    .map_err(|e| Error::Io(e, "unable to seek passed compressed len"))?;
            }
            _ => unimplemented!(),
        }

        Ok(())
    }
}

fn range_len(value: Range<u64>) -> u64 {
    value.end.saturating_sub(value.start)
}

fn range_usize(value: Range<u64>) -> Result<usize> {
    Ok(usize::try_from(value.end.saturating_sub(value.start))?)
}

#[cfg(test)]
mod tests {
    use core::ops::Range;

    #[test]
    fn encode_header_v1() {
        let expected = b"\x45\x4d\x69\x4c\x01\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\
                         \x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let header = super::Header {
            range: Range {
                start: 0x1000,
                end: 0x20001,
            },
            version: 1,
        };
        assert!(matches!(header.encode(), Ok(x) if x == *expected));
    }

    #[test]
    fn encode_header_v2() {
        let expected = b"\x41\x56\x4d\x4c\x02\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\
                         \x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let header = super::Header {
            range: Range {
                start: 0x1000,
                end: 0x20001,
            },
            version: 2,
        };
        assert!(matches!(header.encode(), Ok(x) if x == *expected));
    }
}
