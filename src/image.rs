// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::io::snappy::SnapCountWriter;
use byteorder::{ByteOrder as _, LittleEndian, ReadBytesExt as _};
use core::ops::Range;
#[cfg(target_family = "unix")]
use libc::O_NOFOLLOW;
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
    #[error("io error: {context}")]
    Io {
        context: &'static str,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid padding")]
    InvalidPadding,

    #[error("file is too large")]
    TooLarge,

    #[error("unsupported format")]
    UnsupportedFormat,

    #[error("write block failed: {range:?}")]
    WriteBlock {
        range: Range<u64>,
        #[source]
        source: Box<Error>,
    },

    #[error(transparent)]
    IntConversion(#[from] core::num::TryFromIntError),
}

type Result<T> = core::result::Result<T, Error>;

/// On-disk format for a memory snapshot.
///
/// The wire encoding is unchanged across the two variants: a 32-bit
/// little-endian magic followed by a 32-bit little-endian version. This
/// enum exists so that internal code dispatches on a named format rather
/// than passing around bare integers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// `LiME` v1: uncompressed memory blocks with `LiME` headers.
    Lime,
    /// AVML v2: Snappy-compressed memory blocks with AVML headers.
    AvmlCompressed,
}

impl Format {
    const LIME_MAGIC: u32 = 0x4c69_4d45; // "LiME"
    const AVML_MAGIC: u32 = 0x4c4d_5641; // "AVML"

    const fn magic(self) -> u32 {
        match self {
            Self::Lime => Self::LIME_MAGIC,
            Self::AvmlCompressed => Self::AVML_MAGIC,
        }
    }

    const fn version(self) -> u32 {
        match self {
            Self::Lime => 1,
            Self::AvmlCompressed => 2,
        }
    }

    fn from_wire(magic: u32, version: u32) -> Result<Self> {
        match (magic, version) {
            (Self::LIME_MAGIC, 1) => Ok(Self::Lime),
            (Self::AVML_MAGIC, 2) => Ok(Self::AvmlCompressed),
            _ => Err(Error::UnsupportedFormat),
        }
    }
}

impl From<bool> for Format {
    /// `true` selects the compressed AVML format; `false` selects `LiME`.
    fn from(compress: bool) -> Self {
        if compress {
            Self::AvmlCompressed
        } else {
            Self::Lime
        }
    }
}

/// Largest block AVML emits in a single header. Ranges larger than this
/// are split into `MAX_BLOCK_SIZE`-sized chunks before being written.
///
/// Blocks at or below this threshold are buffered fully in memory so
/// all-zero blocks can be elided (`copy_if_nonzero`); larger blocks
/// stream straight through without zero-elision. So this constant also
/// caps the per-block buffer allocation (currently 16 MiB) and sets
/// the granularity at which zero regions are skipped.
pub const MAX_BLOCK_SIZE: u64 = 0x1000 * 0x1000;
const PAGE_SIZE: usize = 0x1000;
const HEADER_LEN: usize = 32;

#[derive(Debug, Clone)]
pub struct Header {
    pub range: Range<u64>,
    pub format: Format,
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
        let magic = src.read_u32::<LittleEndian>().map_err(|source| Error::Io {
            context: "unable to read header magic",
            source,
        })?;
        let version = src.read_u32::<LittleEndian>().map_err(|source| Error::Io {
            context: "unable to read header version",
            source,
        })?;
        let start = src.read_u64::<LittleEndian>().map_err(|source| Error::Io {
            context: "unable to read header start offset",
            source,
        })?;
        let end = src
            .read_u64::<LittleEndian>()
            .map_err(|source| Error::Io {
                context: "unable to read header end offset",
                source,
            })?
            .checked_add(1)
            .ok_or(Error::TooLarge)?;
        let padding = src.read_u64::<LittleEndian>().map_err(|source| Error::Io {
            context: "unable to read header padding",
            source,
        })?;
        if padding != 0 {
            return Err(Error::InvalidPadding);
        }
        let format = Format::from_wire(magic, version)?;

        Ok(Self {
            range: Range { start, end },
            format,
        })
    }

    fn encode(&self) -> [u8; HEADER_LEN] {
        let mut bytes = [0; HEADER_LEN];
        LittleEndian::write_u32_into(
            &[self.format.magic(), self.format.version()],
            &mut bytes[..8],
        );
        LittleEndian::write_u64_into(
            &[self.range.start, self.range.end.saturating_sub(1), 0],
            &mut bytes[8..],
        );
        bytes
    }

    /// Writes the header to the destination writer.
    ///
    /// # Errors
    /// Returns an error if the header cannot be written to the destination.
    pub fn write<W>(&self, mut dst: W) -> Result<()>
    where
        W: Write,
    {
        let bytes = self.encode();
        dst.write_all(&bytes).map_err(|source| Error::Io {
            context: "unable to write header",
            source,
        })?;
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
            src.read_exact(&mut buf).map_err(|source| Error::Io {
                context: "unable to read memory page",
                source,
            })?;
            dst.write_all(&buf).map_err(|source| Error::Io {
                context: "unable to write memory page",
                source,
            })?;
            size = size.saturating_sub(PAGE_SIZE);
        }
        if size > 0 {
            buf.resize(size, 0);
            src.read_exact(&mut buf).map_err(|source| Error::Io {
                context: "unable to read memory page",
                source,
            })?;
            dst.write_all(&buf).map_err(|source| Error::Io {
                context: "unable to write memory page",
                source,
            })?;
        }
    } else {
        let mut src = src.take(size.try_into()?);
        std::io::copy(&mut src, &mut dst).map_err(|source| Error::Io {
            context: "unable to copy memory pages",
            source,
        })?;
    }
    Ok(())
}

pub struct Image<R: Read + Seek, W: Write> {
    pub(crate) format: Format,
    pub(crate) align_src: bool,
    pub src: R,
    pub dst: W,
}

impl<R: Read + Seek, W: Write> Image<R, W> {
    /// Build an `Image` over arbitrary streams.
    pub fn from_streams(format: Format, src: R, dst: W) -> Self {
        Self {
            format,
            align_src: false,
            src,
            dst,
        }
    }

    /// The destination format this `Image` writes.
    #[must_use]
    pub fn format(&self) -> Format {
        self.format
    }

    #[cfg(target_family = "windows")]
    fn open_dst(path: &Path) -> Result<File> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|source| Error::Io {
                context: "unable to create snapshot file",
                source,
            })
    }

    #[cfg(target_family = "unix")]
    fn open_dst(path: &Path) -> Result<File> {
        OpenOptions::new()
            .mode(0o600)
            .custom_flags(O_NOFOLLOW)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|source| Error::Io {
                context: "unable to create snapshot file",
                source,
            })
    }

    /// Open `src_filename` for reading and `dst_filename` for writing,
    /// producing an `Image` that emits the given destination `format`.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The source file cannot be opened for reading
    /// - The destination file cannot be created or opened for writing
    pub fn new(
        format: Format,
        src_filename: &Path,
        dst_filename: &Path,
    ) -> Result<Image<File, File>> {
        let src_filename = canonicalize(src_filename).map_err(|source| Error::Io {
            context: "unable to canonicalize path",
            source,
        })?;
        let align_src = [
            Path::new("/dev/crash"),
            Path::new("/dev/mem"),
            Path::new("/proc/kcore"),
        ]
        .contains(&src_filename.as_path());

        let src = OpenOptions::new()
            .read(true)
            .open(&src_filename)
            .map_err(|source| Error::Io {
                context: "unable to open memory source",
                source,
            })?;

        let dst = Self::open_dst(dst_filename)?;

        Ok(Image::<File, File> {
            format,
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
            self.write_block(block).map_err(|e| Error::WriteBlock {
                range: block.range.clone(),
                source: Box::new(e),
            })?;
        }
        Ok(())
    }

    fn write_block(&mut self, block: &Block) -> Result<()> {
        if block.offset > 0 {
            self.src
                .seek(SeekFrom::Start(block.offset))
                .map_err(|source| Error::Io {
                    context: "unable to see to page",
                    source,
                })?;
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
            format: self.format,
        }
        .write(&mut self.dst)
    }

    /// Copies a memory block from the source reader to the destination writer.
    ///
    /// Ranges larger than `MAX_BLOCK_SIZE` are split into `MAX_BLOCK_SIZE`
    /// chunks. The chunk granularity determines how aggressively zero
    /// regions are elided (`copy_if_nonzero`) and caps the per-chunk
    /// in-memory buffer.
    ///
    /// This applies to both v1 (`LiME`) and v2 (AVML compressed) output. For
    /// v1 specifically, a single iomem range larger than `MAX_BLOCK_SIZE`
    /// now produces multiple `LiME` records in the output instead of one.
    /// The `LiME` format is a sequence of records walked in order, and
    /// existing v1 output already contains gaps between iomem ranges, so
    /// readers that handle the existing inter-range gaps will handle the
    /// new intra-range gaps the same way. Tools that assume a strict
    /// one-record-per-iomem-range mapping will see a behavior change.
    ///
    /// # Errors
    /// Returns an error if:
    /// - Reading from the source fails
    /// - Writing to the destination fails
    /// - Size conversion from u64 to usize fails
    pub fn copy_block(&mut self, range: Range<u64>) -> Result<()>
    where
        R: Read,
        W: Write,
    {
        let mut start = range.start;
        while start < range.end {
            let end = range
                .end
                .min(start.checked_add(MAX_BLOCK_SIZE).ok_or(Error::TooLarge)?);
            self.copy_if_nonzero(start..end)?;
            start = end;
        }
        Ok(())
    }

    // read the entire block into memory, and only write it if it's not empty.
    //
    // Caller (`copy_block`) guarantees `range_len(range) <= MAX_BLOCK_SIZE`,
    // which bounds the in-memory allocation.
    fn copy_if_nonzero(&mut self, range: Range<u64>) -> Result<()> {
        let size = range_usize(range.clone())?;

        // read the entire block into memory, but still read page by page
        let mut buf = Cursor::new(vec![0; size]);
        copy(size, self.align_src, &mut self.src, &mut buf)?;
        let buf = buf.into_inner();

        // if the entire block is zero, we can skip it
        if buf.iter().all(|x| x == &0) {
            return Ok(());
        }

        self.write_header(range.clone())?;
        match self.format {
            Format::Lime => {
                self.dst.write_all(&buf).map_err(|source| Error::Io {
                    context: "unable to write non-zero block",
                    source,
                })?;
            }
            Format::AvmlCompressed => {
                let mut encoder = SnapCountWriter::new(&mut self.dst);
                encoder.write_all(&buf).map_err(|source| Error::Io {
                    context: "unable to write compressed block",
                    source,
                })?;
                encoder.finalize().map_err(|source| Error::Io {
                    context: "unable to finalize compressed block",
                    source,
                })?;
            }
        }
        Ok(())
    }

    pub fn convert_block(&mut self) -> Result<()> {
        let header = self.read_header()?;
        match header.format {
            Format::Lime => {
                self.copy_block(header.range)?;
            }
            Format::AvmlCompressed => {
                self.write_header(header.range.clone())?;
                {
                    let size = range_len(header.range.clone());
                    let mut decoder = FrameDecoder::new(&mut self.src).take(size);
                    std::io::copy(&mut decoder, &mut self.dst).map_err(|source| Error::Io {
                        context: "unable to copy compressed data",
                        source,
                    })?;
                }
                self.src
                    .seek(SeekFrom::Current(8))
                    .map_err(|source| Error::Io {
                        context: "unable to seek passed compressed len",
                        source,
                    })?;
            }
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
    use super::{Format, Header, Image};
    use core::ops::Range;
    use std::io::Cursor;

    #[test]
    fn encode_header_lime() {
        let expected = b"\x45\x4d\x69\x4c\x01\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\
                         \x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let header = Header {
            range: Range {
                start: 0x1000,
                end: 0x20001,
            },
            format: Format::Lime,
        };
        assert_eq!(header.encode(), *expected);
    }

    #[test]
    fn encode_header_avml() {
        let expected = b"\x41\x56\x4d\x4c\x02\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\
                         \x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let header = Header {
            range: Range {
                start: 0x1000,
                end: 0x20001,
            },
            format: Format::AvmlCompressed,
        };
        assert_eq!(header.encode(), *expected);
    }

    #[test]
    fn copy_block_skips_all_zero_ranges() -> super::Result<()> {
        for format in [Format::Lime, Format::AvmlCompressed] {
            let src = Cursor::new(vec![0; 0x4000]);
            let dst = Cursor::new(vec![]);
            let mut image = Image::from_streams(format, src, dst);
            image.copy_block(0..0x4000)?;
            assert!(image.dst.get_ref().is_empty());
        }

        Ok(())
    }
}
