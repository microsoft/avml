// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use std::error::Error;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::ops::Range;

const PAGE_SIZE: usize = 0x1000;
const LIME_MAGIC: u32 = 0x4c69_4d45; // EMiL as u32le
const AVML_MAGIC: u32 = 0x4c4d_5641; // AVML as u32le

#[derive(Debug, Clone)]
pub struct Header {
    pub range: Range<u64>,
    pub version: u32,
}

impl Header {
    pub fn read(mut src: &std::fs::File) -> Result<Self, Box<dyn Error>> {
        let magic = src.read_u32::<LittleEndian>()?;
        let version = src.read_u32::<LittleEndian>()?;
        let start = src.read_u64::<LittleEndian>()?;
        let end = src.read_u64::<LittleEndian>()?;
        let padding = src.read_u64::<LittleEndian>()?;
        if padding != 0 {
            return Err(From::from(format!("invalid padding: {}", padding)));
        }
        if !(magic == LIME_MAGIC && version == 1 || magic == AVML_MAGIC && version == 2) {
            return Err(From::from("unknown format"));
        };

        Ok(Self {
            range: Range { start, end },
            version,
        })
    }

    fn encode(&self) -> Result<[u8; 32], Box<dyn Error>> {
        let magic = match self.version {
            1 => LIME_MAGIC,
            2 => AVML_MAGIC,
            _ => unimplemented!("unimplemented version"),
        };
        let mut bytes = [0; 32];
        LittleEndian::write_u32_into(&[magic, self.version], &mut bytes[..8]);
        LittleEndian::write_u64_into(&[self.range.start, self.range.end, 0], &mut bytes[8..]);
        Ok(bytes)
    }

    pub fn write<W>(&self, dst: &mut W) -> Result<(), Box<dyn Error>>
    where
        W: Write,
    {
        println!("WUT {} {}", self.range.start, self.range.end);
        let bytes = self.encode()?;
        println!("header: {:?}", bytes);
        dst.write_all(&bytes)?;
        Ok(())
    }
}

pub fn copy<R, W>(mut size: usize, src: &mut R, dst: &mut W) -> Result<(), Box<dyn Error>>
where
    R: Read,
    W: Write,
{
    let mut buf = vec![0; PAGE_SIZE as usize];
    while size >= PAGE_SIZE {
        src.read_exact(&mut buf)?;
        dst.write_all(&buf)?;
        size -= PAGE_SIZE;
    }
    Ok(())
}

pub fn copy_block<R, W>(header: &Header, src: &mut R, mut dst: &mut W) -> Result<(), Box<dyn Error>>
where
    R: Read,
    W: Write + std::io::Seek,
{
    header.write(dst)?;
    let size = 1 + (header.range.end - header.range.start);
    if header.version == 1 {
        copy(size as usize, src, dst)?;
    } else {
        let begin = dst.seek(SeekFrom::Current(0))?;
        {
            let mut snap_fh = snap::Writer::new(&mut dst);
            copy(size as usize, src, &mut snap_fh)?;
        }
        let end = dst.seek(SeekFrom::Current(0))?;
        let mut size_bytes = [0; 8];
        LittleEndian::write_u64_into(&[end - begin], &mut size_bytes);
        dst.write_all(&size_bytes)?;
    }
    Ok(())
}

pub struct Image {
    pub version: u32,
    pub src: std::fs::File,
    pub dst: std::fs::File,
}

impl Image {
    pub fn new(
        version: u32,
        src_filename: &str,
        dst_filename: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let src = OpenOptions::new().read(true).open(src_filename)?;
        let dst = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(dst_filename)?;
        Ok(Self { version, src, dst })
    }

    pub fn write_block(
        &mut self,
        offset: u64,
        mut range: Range<u64>,
    ) -> Result<(), Box<dyn Error>> {
        range.end -= 1;
        let header = Header {
            range,
            version: self.version,
        };

        if offset > 0 {
            self.src.seek(SeekFrom::Start(offset))?;
        }

        copy_block(&header, &mut self.src, &mut self.dst)?;
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
                end: 0x20000,
            },
            version: 1,
        };
        assert_eq!(header.encode().unwrap(), *expected);

        let expected = b"\x41\x56\x4d\x4c\x02\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\
                         \x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let header = super::Header {
            range: Range {
                start: 0x1000,
                end: 0x20000,
            },
            version: 2,
        };
        assert_eq!(header.encode().unwrap(), *expected);
    }
}
