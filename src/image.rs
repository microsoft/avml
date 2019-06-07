// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use byteorder::{ByteOrder, LittleEndian};
use std::error::Error;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::SeekFrom;

const PAGE_SIZE: u64 = 0x1000;

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

    fn lime_header(start: u64, end: u64, version: u32) -> Result<[u8; 32], Box<dyn Error>> {
        let magic = match version {
            1 => b"EMiL",
            2 => b"AVML",
            _ => unimplemented!("unimplemented version"),
        };
        let magic = LittleEndian::read_u32(magic);

        let mut bytes = [0; 32];
        LittleEndian::write_u32_into(&[magic, version], &mut bytes[..8]);
        LittleEndian::write_u64_into(&[start, end, 0], &mut bytes[8..]);
        Ok(bytes)
    }

    pub fn handle_block(
        &mut self,
        offset: u64,
        start: u64,
        size: u64,
    ) -> Result<(), Box<dyn Error>> {
        // force memory ranges to align to page boundaries
        let mut size = (size >> 12) << 12;
        let header = Self::lime_header(start, start + size - 1, self.version)?;
        self.dst.write_all(&header)?;

        self.src.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0; PAGE_SIZE as usize];

        if self.version == 2 {
            let begin = self.dst.seek(SeekFrom::Current(0))?;
            {
                let mut snap_fh = snap::Writer::new(&self.dst);
                while size >= PAGE_SIZE {
                    self.src.read_exact(&mut buf)?;
                    snap_fh.write_all(&buf)?;
                    size -= PAGE_SIZE;
                }
            }
            let end = self.dst.seek(SeekFrom::Current(0))?;
            let mut size_bytes = [0; 8];
            LittleEndian::write_u64_into(&[end - begin], &mut size_bytes);
            self.dst.write_all(&size_bytes)?;
        } else {
            while size >= PAGE_SIZE {
                self.src.read_exact(&mut buf)?;
                self.dst.write_all(&buf)?;
                size -= PAGE_SIZE;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn make_header() {
        let expected = b"\x45\x4d\x69\x4c\x01\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\
                         \x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let header = super::Image::lime_header(0x1000, 0x20000, 1).unwrap();
        assert_eq!(header.to_vec(), expected);

        let expected = b"\x41\x56\x4d\x4c\x02\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\
                         \x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let header = super::Image::lime_header(0x1000, 0x20000, 2).unwrap();
        assert_eq!(header.to_vec(), expected);
    }
}
