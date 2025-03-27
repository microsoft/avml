// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::io::counter::Counter;
use snap::write::FrameEncoder;
use std::io::{Result, Write};

pub struct SnapCountWriter<W: Write> {
    inner: FrameEncoder<Counter<W>>,
}

impl<W: Write> SnapCountWriter<W> {
    pub fn new(handle: W) -> Self {
        Self {
            inner: FrameEncoder::new(Counter::new(handle)),
        }
    }

    pub fn finalize(mut self) -> Result<()> {
        self.flush()?;
        let inner = self
            .inner
            .into_inner()
            .map_err(snap::write::IntoInnerError::into_error)?;

        let count = u64::try_from(inner.count()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unable to convert compressed length to u64",
            )
        })?;
        let mut handle = inner.into_inner();
        handle.write_all(&count.to_le_bytes())?;

        Ok(())
    }
}

impl<W: Write> Write for SnapCountWriter<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snap::read::FrameDecoder;
    use std::io::{Cursor, copy};

    #[test]
    fn encode_snap() -> Result<()> {
        let size = 1000;
        let many_a = "A".repeat(size).into_bytes();

        let mut compressed_data = Vec::new();
        {
            let cursor = Cursor::new(&mut compressed_data);
            let mut writer = SnapCountWriter::new(cursor);
            writer.write_all(&many_a)?;
            writer.finalize()?;
        }

        let compressed_len = compressed_data.split_off(compressed_data.len() - 8);
        let compressed_len = u64::from_le_bytes(compressed_len.try_into().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unable to convert compressed length to u64",
            )
        })?);
        assert_eq!(
            compressed_len,
            u64::try_from(compressed_data.len()).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unable to convert compressed length to u64",
                )
            })?
        );

        assert_ne!(compressed_data, many_a);

        let result = {
            let mut compressed = Cursor::new(compressed_data);
            let decoded = Cursor::new(vec![]);
            let mut counter = Counter::new(decoded);
            {
                let mut snap = FrameDecoder::new(&mut compressed);
                copy(&mut snap, &mut counter)?;
            }
            assert_eq!(counter.count(), size, "verify decoded size");
            counter.into_inner().into_inner()
        };
        assert_eq!(many_a, result, "verify decoded byte are equal");
        Ok(())
    }
}
