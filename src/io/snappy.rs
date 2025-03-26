// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::io::counter::Counter;
use snap::write::FrameEncoder;
use std::io::{Result, Write};

pub struct SnapWriter<W: Write> {
    inner: FrameEncoder<Counter<W>>,
}

impl<W: Write> SnapWriter<W> {
    pub fn new(handle: W) -> Self {
        Self {
            inner: FrameEncoder::new(Counter::new(handle)),
        }
    }

    pub fn into_inner(mut self) -> Result<(usize, W)> {
        self.flush()?;
        let inner = self
            .inner
            .into_inner()
            .map_err(snap::write::IntoInnerError::into_error)?;
        let count = inner.count();
        Ok((count, inner.into_inner()))
    }
}

impl<W: Write> Write for SnapWriter<W> {
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
    use std::io::Cursor;

    #[test]
    fn encode_snap() -> Result<()> {
        let size = 1000;
        let many_a = "A".repeat(size).into_bytes();

        let cursor = Cursor::new(vec![]);
        let mut writer = SnapWriter::new(cursor);
        writer.write_all(&many_a)?;
        let (count, compressed_cursor) = writer.into_inner()?;
        assert!(count <= size, "{count} < {size}");

        let compressed_data = compressed_cursor.into_inner();
        assert_ne!(compressed_data, many_a);

        let result = {
            let mut compressed = Cursor::new(compressed_data);
            let decoded = Cursor::new(vec![]);
            let mut counter = Counter::new(decoded);
            {
                let mut snap = FrameDecoder::new(&mut compressed);
                std::io::copy(&mut snap, &mut counter)?;
            }
            assert_eq!(counter.count(), size, "verify decoded size");
            counter.into_inner().into_inner()
        };
        assert_eq!(many_a, result, "verify decoded byte are equal");
        Ok(())
    }
}
