// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use std::io::{Result, Write};

/// Write implementation that counts the number of bytes successfully written.
pub struct Counter<W> {
    inner: W,
    count: usize,
}

impl<W> Counter<W> {
    /// Creates a new `Counter` wrapping the given writer.
    pub fn new(inner: W) -> Self {
        Self { inner, count: 0 }
    }

    /// Returns the number of bytes written to the underlying writer.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Consumes this Counter, returning the underlying writer.
    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for Counter<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let count = self.inner.write(buf)?;
        self.count = self.count.saturating_add(count);
        Ok(count)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snap::{read::FrameDecoder, write::FrameEncoder};
    use std::io::Cursor;

    #[test]
    fn encode_header() -> Result<()> {
        let data = "hello world".as_bytes();

        let buf = Cursor::new(vec![]);
        let mut counter = Counter::new(buf);

        counter.write_all(data)?;
        assert_eq!(counter.count(), data.len());
        assert_eq!(counter.into_inner().into_inner(), data);
        Ok(())
    }

    #[test]
    fn encode_snap() -> Result<()> {
        let size = 1000;
        let many_a = "A".repeat(size).into_bytes();

        let compressed_data = {
            let cursor = Cursor::new(vec![]);
            let mut counter = Counter::new(cursor);
            {
                let mut snap = FrameEncoder::new(&mut counter);
                snap.write_all(&many_a)?;
            }
            // verify we had some compression here...
            assert!(counter.count() < size);
            counter.into_inner().into_inner()
        };

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
