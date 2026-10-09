// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use core::{num::ParseIntError, ops::Range};
use std::{fs::read_to_string, io::Error as IoError, path::Path};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unable to read from /proc/iomem")]
    Io(#[from] IoError),
    #[error("unable to parse value")]
    Parse(#[from] ParseIntError),
    #[error("unable to parse line: {0}")]
    ParseLine(String),
    #[error("need CAP_SYS_ADMIN to read /proc/iomem")]
    PermissionDenied,
}

/// Parse /proc/iomem and return System RAM memory ranges
///
/// # Errors
/// Returns an error if:
/// - Failed to read the /proc/iomem file
/// - Failed to parse memory ranges from the file content
/// - Permission denied when trying to read /proc/iomem (requires `CAP_SYS_ADMIN`)
pub fn parse() -> Result<Vec<Range<u64>>, Error> {
    parse_file(Path::new("/proc/iomem"))
}

fn parse_file(path: &Path) -> Result<Vec<Range<u64>>, Error> {
    let buffer = read_to_string(path)?;

    let mut ranges = Vec::new();
    for line in buffer.split_terminator('\n') {
        if line.starts_with(' ') {
            continue;
        }
        if !line.ends_with(" : System RAM") {
            continue;
        }
        let mut line1 = line
            .split_terminator(' ')
            .next()
            .ok_or_else(|| Error::ParseLine("invalid iomem line".to_string()))?
            .split_terminator('-');

        let start = line1
            .next()
            .ok_or_else(|| Error::ParseLine("invalid range start".to_string()))?;
        let start = u64::from_str_radix(start, 16)?;

        let end = line1
            .next()
            .ok_or_else(|| Error::ParseLine("invalid range end".to_string()))?;
        let end = u64::from_str_radix(end, 16)?;

        if start == 0 && end == 0 {
            return Err(Error::PermissionDenied);
        }

        // /proc/iomem endpoints are inclusive; AVML uses half-open ranges.
        // u64::MAX has no representable one-past endpoint, so clamp it.
        let end = end.saturating_add(1);
        ranges.push(start..end);
    }

    Ok(merge_ranges(ranges))
}

#[must_use]
pub fn merge_ranges(mut ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
    ranges.sort_unstable_by_key(|r| r.start);

    let mut result: Vec<Range<u64>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        match result.last_mut() {
            Some(last) if last.end >= range.start => {
                last.end = last.end.max(range.end);
            }
            _ => result.push(range),
        }
    }
    result
}

#[must_use]
pub fn split_ranges(ranges: Vec<Range<u64>>, max_size: u64) -> Vec<Range<u64>> {
    let mut result = vec![];

    for mut range in ranges {
        while range.end.saturating_sub(range.start) > max_size {
            let end = range.start.saturating_add(max_size);
            result.push(Range {
                start: range.start,
                end,
            });
            range.start = end;
        }
        if !range.is_empty() {
            result.push(range);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::image::{Error as ImageError, Format, Header, Image};
    use insta::assert_json_snapshot;
    use std::{
        io::{Cursor, ErrorKind, Seek as _, Write as _},
        path::PathBuf,
    };

    fn parse_one_page() -> Result<Vec<Range<u64>>, Box<dyn std::error::Error>> {
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(b"00001000-00001fff : System RAM\n")?;
        Ok(parse_file(file.path())?)
    }

    #[test]
    fn test_merge_ranges() {
        assert_json_snapshot!(merge_ranges(vec![0..3, 3..6, 7..10, 12..15]));
        assert_json_snapshot!(merge_ranges(vec![0..3, 3..6, 6..10]));
    }

    #[test]
    fn test_split_ranges() {
        assert_json_snapshot!(split_ranges(vec![0..30; 1], 10));
        assert_json_snapshot!(split_ranges(vec![0..30; 1], 7));
        assert_json_snapshot!(split_ranges(vec![0..10, 10..20, 20..30], 7));
    }

    #[test]
    fn test_parse_inclusive_end_as_exclusive() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(parse_one_page()?, vec![0x1000..0x2000]);
        Ok(())
    }

    #[test]
    fn test_parse_preserves_final_nonzero_byte() -> Result<(), Box<dyn std::error::Error>> {
        let mut page = vec![0_u8; 4096];
        *page.last_mut().ok_or("empty page")? = 0xa5;
        let mut image =
            Image::from_streams(Format::Lime, Cursor::new(page), Cursor::new(Vec::new()));
        image.align_src = true;

        for range in parse_one_page()? {
            image.copy_block(range)?;
        }

        let output = image.dst.into_inner();
        assert_eq!(output.len(), 32 + 4096);
        assert_eq!(output.last(), Some(&0xa5));
        Ok(())
    }

    #[test]
    fn test_page_sized_iomem_range_requires_one_full_page() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut iomem = tempfile::NamedTempFile::new()?;
        iomem.write_all(b"00000000-00000fff : System RAM\n")?;
        let ranges = parse_file(iomem.path())?;
        assert_eq!(ranges, vec![0..4096]);

        for source_size in [4095_usize, 4096, 4097] {
            let mut source_file = tempfile::NamedTempFile::new()?;
            let source = (0_u8..=u8::MAX)
                .cycle()
                .take(source_size)
                .collect::<Vec<_>>();
            source_file.write_all(&source)?;
            for format in [Format::Lime, Format::AvmlCompressed] {
                let mut image =
                    Image::from_streams(format, source_file.reopen()?, Cursor::new(Vec::new()));
                image.align_src = true;

                let result = image.copy_block(ranges.first().ok_or("missing range")?.clone());
                if source_size == 4095 {
                    assert!(matches!(
                        result,
                        Err(ImageError::Io {
                            context: "unable to read memory page",
                            source: io_error
                        }) if io_error.kind() == ErrorKind::UnexpectedEof
                    ));
                    let expected: &[u8] = &[];
                    assert_eq!(image.dst.get_ref().as_slice(), expected);
                    continue;
                }

                result?;
                assert_eq!(image.src.stream_position()?, 4096);
                let mut output = image.dst.into_inner();
                if format == Format::AvmlCompressed {
                    let mut converter = Image::from_streams(
                        Format::Lime,
                        Cursor::new(output),
                        Cursor::new(Vec::new()),
                    );
                    converter.convert_block()?;
                    output = converter.dst.into_inner();
                }
                assert_eq!(output.len(), 32 + 4096);
                assert_eq!(Header::read(Cursor::new(&output))?.range, 0..4096);
                assert_eq!(output.get(32..), source.get(..4096));
            }
        }

        Ok(())
    }

    #[test]
    fn test_parse_clamps_unrepresentable_exclusive_end() -> Result<(), Box<dyn std::error::Error>> {
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(b"0000000000000001-ffffffffffffffff : System RAM\n")?;

        assert_eq!(parse_file(file.path())?, vec![1..u64::MAX]);
        Ok(())
    }

    #[test]
    fn test_parse_iomem() -> Result<(), Error> {
        let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test");
        for (filename, expected) in [
            (
                "iomem.txt",
                vec![
                    4096..654_336,
                    1_048_576..1_073_676_288,
                    4_294_967_296..6_979_321_856,
                ],
            ),
            (
                "iomem-2.txt",
                vec![
                    4096..655_360,
                    1_048_576..1_055_838_208,
                    1_056_026_624..1_073_328_128,
                    1_073_737_728..1_073_741_824,
                    4_294_967_296..6_979_321_856,
                ],
            ),
            (
                "iomem-3.txt",
                vec![
                    65_536..649_216,
                    1_048_576..2_146_304_000,
                    2_146_435_072..2_147_483_648,
                ],
            ),
            (
                "iomem-4.txt",
                vec![
                    4_096..655_360,
                    1_048_576..1_423_523_840,
                    1_423_585_280..1_511_186_432,
                    1_780_150_272..1_818_624_000,
                    1_818_828_800..1_843_613_696,
                    2_071_535_616..2_071_986_176,
                    4_294_967_296..414_464_344_064,
                ],
            ),
            (
                "iomem-5.txt",
                vec![
                    4_096..655_360,
                    1_048_576..241_524_736,
                    241_643_520..251_310_080,
                    251_326_464..251_383_808,
                    251_424_768..264_671_232,
                    264_675_328..267_280_384,
                    267_739_136..267_866_112,
                    267_870_208..3_221_225_472,
                    4_294_967_296..13_958_643_712,
                ],
            ),
        ] {
            let ranges = parse_file(&fixtures.join(filename))?;
            assert_eq!(ranges, expected);
        }

        Ok(())
    }
}
