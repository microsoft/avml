// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use std::{fs::OpenOptions, io::prelude::*, ops::Range, path::Path};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unable to read from /proc/iomem")]
    Io(#[from] std::io::Error),

    #[error("unable to parse value")]
    Parse(#[from] std::num::ParseIntError),

    #[error("unable to parse line: {0}")]
    ParseLine(String),

    #[error("need CAP_SYS_ADMIN to read /proc/iomem")]
    PermissionDenied,
}

/// Parse /proc/iomem and return System RAM memory ranges
pub fn parse() -> Result<Vec<Range<u64>>, Error> {
    parse_file(Path::new("/proc/iomem"))
}

fn parse_file(path: &Path) -> Result<Vec<Range<u64>>, Error> {
    let mut f = OpenOptions::new().read(true).open(path)?;
    let mut buffer = String::new();
    f.read_to_string(&mut buffer)?;

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

        ranges.push(start..end);
    }

    Ok(merge_ranges(ranges))
}

#[must_use]
pub fn merge_ranges(mut ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
    let mut result = vec![];
    ranges.sort_unstable_by_key(|r| r.start);

    while !ranges.is_empty() {
        let mut range = ranges.remove(0);

        #[allow(clippy::indexing_slicing)]
        while !ranges.is_empty() && range.end >= ranges[0].start {
            let next = ranges.remove(0);
            range = range.start..next.end;
        }

        result.push(range);
    }

    result
}

#[must_use]
pub fn split_ranges(ranges: Vec<Range<u64>>, max_size: u64) -> Vec<Range<u64>> {
    let mut result = vec![];

    for mut range in ranges {
        while range.end - range.start > max_size {
            result.push(Range {
                start: range.start,
                end: range.start + max_size,
            });
            range.start += max_size;
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

    #[test]
    fn test_merge_ranges() {
        let result = merge_ranges(vec![0..3, 3..6, 7..10, 12..15]);
        let expected = [0..6, 7..10, 12..15];
        assert_eq!(result, expected);

        let result = merge_ranges(vec![0..3, 3..6, 6..10]);
        let expected = [0..10];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_split_ranges() {
        let result = split_ranges(vec![0..30], 10);
        let expected = [0..10, 10..20, 20..30];
        assert_eq!(result, expected);

        let result = split_ranges(vec![0..30], 7);
        let expected = [0..7, 7..14, 14..21, 21..28, 28..30];
        assert_eq!(result, expected);

        let result = split_ranges(vec![0..10, 10..20, 20..30], 7);
        let expected = [0..7, 7..10, 10..17, 17..20, 20..27, 27..30];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_iomem() -> Result<(), Error> {
        for (filename, expected) in [
            (
                Path::new("test/iomem.txt"),
                vec![
                    4096..654_335,
                    1_048_576..1_073_676_287,
                    4_294_967_296..6_979_321_855,
                ],
            ),
            (
                Path::new("test/iomem-2.txt"),
                vec![
                    4096..655_359,
                    1_048_576..1_055_838_207,
                    1_056_026_624..1_073_328_127,
                    1_073_737_728..1_073_741_823,
                    4_294_967_296..6_979_321_855,
                ],
            ),
            (
                Path::new("test/iomem-3.txt"),
                vec![
                    65_536..649_215,
                    1_048_576..2_146_303_999,
                    2_146_435_072..2_147_483_647,
                ],
            ),
            (
                Path::new("test/iomem-4.txt"),
                vec![
                    4_096..655_359,
                    1_048_576..1_423_523_839,
                    1_423_585_280..1_511_186_431,
                    1_780_150_272..1_818_623_999,
                    1_818_828_800..1_843_613_695,
                    2_071_535_616..2_071_986_175,
                    4_294_967_296..414_464_344_063,
                ],
            ),
            (
                Path::new("test/iomem-5.txt"),
                vec![
                    4_096..655_359,
                    1_048_576..175_058_967,
                    175_058_968..175_067_223,
                    175_067_224..175_071_255,
                    175_071_256..175_077_463,
                    175_077_464..175_079_447,
                    175_079_448..175_087_703,
                    175_087_704..175_091_735,
                    175_091_736..175_124_567,
                    175_124_568..241_524_735,
                    241_643_520..251_310_079,
                    251_326_464..251_383_807,
                    251_424_768..264_671_231,
                    264_675_328..267_280_383,
                    267_739_136..267_866_111,
                    267_870_208..3_221_225_471,
                    4_294_967_296..13_958_643_711,
                ],
            ),
        ] {
            let ranges = parse_file(filename)?;
            assert_eq!(ranges, expected);
        }

        Ok(())
    }
}
