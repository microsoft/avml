// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use std::error::Error;
use std::fs::OpenOptions;
use std::io::prelude::*;

/// Parse /proc/iomem and return System RAM memory ranges
pub fn parse(path: &str) -> Result<Vec<std::ops::Range<u64>>, Box<dyn Error>> {
    let mut f = OpenOptions::new().read(true).open(path)?;
    let mut buffer = String::new();
    f.read_to_string(&mut buffer)?;

    let mut ranges = Vec::new();
    for line in buffer.split_terminator('\n') {
        if line.starts_with(" ") {
            continue;
        }
        if !line.ends_with(" : System RAM") {
            continue;
        }
        let mut line1 = line
            .split_terminator(' ')
            .next()
            .expect("invalid iomem line")
            .split_terminator('-');
        let start = line1.next().expect("invalid range");
        let end = line1.next().expect("invalid range end");
        let start = u64::from_str_radix(start, 16)?;
        let end = u64::from_str_radix(end, 16)?;
        if start == 0 && end == 0 {
            return Err(From::from("Need CAP_SYS_ADMIN to read /proc/iomem"));
        }
        ranges.push(start..end);
    }

    Ok(ranges)
}

#[cfg(test)]
mod tests {
    #[test]
    fn parse_iomem() {
        let ranges = super::parse("test/iomem.txt").unwrap();
        let expected = [
            4096..654_335,
            1_048_576..1_073_676_287,
            4_294_967_296..6_979_321_855,
        ];
        assert_eq!(ranges, expected);

        let ranges = super::parse("test/iomem-2.txt").unwrap();
        let expected = [
            4096..655_359,
            1_048_576..1_055_838_207,
            1_056_026_624..1_073_328_127,
            1_073_737_728..1_073_741_823,
            4_294_967_296..6_979_321_855,
        ];
        assert_eq!(ranges, expected);

        let ranges = super::parse("test/iomem-3.txt").unwrap();
        let expected = [
            65_536..649_215,
            1_048_576..2_146_303_999,
            2_146_435_072..2_147_483_647,
        ];
        assert_eq!(ranges, expected);

        let ranges = super::parse("test/iomem-4.txt").unwrap();
        let expected = [
            4096..655359,
            1048576..1423523839,
            1423585280..1511186431,
            1780150272..1818623999,
            1818828800..1843613695,
            2071535616..2071986175,
            4294967296..414464344063,
        ];
        assert_eq!(ranges, expected);
    }
}
