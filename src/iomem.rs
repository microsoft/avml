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
    }
}
