// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::snapshot::{Error, Result};
use std::{ffi::CString, num::NonZeroU64, ops::Range, os::unix::ffi::OsStrExt, path::Path};

/// Assume roughly 100k per block extra overhead
///
/// This should account for the header used by `LiME` chunks as well as
/// potential increase in storage from compression, as Snappy does not guarantee
/// compression results in fewer bytes.
const EXTRA_PADDING: u64 = 1024 * 100;
const EXCESSIVE_VALUE: u64 = 4_000_000_000_000_000_000;

#[derive(Debug)]
struct DiskUsage {
    pub total: u64,
    pub used: u64,
}

/// Check an *estimate* of the disk usage of a snapshot
///
/// This is a best effort attempt to estimate the disk usage of a snapshot and
/// validate the snapshot will fit in the configured parameters.
pub(crate) fn check(
    image_path: &Path,
    memory_ranges: &[Range<u64>],
    max_disk_usage: Option<NonZeroU64>,
    max_disk_usage_percentage: Option<f64>,
) -> Result<()> {
    let estimate_add = estimate(memory_ranges);

    if let Some(max_disk_usage) = max_disk_usage {
        check_max_usage(estimate_add, max_disk_usage)?;
    }

    if let Some(max_disk_usage_percentage) = max_disk_usage_percentage {
        let disk_usage = disk_usage(image_path)?;
        check_max_usage_percentage(estimate_add, &disk_usage, max_disk_usage_percentage)?;
    }

    Ok(())
}

fn check_max_usage(estimated: u64, max_disk_usage: NonZeroU64) -> Result<()> {
    // convert to MB
    let allowed = max_disk_usage.get() * 1024 * 1024;

    if estimated > allowed {
        return Err(Error::DiskUsageEstimateExceeded { estimated, allowed });
    }
    Ok(())
}

fn check_max_usage_percentage(
    estimated: u64,
    disk_usage: &DiskUsage,
    max_disk_usage_percentage: f64,
) -> Result<()> {
    let estimated_used = disk_usage.used.saturating_add(estimated);

    // assuming the disk was empty, how much could we use
    let max_allowed =
        f64_to_u64(u64_to_f64(disk_usage.total)? * (max_disk_usage_percentage / 100.0))?;

    if estimated_used > max_allowed {
        let allowed = max_allowed.saturating_sub(disk_usage.used);
        return Err(Error::DiskUsageEstimateExceeded { estimated, allowed });
    }

    Ok(())
}

/// Attempt to convert u64 into f64
///
/// Of note: The maximum value works out to be more than 4 exabytes, which is *way* more than any
/// memory or disk we're likely to see any time soon.
///
/// `TryInto<f64> for u64` is not implemented.  This tries to be mindful of the
/// following edge condition:
/// 1. The value must be less than or equal to the const `EXCESSIVE_VALUE`
#[allow(clippy::cast_precision_loss)]
/// convert u64 into f64
fn u64_to_f64(value: u64) -> Result<f64> {
    if value > EXCESSIVE_VALUE {
        return Err(Error::Other(
            "unable to convert u64 to f64",
            format!("value is too large to convert to f64: {value}"),
        ));
    }
    Ok(value as f64)
}

/// Attempt to convert f64 into u64
///
/// `TryInto<u64> for f64` is not implemented.  This tries to be mindful of the
/// following edge conditions:
/// 1. The value must be a signed positive value
/// 2. The value is explicitly truncated and clamped to the integer value
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
fn f64_to_u64(value: f64) -> Result<u64> {
    if !value.is_sign_positive() {
        return Err(Error::Other(
            "unable to convert f64 to u64",
            format!("value is not a positive f64: {value}"),
        ));
    }
    Ok(value.trunc() as u64)
}

/// Estimate potential disk usage for a given set of memory ranges
fn estimate(ranges: &[Range<u64>]) -> u64 {
    let mut total: u64 = 0;
    for range in ranges {
        let chunk_size = range.end.saturating_sub(range.start);
        total = total
            .saturating_add(chunk_size)
            .saturating_add(EXTRA_PADDING);
    }
    total
}

fn disk_usage(path: &Path) -> Result<DiskUsage> {
    let cstr = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| Error::Other("unable to convert path to CString", e.to_string()))?;

    let mut statfs: libc::statfs64 = unsafe { std::mem::zeroed() };
    unsafe {
        let ret = libc::statfs64(cstr.as_ptr(), &mut statfs);
        if ret < 0 {
            return Err(Error::Disk(std::io::Error::last_os_error()))?;
        }
    }

    let f_bsize: u64 = statfs
        .f_bsize
        .try_into()
        .map_err(|e| Error::Other("unable to identify block size", format!("{e}")))?;

    let total = statfs.f_blocks * f_bsize;
    let free = statfs.f_bavail * f_bsize;
    let used = total - free;

    let result = DiskUsage { total, used };

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXCESSIVE_VALUE_F64: f64 = 4_000_000_000_000_000_000.0;

    #[test]
    fn test_disk_usage() -> Result<()> {
        let current_exe = std::env::current_exe()
            .map_err(|e| Error::Other("unable to get current exe", e.to_string()))?;
        // check that we can get disk usage for at least one file system, here
        // we check file system that the current exe resides on
        let result = disk_usage(&current_exe)?;

        // We can't really test this, but we can at least make sure it's not zero
        assert!(result.total > 0);

        // Since we know that at least this executable should reside on this
        // disk, we can at least check we have some disk usage from that executable alone
        assert!(result.used > 0);

        Ok(())
    }

    // test f64<->u64
    //
    // This tests values up to 4EB, which is larger than we can reasonably
    // expect to see any time soon.
    #[test]
    fn test_conversion() -> Result<()> {
        // validate our assumptions that this should always reduce down to u64::MAX
        assert_eq!(f64_to_u64(f64::MAX - 1.0)?, u64::MAX);

        f64_to_u64(0.0)?;
        assert!(f64_to_u64(-0.1).is_err());
        assert_eq!(f64_to_u64(EXCESSIVE_VALUE_F64)?, EXCESSIVE_VALUE);

        // note: testing equality of floating point values is tricky.
        //
        // these follow the guidelines set by `clippy::float_cmp`
        assert!((u64_to_f64(0)? - 0.0).abs() < f64::EPSILON);
        assert!((u64_to_f64(EXCESSIVE_VALUE)? - EXCESSIVE_VALUE_F64).abs() < f64::EPSILON);

        Ok(())
    }

    #[test]
    fn test_estimates() {
        insta::assert_json_snapshot!(estimate(&[0..100, 100..200, 200..300]));
        insta::assert_json_snapshot!(estimate(&[
            0..1024 * 1024,
            (1024 * 1024) + 10..(1024 * 1024 * 1024)
        ]));
    }

    #[test]
    fn test_check_max_usable() -> Result<()> {
        let ten = NonZeroU64::new(10)
            .ok_or_else(|| Error::Other("unable to create NonZeroU64", String::new()))?;
        check_max_usage(1, ten)?;
        check_max_usage(10, ten)?;
        assert!(check_max_usage(11 * 1024 * 1024, ten).is_err());
        Ok(())
    }

    // Testing where we are using disk percentage is more difficult as we don't
    // know how much disk space is available on CICD apriori.
    //
    // Instead we have to provide pre-computed values for estimates and disk
    // usage to the underlying check function.
    #[test]
    fn test_check_max_usage_percentage() -> Result<()> {
        // usage should be well below allowed %
        check_max_usage_percentage(
            10,
            &DiskUsage {
                total: 1000,
                used: 0,
            },
            10.0,
        )?;

        // usage should just at the allowed value
        check_max_usage_percentage(
            1,
            &DiskUsage {
                total: 1000,
                used: 99,
            },
            10.0,
        )?;

        // disk is already past the max allowed, should fail even with a tiny addition
        assert!(check_max_usage_percentage(
            1,
            &DiskUsage {
                total: 1000,
                used: 910
            },
            10.0
        )
        .is_err());

        Ok(())
    }
}
