// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#[cfg(target_family = "unix")]
use crate::disk_usage;
use crate::{
    errors::format_error,
    image::{Block, Image},
};
use clap::ValueEnum;
use core::{
    fmt::{Debug as FmtDebug, Display as FmtDisplay, Formatter, Result as FmtResult},
    num::NonZeroU64,
    ops::Range,
};
use elf::{abi::PT_LOAD, endian::NativeEndian, segment::ProgramHeader};
#[cfg(not(target_family = "unix"))]
use std::env::consts::OS;
use std::io::{Read, Seek, Write};
use std::{
    fs::{File, OpenOptions, metadata},
    path::{Path, PathBuf},
};

#[derive(thiserror::Error)]
pub enum Error {
    #[error("unable to parse elf structures: {0}")]
    Elf(elf::ParseError),

    #[error("locked down /proc/kcore")]
    LockedDownKcore,

    #[error(
        "estimated usage exceeds specified bounds: estimated size:{estimated} bytes. allowed:{allowed} bytes"
    )]
    DiskUsageEstimateExceeded { estimated: u64, allowed: u64 },

    #[error("unable to create memory snapshot")]
    UnableToCreateMemorySnapshot(#[from] crate::image::Error),

    #[error("unable to create memory snapshot from source: {1}")]
    UnableToCreateSnapshotFromSource(#[source] Box<Error>, Source),

    #[error("unable to create memory snapshot: {0}")]
    UnableToCreateSnapshot(String),

    #[error("{0}: {1}")]
    Other(&'static str, String),

    #[error("disk error")]
    Disk(#[source] std::io::Error),
}

impl FmtDebug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        format_error(self, f)
    }
}

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Clone, ValueEnum)]
pub enum Source {
    /// Provides a read-only view of physical memory.  Access to memory using
    /// this device must be paged aligned and read one page at a time.
    ///
    /// On RHEL based distributions, this device is frequently provided by
    /// default.  A loadable kernel module version is available as part of
    /// the Linux utility `crash`:
    /// <https://github.com/crash-utility/crash/tree/master/memory_driver>
    #[value(name = "/dev/crash")]
    DevCrash,

    /// Provides a read-write view of physical memory, though AVML opens it in a
    /// read-only fashion.  Access to to memory using this device can be
    /// disabled using the kernel configuration options `CONFIG_STRICT_DEVMEM`
    /// or `CONFIG_IO_STRICT_DEVMEM`.
    ///
    /// With `CONFIG_STRICT_DEVMEM`, only the first 1MB of memory can be
    /// accessed.
    #[value(name = "/dev/mem")]
    DevMem,

    /// Provides a virtual ELF coredump of kernel memory.  This can be used to
    /// access physical memory.
    ///
    /// If `LOCKDOWN_KCORE` is set in the kernel, then /proc/kcore may exist but
    /// is either inaccessible or doesn't allow access to all of the kernel
    /// memory.
    #[value(name = "/proc/kcore")]
    ProcKcore,

    /// User-specified path to a raw memory file
    #[value(skip)]
    Raw(PathBuf),
}

impl FmtDisplay for Source {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match *self {
            Self::DevCrash => write!(f, "/dev/crash"),
            Self::DevMem => write!(f, "/dev/mem"),
            Self::ProcKcore => write!(f, "/proc/kcore"),
            Self::Raw(ref path) => write!(f, "{}", path.display()),
        }
    }
}

#[must_use]
fn can_open(src: &Path) -> bool {
    OpenOptions::new().read(true).open(src).is_ok()
}

// The file /proc/kcore is a pseudo-file in ELF core format that is 4KB+physical
// memory in size.
//
// If LOCKDOWN_KCORE is set in the kernel, then /proc/kcore may exist but is
// either inaccessible or doesn't allow access to all of the kernel memory.
//
// /dev/mem and /dev/crash, if available, are devices, rather than virtual
// files.  As such, we don't check those for size.
#[must_use]
fn is_kcore_ok() -> bool {
    metadata(Path::new("/proc/kcore"))
        .map(|x| x.len() > 0x2000)
        .unwrap_or(false)
        && can_open(Path::new("/proc/kcore"))
}

// try to perform an action, either returning on success, or having the result
// of the error in an indented string.
//
// This special cases `DiskUsageEstimateExceeded` errors, as we want this to
// fail fast and bail out of the `try_method` caller.
macro_rules! try_method {
    ($func:expr) => {{
        match $func {
            Ok(x) => return Ok(x),
            Err(err) => {
                if let Error::UnableToCreateSnapshotFromSource(ref x, _) = err {
                    if let Error::DiskUsageEstimateExceeded { .. } = **x {
                        return Err(err);
                    }
                }
                crate::indent(format!("{:?}", err), 4)
            }
        }
    }};
}

pub struct Snapshot<'a, 'b> {
    source: Option<&'b Source>,
    destination: &'a Path,
    memory_ranges: Vec<Range<u64>>,
    version: u32,
    max_disk_usage: Option<NonZeroU64>,
    max_disk_usage_percentage: Option<f64>,
}

impl<'a, 'b> Snapshot<'a, 'b> {
    /// Create a new memory snapshot.
    ///
    /// The default version implements the `LiME` format.
    #[must_use]
    pub fn new(destination: &'a Path, memory_ranges: Vec<Range<u64>>) -> Self {
        Self {
            source: None,
            destination,
            memory_ranges,
            version: 1,
            max_disk_usage: None,
            max_disk_usage_percentage: None,
        }
    }

    /// Specify the maximum disk usage to stay under as a percentage
    ///
    /// This is an estimation, calculated at start time
    #[must_use]
    pub fn max_disk_usage_percentage(self, max_disk_usage_percentage: Option<f64>) -> Self {
        Self {
            max_disk_usage_percentage,
            ..self
        }
    }

    /// Specify the maximum disk space in MB to use
    ///
    /// This is an estimation, calculated at start time
    #[must_use]
    pub fn max_disk_usage(self, max_disk_usage: Option<NonZeroU64>) -> Self {
        Self {
            max_disk_usage,
            ..self
        }
    }

    /// Specify the source for creating the snapshot
    #[must_use]
    pub fn source(self, source: Option<&'b Source>) -> Self {
        Self { source, ..self }
    }

    /// Specify the version of the snapshot format
    #[must_use]
    pub fn version(self, version: u32) -> Self {
        Self { version, ..self }
    }

    fn create_source(&self, src: &Source) -> Result<()> {
        match *src {
            Source::ProcKcore => self.kcore(),
            Source::DevCrash => self.phys(Path::new("/dev/crash")),
            Source::DevMem => self.phys(Path::new("/dev/mem")),
            Source::Raw(ref s) => self.phys(s),
        }
        .map_err(|e| Error::UnableToCreateSnapshotFromSource(Box::new(e), src.clone()))
    }

    /// Create a memory snapshot
    ///
    /// # Errors
    /// Returns an error if:
    /// - No source is available for creating the snapshot
    /// - There is a failure reading from the specified source
    /// - The estimated disk usage exceeds the specified limits
    /// - Failed to create or write to the destination file
    pub fn create(&self) -> Result<()> {
        if let Some(src) = self.source {
            self.create_source(src)?;
        } else if self.destination == Path::new("/dev/stdout") {
            // If we're writing to stdout, we can't start over if reading from a
            // source fails.  As such, we need to do more work to pick a source
            // rather than just trying all available options.
            if is_kcore_ok() {
                self.create_source(&Source::ProcKcore)?;
            } else if can_open(Path::new("/dev/crash")) {
                self.create_source(&Source::DevCrash)?;
            } else if can_open(Path::new("/dev/mem")) {
                self.create_source(&Source::DevMem)?;
            } else {
                return Err(Error::UnableToCreateSnapshot(
                    "no source available".to_string(),
                ));
            }
        } else {
            let crash_err = try_method!(self.create_source(&Source::DevCrash));
            let kcore_err = try_method!(self.create_source(&Source::ProcKcore));
            let devmem_err = try_method!(self.create_source(&Source::DevMem));

            let reason = [String::new(), crash_err, kcore_err, devmem_err].join("\n");

            return Err(Error::UnableToCreateSnapshot(crate::indent(reason, 4)));
        }

        Ok(())
    }

    // given a set of ranges from iomem and a set of Blocks derived from the
    // pseudo-elf phys section headers, derive a set of ranges that can be used
    // to create a snapshot.
    fn find_kcore_blocks(ranges: &[Range<u64>], headers: &[Block]) -> Vec<Block> {
        let mut result = vec![];

        'outer: for range in ranges {
            let mut range = range.clone();

            'inner: for header in headers {
                match (
                    header.range.contains(&range.start),
                    // TODO: ranges is currently inclusive, but not a
                    // RangeInclusive.  this should be adjusted.
                    header.range.contains(&(range.end.saturating_sub(1))),
                ) {
                    (true, true) => {
                        let block = Block {
                            offset: header
                                .offset
                                .saturating_add(range.start)
                                .saturating_sub(header.range.start),
                            range: range.clone(),
                        };

                        result.push(block);
                        continue 'outer;
                    }
                    (true, false) => {
                        let block = Block {
                            offset: header
                                .offset
                                .saturating_add(range.start)
                                .saturating_sub(header.range.start),
                            range: range.start..header.range.end,
                        };

                        result.push(block);
                        range.start = header.range.end;
                    }
                    _ => {
                        continue 'inner;
                    }
                };
            }
        }

        result
    }

    /// Check disk usage of the destination
    ///
    /// NOTE: This requires `Image` because we want to ensure this is called
    /// after the file is created.
    #[cfg(target_family = "unix")]
    fn check_disk_usage<R: Read + Seek, W: Write>(&self, _: &Image<R, W>) -> Result<()> {
        disk_usage::check(
            self.destination,
            &self.memory_ranges,
            self.max_disk_usage,
            self.max_disk_usage_percentage,
        )
    }

    /// Check disk usage of the destination
    ///
    /// On non-Unix platforms, this operation is a no-op.
    #[cfg(not(target_family = "unix"))]
    fn check_disk_usage<R: Read + Seek, W: Write>(&self, _: &Image<R, W>) -> Result<()> {
        if self.max_disk_usage.is_some() || self.max_disk_usage_percentage.is_some() {
            return Err(Error::Other(
                "unable to check disk usage on this platform",
                format!("os:{OS}"),
            ));
        }
        Ok(())
    }

    fn kcore(&self) -> Result<()> {
        if !is_kcore_ok() {
            return Err(Error::LockedDownKcore);
        }

        let mut image =
            Image::<File, File>::new(self.version, Path::new("/proc/kcore"), self.destination)?;
        self.check_disk_usage(&image)?;

        let file =
            elf::ElfStream::<NativeEndian, _>::open_stream(&mut image.src).map_err(Error::Elf)?;
        let mut segments: Vec<&ProgramHeader> = file
            .segments()
            .iter()
            .filter(|x| x.p_type == PT_LOAD)
            .collect();
        segments.sort_by(|a, b| a.p_vaddr.cmp(&b.p_vaddr));

        let first_vaddr = segments
            .first()
            .ok_or_else(|| Error::UnableToCreateSnapshot("no initial addresses".to_string()))?
            .p_vaddr;
        let first_start = self
            .memory_ranges
            .first()
            .ok_or_else(|| Error::UnableToCreateSnapshot("no initial memory range".to_string()))?
            .start;
        let start = first_vaddr.saturating_sub(first_start);

        let mut physical_ranges = vec![];

        for phdr in segments {
            let entry_start = phdr.p_vaddr.checked_sub(start).ok_or_else(|| {
                Error::UnableToCreateSnapshot("unable to calculate start address".to_string())
            })?;
            let entry_end = entry_start.checked_add(phdr.p_memsz).ok_or_else(|| {
                Error::UnableToCreateSnapshot("unable to calculate end address".to_string())
            })?;

            physical_ranges.push(Block {
                range: entry_start..entry_end,
                offset: phdr.p_offset,
            });
        }

        let blocks = Self::find_kcore_blocks(&self.memory_ranges, &physical_ranges);
        image.write_blocks(&blocks)?;
        Ok(())
    }

    fn phys(&self, mem: &Path) -> Result<()> {
        let is_crash = mem == Path::new("/dev/crash");
        let blocks = self
            .memory_ranges
            .iter()
            .map(|x| Block {
                offset: x.start,
                range: if is_crash {
                    x.start..((x.end >> 12) << 12)
                } else {
                    x.start..x.end
                },
            })
            .collect::<Vec<_>>();

        let mut image = Image::<File, File>::new(self.version, mem, self.destination)?;
        self.check_disk_usage(&image)?;

        image.write_blocks(&blocks)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translate_ranges() {
        let ranges = [10..20, 30..35, 45..55];

        let core_ranges = [
            Block {
                range: 10..20,
                offset: 0,
            },
            Block {
                range: 25..35,
                offset: 10,
            },
            Block {
                range: 40..50,
                offset: 20,
            },
            Block {
                range: 50..55,
                offset: 35,
            },
        ];

        let expected = vec![
            Block {
                offset: 0,
                range: 10..20,
            },
            Block {
                offset: 10 + 5,
                range: 30..35,
            },
            Block {
                offset: 25,
                range: 45..50,
            },
            Block {
                offset: 35,
                range: 50..55,
            },
        ];

        let result = Snapshot::find_kcore_blocks(&ranges, &core_ranges);

        assert_eq!(result, expected);
    }
}
