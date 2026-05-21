// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#[cfg(target_family = "unix")]
use crate::disk_usage;
use crate::{
    errors::format_error,
    image::{Block, Format, Image},
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
use std::{
    fs::{File, OpenOptions, metadata},
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
};

#[derive(thiserror::Error)]
pub enum Error {
    #[error("unable to parse elf structures: {0}")]
    Elf(#[from] elf::ParseError),

    #[error("locked down /proc/kcore")]
    LockedDownKcore,

    #[error(
        "estimated usage exceeds specified bounds: estimated size:{estimated} bytes. allowed:{allowed} bytes"
    )]
    DiskUsageEstimateExceeded { estimated: u64, allowed: u64 },

    #[error("unable to create memory snapshot")]
    UnableToCreateMemorySnapshot(#[from] crate::image::Error),

    #[error("unable to create memory snapshot from source: {src}")]
    UnableToCreateSnapshotFromSource {
        src: Source,
        #[source]
        source: Box<Error>,
    },

    #[error("no memory source available")]
    NoSourceAvailable,

    #[error(
        "all memory sources failed:\n{}",
        fmt_all_sources(crash, kcore, devmem)
    )]
    AllSourcesFailed {
        crash: Box<Error>,
        kcore: Box<Error>,
        devmem: Box<Error>,
    },

    #[error("unable to parse /proc/kcore: {0}")]
    KcoreParse(&'static str),

    #[error("u64 value {value} is too large to convert to f64")]
    F64Conversion { value: u64 },

    #[error("f64 value {value} cannot be converted to u64")]
    U64Conversion { value: f64 },

    #[error("snapshot destination path contains a NUL byte")]
    PathContainsNul(#[from] std::ffi::NulError),

    #[error("filesystem block size {value} does not fit in a u64")]
    BlockSize { value: i128 },

    #[error("operation not supported on this platform: {os}")]
    UnsupportedPlatform { os: &'static str },

    #[error("disk error")]
    Disk(#[source] std::io::Error),
}

fn fmt_all_sources(crash: &Error, kcore: &Error, devmem: &Error) -> String {
    use core::error::Error as _;
    use core::fmt::Write as _;
    let mut buf = String::new();
    for (name, err) in [("crash", crash), ("kcore", kcore), ("devmem", devmem)] {
        let _ = writeln!(buf, "  {name}: {err}");
        let mut source: Option<&dyn core::error::Error> = err.source();
        let mut depth = 4_usize;
        while let Some(s) = source {
            let _ = writeln!(buf, "{:depth$}caused by: {s}", "");
            source = s.source();
            depth = depth.saturating_add(2);
        }
    }
    buf.trim_end().to_string()
}

impl FmtDebug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        format_error(self, f)
    }
}

pub type Result<T> = core::result::Result<T, Error>;

impl Error {
    /// True when the underlying failure is a pre-acquisition disk-usage
    /// rejection. These are surfaced immediately rather than aggregated:
    /// trying the next source won't change the answer.
    fn is_disk_usage_exceeded(&self) -> bool {
        matches!(
            self,
            Error::UnableToCreateSnapshotFromSource { source: inner, .. }
                if matches!(**inner, Error::DiskUsageEstimateExceeded { .. })
        )
    }
}

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
            // Deliberately use Debug formatting rather than `path.display()`:
            // this value is embedded in error messages that may be logged or
            // shown in CI annotations. Debug quotes and escapes control
            // characters, ANSI sequences, and embedded newlines; Display via
            // `path.display()` would let them through verbatim.
            #[expect(
                clippy::unnecessary_debug_formatting,
                reason = "escaping is the point — see comment above"
            )]
            Self::Raw(ref path) => write!(f, "{path:?}"),
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
    metadata(Path::new("/proc/kcore")).is_ok_and(|x| x.len() > 0x2000)
        && can_open(Path::new("/proc/kcore"))
}

pub struct Snapshot<'a> {
    source: Option<Source>,
    destination: &'a Path,
    memory_ranges: Vec<Range<u64>>,
    format: Format,
    max_disk_usage: Option<NonZeroU64>,
    max_disk_usage_percentage: Option<f64>,
}

impl<'a> Snapshot<'a> {
    /// Create a new memory snapshot.
    ///
    /// Defaults to the `LiME` format.
    #[must_use]
    pub fn new(destination: &'a Path, memory_ranges: Vec<Range<u64>>) -> Self {
        Self {
            source: None,
            destination,
            memory_ranges,
            format: Format::Lime,
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
    pub fn source(self, source: Option<Source>) -> Self {
        Self { source, ..self }
    }

    /// Specify the snapshot format.
    #[must_use]
    pub fn format(self, format: Format) -> Self {
        Self { format, ..self }
    }

    fn create_source(&self, src: &Source) -> Result<()> {
        match *src {
            Source::ProcKcore => self.kcore(),
            Source::DevCrash => self.phys(Path::new("/dev/crash")),
            Source::DevMem => self.phys(Path::new("/dev/mem")),
            Source::Raw(ref s) => self.phys(s),
        }
        .map_err(|e| Error::UnableToCreateSnapshotFromSource {
            src: src.clone(),
            source: Box::new(e),
        })
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
        if let Some(ref src) = self.source {
            self.create_source(src)?;
        } else if self.destination == Path::new("/dev/stdout") {
            let src = Self::probe_single_source()?;
            self.create_source(&src)?;
        } else {
            let crash = match self.create_source(&Source::DevCrash) {
                Ok(()) => return Ok(()),
                Err(e) if e.is_disk_usage_exceeded() => return Err(e),
                Err(e) => Box::new(e),
            };
            let kcore = match self.create_source(&Source::ProcKcore) {
                Ok(()) => return Ok(()),
                Err(e) if e.is_disk_usage_exceeded() => return Err(e),
                Err(e) => Box::new(e),
            };
            let devmem = match self.create_source(&Source::DevMem) {
                Ok(()) => return Ok(()),
                Err(e) if e.is_disk_usage_exceeded() => return Err(e),
                Err(e) => Box::new(e),
            };

            return Err(Error::AllSourcesFailed {
                crash,
                kcore,
                devmem,
            });
        }

        Ok(())
    }

    /// Probe for an available source without trying multiple. Used when the
    /// destination cannot be rewound (`/dev/stdout`, streaming blob upload).
    ///
    /// Preference order matches the historical `/dev/stdout` branch: kcore,
    /// then `/dev/crash`, then `/dev/mem`.
    ///
    /// # Errors
    /// Returns `NoSourceAvailable` if none of the three probes succeed.
    pub fn probe_single_source() -> Result<Source> {
        if is_kcore_ok() {
            Ok(Source::ProcKcore)
        } else if can_open(Path::new("/dev/crash")) {
            Ok(Source::DevCrash)
        } else if can_open(Path::new("/dev/mem")) {
            Ok(Source::DevMem)
        } else {
            Err(Error::NoSourceAvailable)
        }
    }

    /// Stream a memory snapshot to an arbitrary writer.
    ///
    /// Unlike [`Self::create`], this does **not** auto-retry across
    /// sources. The destination is not assumed to be rewindable, so once
    /// any bytes are written we cannot fall back to a different source.
    /// The caller must either supply a [`Source`] via [`Self::source`] or
    /// rely on [`Self::probe_single_source`] to pick one up front.
    ///
    /// `max_disk_usage` and `max_disk_usage_percentage` are ignored: with
    /// no local disk involvement the limits don't apply. The caller is
    /// expected to enforce any blob-side size limits separately.
    ///
    /// # Errors
    /// Returns an error if:
    /// - No source is available
    /// - There is a failure reading from the source
    /// - Writing to `dst` fails
    pub fn create_to_writer<W: Write>(&self, dst: W) -> Result<()> {
        let source = match self.source {
            Some(ref s) => s.clone(),
            None => Self::probe_single_source()?,
        };

        match source {
            Source::ProcKcore => self.kcore_to_writer(dst),
            Source::DevCrash => self.phys_to_writer(Path::new("/dev/crash"), dst),
            Source::DevMem => self.phys_to_writer(Path::new("/dev/mem"), dst),
            Source::Raw(ref s) => self.phys_to_writer(s, dst),
        }
        .map_err(|e| Error::UnableToCreateSnapshotFromSource {
            src: source,
            source: Box::new(e),
        })
    }

    // given a set of ranges from iomem and a set of Blocks derived from the
    // pseudo-elf phys section headers, derive a set of ranges that can be used
    // to create a snapshot.
    //
    // Both `ranges` and `headers` must be sorted ascending and non-overlapping.
    // For every header that overlaps a range, the intersection is emitted as a
    // Block whose `offset` points at the corresponding position inside the
    // kcore source. Sections of `range` that fall in gaps between PT_LOAD
    // segments are skipped -- those addresses are not readable via kcore.
    fn find_kcore_blocks(ranges: &[Range<u64>], headers: &[Block]) -> Vec<Block> {
        let mut result = vec![];

        'outer: for range in ranges {
            let mut range = range.clone();

            for header in headers {
                // headers are sorted: once one starts past the remaining range,
                // no later header can intersect it either.
                if range.end <= header.range.start {
                    continue 'outer;
                }
                // range starts after this header; try the next.
                if range.start >= header.range.end {
                    continue;
                }

                // overlap. emit the intersection.
                let intersect_start = range.start.max(header.range.start);
                let intersect_end = range.end.min(header.range.end);
                result.push(Block {
                    offset: header
                        .offset
                        .saturating_add(intersect_start)
                        .saturating_sub(header.range.start),
                    range: intersect_start..intersect_end,
                });

                if range.end <= header.range.end {
                    continue 'outer;
                }
                range.start = header.range.end;
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
            return Err(Error::UnsupportedPlatform { os: OS });
        }
        Ok(())
    }

    fn kcore(&self) -> Result<()> {
        if !is_kcore_ok() {
            return Err(Error::LockedDownKcore);
        }

        let mut image =
            Image::<File, File>::new(self.format, Path::new("/proc/kcore"), self.destination)?;
        self.check_disk_usage(&image)?;
        Self::write_kcore_blocks(&mut image, &self.memory_ranges)
    }

    fn kcore_to_writer<W: Write>(&self, dst: W) -> Result<()> {
        if !is_kcore_ok() {
            return Err(Error::LockedDownKcore);
        }

        let mut image = Image::<File, W>::with_dst(self.format, Path::new("/proc/kcore"), dst)?;
        Self::write_kcore_blocks(&mut image, &self.memory_ranges)
    }

    fn write_kcore_blocks<W: Write>(
        image: &mut Image<File, W>,
        memory_ranges: &[Range<u64>],
    ) -> Result<()> {
        let file = elf::ElfStream::<NativeEndian, _>::open_stream(&mut image.src)?;
        let physical_ranges = Self::physical_ranges_from_segments(file.segments());

        if physical_ranges.is_empty() {
            return Err(Error::KcoreParse(
                "no usable PT_LOAD segments in /proc/kcore",
            ));
        }
        if memory_ranges.is_empty() {
            return Err(Error::KcoreParse("no initial memory range"));
        }

        let blocks = Self::find_kcore_blocks(memory_ranges, &physical_ranges);
        image.write_blocks(&blocks)?;
        Ok(())
    }

    // Translate /proc/kcore PT_LOAD segments into physical-address Blocks,
    // sorted ascending by p_paddr.
    //
    // Segments with `p_paddr` set to the all-ones sentinel (u64::MAX on
    // ELFCLASS64, u32::MAX widened on ELFCLASS32) are kernel virtual-only
    // mappings (vmalloc, modules) with no physical backing; skip them.
    // Zero-length segments are also skipped.
    fn physical_ranges_from_segments<I>(segments: I) -> Vec<Block>
    where
        I: IntoIterator,
        I::Item: core::borrow::Borrow<ProgramHeader>,
    {
        use core::borrow::Borrow as _;

        const PADDR_SENTINEL_64: u64 = u64::MAX;
        const PADDR_SENTINEL_32: u64 = 0xffff_ffff;

        let mut blocks: Vec<Block> = segments
            .into_iter()
            .filter_map(|phdr| {
                let phdr = phdr.borrow();
                if phdr.p_type != PT_LOAD {
                    return None;
                }
                if phdr.p_memsz == 0 {
                    return None;
                }
                if phdr.p_paddr == PADDR_SENTINEL_64 || phdr.p_paddr == PADDR_SENTINEL_32 {
                    return None;
                }
                let end = phdr.p_paddr.checked_add(phdr.p_memsz)?;
                Some(Block {
                    range: phdr.p_paddr..end,
                    offset: phdr.p_offset,
                })
            })
            .collect();
        blocks.sort_by_key(|b| b.range.start);
        blocks
    }

    fn phys(&self, mem: &Path) -> Result<()> {
        let blocks = Self::phys_blocks(mem, &self.memory_ranges);
        let mut image = Image::<File, File>::new(self.format, mem, self.destination)?;
        self.check_disk_usage(&image)?;
        image.write_blocks(&blocks)?;
        Ok(())
    }

    fn phys_to_writer<W: Write>(&self, mem: &Path, dst: W) -> Result<()> {
        let blocks = Self::phys_blocks(mem, &self.memory_ranges);
        let mut image = Image::<File, W>::with_dst(self.format, mem, dst)?;
        image.write_blocks(&blocks)?;
        Ok(())
    }

    fn phys_blocks(mem: &Path, memory_ranges: &[Range<u64>]) -> Vec<Block> {
        let is_crash = mem == Path::new("/dev/crash");
        memory_ranges
            .iter()
            .map(|x| Block {
                offset: x.start,
                range: if is_crash {
                    x.start..((x.end >> 12) << 12)
                } else {
                    x.start..x.end
                },
            })
            .collect::<Vec<_>>()
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

    fn fake_phdr(p_type: u32, p_paddr: u64, p_memsz: u64, p_offset: u64) -> ProgramHeader {
        ProgramHeader {
            p_type,
            p_flags: 0,
            p_offset,
            p_vaddr: 0,
            p_paddr,
            p_filesz: p_memsz,
            p_memsz,
            p_align: 0x1000,
        }
    }

    #[test]
    fn physical_ranges_use_paddr_not_vaddr() {
        // The whole point: a kernel that maps physical memory through two
        // non-contiguous virtual slabs (PPC64-style) must still produce
        // physical-address Blocks pointing at the right file offsets.
        // p_vaddr is intentionally garbage relative to p_paddr.
        let segments = [
            fake_phdr(PT_LOAD, 0x1000, 0x1000, 0x4000),
            fake_phdr(PT_LOAD, 0x10_0000, 0x2000, 0x5000),
        ];
        let result = Snapshot::physical_ranges_from_segments(segments);
        assert_eq!(
            result,
            vec![
                Block {
                    range: 0x1000..0x2000,
                    offset: 0x4000,
                },
                Block {
                    range: 0x10_0000..0x10_2000,
                    offset: 0x5000,
                },
            ]
        );
    }

    #[test]
    fn physical_ranges_skip_non_pt_load_segments() {
        // PT_NOTE, PT_DYNAMIC, etc. must not appear in the output.
        const PT_NOTE: u32 = 4;
        let segments = [
            fake_phdr(PT_NOTE, 0x1000, 0x100, 0x4000),
            fake_phdr(PT_LOAD, 0x2000, 0x100, 0x5000),
        ];
        let result = Snapshot::physical_ranges_from_segments(segments);
        assert_eq!(
            result,
            vec![Block {
                range: 0x2000..0x2100,
                offset: 0x5000,
            }]
        );
    }

    #[test]
    fn physical_ranges_skip_sentinel_paddrs() {
        // Kernel virtual-only mappings (vmalloc, modules) advertise
        // p_paddr == -1 to signal "no physical backing". Filter both
        // the 64-bit and zero-extended 32-bit forms.
        let segments = [
            fake_phdr(PT_LOAD, u64::MAX, 0x1000, 0x4000),
            fake_phdr(PT_LOAD, u64::from(u32::MAX), 0x1000, 0x5000),
            fake_phdr(PT_LOAD, 0x1000, 0x1000, 0x6000),
        ];
        let result = Snapshot::physical_ranges_from_segments(segments);
        assert_eq!(
            result,
            vec![Block {
                range: 0x1000..0x2000,
                offset: 0x6000,
            }]
        );
    }

    #[test]
    fn physical_ranges_skip_zero_size_segments() {
        let segments = [
            fake_phdr(PT_LOAD, 0x1000, 0, 0x4000),
            fake_phdr(PT_LOAD, 0x2000, 0x100, 0x5000),
        ];
        let result = Snapshot::physical_ranges_from_segments(segments);
        assert_eq!(
            result,
            vec![Block {
                range: 0x2000..0x2100,
                offset: 0x5000,
            }]
        );
    }

    #[test]
    fn physical_ranges_sorted_by_paddr() {
        // Caller (`find_kcore_blocks`) requires ascending sort.
        let segments = [
            fake_phdr(PT_LOAD, 0x3000, 0x100, 0x6000),
            fake_phdr(PT_LOAD, 0x1000, 0x100, 0x4000),
            fake_phdr(PT_LOAD, 0x2000, 0x100, 0x5000),
        ];
        let result = Snapshot::physical_ranges_from_segments(segments);
        let starts: Vec<u64> = result.iter().map(|b| b.range.start).collect();
        assert_eq!(starts, vec![0x1000, 0x2000, 0x3000]);
    }

    #[test]
    fn translate_ranges_with_straddled_gap() {
        // iomem range straddles a gap between two PT_LOAD segments. The
        // slice inside the second segment must still be emitted.
        let ranges = [Range {
            start: 400_u64,
            end: 900,
        }];
        let core_ranges = [
            Block {
                range: 100..500,
                offset: 0,
            },
            Block {
                range: 600..1000,
                offset: 400,
            },
        ];

        let expected = vec![
            Block {
                offset: 300,
                range: 400..500,
            },
            Block {
                offset: 400,
                range: 600..900,
            },
        ];

        assert_eq!(Snapshot::find_kcore_blocks(&ranges, &core_ranges), expected);
    }

    #[test]
    fn translate_ranges_with_range_spanning_header() {
        // iomem range fully contains a PT_LOAD segment and extends past it.
        // Both the spanned-header slice and the post-header overlap must
        // be emitted.
        let ranges = [Range {
            start: 0_u64,
            end: 1500,
        }];
        let core_ranges = [
            Block {
                range: 100..500,
                offset: 0,
            },
            Block {
                range: 1000..2000,
                offset: 400,
            },
        ];

        let expected = vec![
            Block {
                offset: 0,
                range: 100..500,
            },
            Block {
                offset: 400,
                range: 1000..1500,
            },
        ];

        assert_eq!(Snapshot::find_kcore_blocks(&ranges, &core_ranges), expected);
    }
}
