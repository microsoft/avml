// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#[cfg(target_family = "unix")]
use crate::disk_usage;
use crate::{
    errors::format_error,
    image::{Block, Format, Image, MAX_BLOCK_SIZE},
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
    fs::File,
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
    /// With `CONFIG_STRICT_DEVMEM`, only the first 1MiB of memory can be
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

struct PreparedSnapshot {
    source: Source,
    image: Image<File, Vec<u8>>,
    blocks: Vec<Block>,
}

impl PreparedSnapshot {
    fn write_to<W: Write>(self, mut dst: W) -> Result<()> {
        dst.write_all(&self.image.dst)
            .map_err(|source| crate::image::Error::Io {
                context: "unable to write initial memory chunk",
                source,
            })?;
        drop(self.image.dst);
        let mut image = Image {
            format: self.image.format,
            align_src: self.image.align_src,
            src: self.image.src,
            dst,
        };
        image.write_blocks(&self.blocks)?;
        Ok(())
    }
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

    /// Specify the maximum disk space in MiB to use
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

    fn prepare_source(&self, source: &Source) -> Result<PreparedSnapshot> {
        (|| {
            let path = match *source {
                Source::ProcKcore => Path::new("/proc/kcore"),
                Source::DevCrash => Path::new("/dev/crash"),
                Source::DevMem => Path::new("/dev/mem"),
                Source::Raw(ref path) => path,
            };
            let mut image = Image::<File, Vec<u8>>::with_dst(self.format, path, Vec::new())?;
            for range in &self.memory_ranges {
                if range.start >= range.end {
                    return Err(crate::image::Error::InvalidRange {
                        range: range.clone(),
                    }
                    .into());
                }
            }
            let mut blocks = if matches!(*source, Source::ProcKcore) {
                Self::kcore_blocks(&mut image.src, &self.memory_ranges)?
            } else {
                Self::phys_blocks(path, &self.memory_ranges)
            };
            for block in &blocks {
                if block.range.start >= block.range.end {
                    return Err(crate::image::Error::InvalidRange {
                        range: block.range.clone(),
                    }
                    .into());
                }
                block
                    .offset
                    .checked_add(block.range.end.saturating_sub(block.range.start))
                    .ok_or(crate::image::Error::TooLarge)?;
            }
            let first = blocks.first_mut().ok_or_else(|| crate::image::Error::Io {
                context: "no readable memory ranges for source",
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "no memory blocks overlap the requested ranges",
                ),
            })?;
            let end = first
                .range
                .end
                .min(first.range.start.saturating_add(MAX_BLOCK_SIZE));
            let first_chunk = Block {
                offset: first.offset,
                range: first.range.start..end,
            };
            first.offset = first
                .offset
                .checked_add(end.saturating_sub(first.range.start))
                .ok_or(crate::image::Error::TooLarge)?;
            first.range.start = end;
            blocks.retain(|block| !block.range.is_empty());
            image.write_blocks(&[first_chunk])?;
            Ok(PreparedSnapshot {
                source: source.clone(),
                image,
                blocks,
            })
        })()
        .map_err(|error| Error::UnableToCreateSnapshotFromSource {
            src: source.clone(),
            source: Box::new(error),
        })
    }

    fn select_source(&self, [kcore, crash, devmem]: [Source; 3]) -> Result<PreparedSnapshot> {
        let kcore = match self.prepare_source(&kcore) {
            Ok(prepared) => return Ok(prepared),
            Err(error) => Box::new(error),
        };
        let crash = match self.prepare_source(&crash) {
            Ok(prepared) => return Ok(prepared),
            Err(error) => Box::new(error),
        };
        let devmem = match self.prepare_source(&devmem) {
            Ok(prepared) => return Ok(prepared),
            Err(error) => Box::new(error),
        };
        Err(Error::AllSourcesFailed {
            crash,
            kcore,
            devmem,
        })
    }

    fn prepare(&self) -> Result<PreparedSnapshot> {
        if let Some(ref source) = self.source {
            self.prepare_source(source)
        } else {
            self.select_source([Source::ProcKcore, Source::DevCrash, Source::DevMem])
        }
    }

    /// Create a memory snapshot.
    ///
    /// Validate the source's memory ranges and buffer its first chunk before
    /// opening the destination. Automatic selection tries `/proc/kcore`, then
    /// `/dev/crash`, then `/dev/mem`. Once selected, the source is never changed:
    /// a later failure ends acquisition and leaves any partial output in place.
    ///
    /// # Errors
    /// Returns an error if:
    /// - No source is available for creating the snapshot
    /// - There is a failure reading from the specified source
    /// - The estimated disk usage exceeds the specified limits
    /// - Failed to create or write to the destination file
    /// - The destination already exists, except for Unix `/dev/null` and `/dev/stdout`
    pub fn create(&self) -> Result<()> {
        let prepared = self.prepare()?;
        let source = prepared.source.clone();
        (|| {
            let dst = Image::<File, File>::open_dst(self.destination)?;
            self.check_disk_usage()?;
            prepared.write_to(dst)
        })()
        .map_err(|error| Error::UnableToCreateSnapshotFromSource {
            src: source,
            source: Box::new(error),
        })
    }

    /// Validate sources using the current `/proc/iomem` ranges, returning the
    /// first candidate whose range mapping and first chunk can be read.
    ///
    /// Preference order is `/proc/kcore`, then `/dev/crash`, then `/dev/mem`.
    /// This helper drops the probed handle. Acquisition methods instead retain
    /// their own validated handle and buffered chunk through completion.
    ///
    /// # Errors
    /// Returns an error if memory ranges cannot be read or all sources fail.
    pub fn probe_single_source() -> Result<Source> {
        let ranges = crate::iomem::parse().map_err(|source| crate::image::Error::Io {
            context: "unable to read memory ranges for source validation",
            source: std::io::Error::other(source),
        })?;
        let prepared = Self::new(Path::new("/dev/null"), ranges).prepare()?;
        Ok(prepared.source)
    }

    /// Stream a memory snapshot to an arbitrary writer.
    ///
    /// Like [`Self::create`], this validates the source and buffers its first
    /// chunk before writing any output. Source fallback is only allowed during
    /// validation; any later read or write failure ends acquisition.
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
        let prepared = self.prepare()?;
        let source = prepared.source.clone();
        prepared
            .write_to(dst)
            .map_err(|error| Error::UnableToCreateSnapshotFromSource {
                src: source,
                source: Box::new(error),
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

    /// Check disk usage after creating the destination.
    #[cfg(target_family = "unix")]
    fn check_disk_usage(&self) -> Result<()> {
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
    fn check_disk_usage(&self) -> Result<()> {
        if self.max_disk_usage.is_some() || self.max_disk_usage_percentage.is_some() {
            return Err(Error::UnsupportedPlatform { os: OS });
        }
        Ok(())
    }

    fn kcore_blocks<R: Read + Seek>(
        src: &mut R,
        memory_ranges: &[Range<u64>],
    ) -> Result<Vec<Block>> {
        let file = elf::ElfStream::<NativeEndian, _>::open_stream(src)?;
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
        if blocks.is_empty() {
            return Err(Error::KcoreParse(
                "no PT_LOAD segments overlap the requested memory ranges",
            ));
        }
        Ok(blocks)
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
    use std::{fs, io::Cursor};

    type TestResult<T = ()> = core::result::Result<T, Box<dyn core::error::Error>>;

    fn raw_source(path: &Path, bytes: &[u8]) -> std::io::Result<Source> {
        fs::write(path, bytes)?;
        Ok(Source::Raw(path.to_path_buf()))
    }

    fn encoded_blocks(bytes: &[u8], ranges: &[Range<u64>], format: Format) -> TestResult<Vec<u8>> {
        let mut image = Image::from_streams(format, Cursor::new(bytes), Vec::new());
        let blocks: Vec<_> = ranges
            .iter()
            .map(|range| Block {
                offset: range.start,
                range: range.clone(),
            })
            .collect();
        image.write_blocks(&blocks)?;
        Ok(image.dst)
    }

    #[test]
    fn invalid_source_does_not_create_or_write_output() -> TestResult {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("source");
        let source = raw_source(&path, b"abc")?;
        let dst = dir.path().join("output");
        for ranges in [
            vec![Range { start: 0, end: 4 }],
            Vec::new(),
            vec![Range { start: 4, end: 3 }],
        ] {
            let snapshot = Snapshot::new(&dst, ranges).source(Some(source.clone()));
            assert!(matches!(
                snapshot.create(),
                Err(Error::UnableToCreateSnapshotFromSource { .. })
            ));
            assert!(!dst.exists());
            let mut output = Vec::new();
            assert!(matches!(
                snapshot.create_to_writer(&mut output),
                Err(Error::UnableToCreateSnapshotFromSource { .. })
            ));
            assert_eq!(output, Vec::<u8>::new());
        }
        Ok(())
    }

    #[test]
    fn selection_skips_failed_initial_reads_but_never_retries_later_failures() -> TestResult {
        let dir = tempfile::tempdir()?;
        let bad = raw_source(&dir.path().join("bad"), b"ab")?;
        let partial = raw_source(&dir.path().join("partial"), b"abcd")?;
        let good = raw_source(&dir.path().join("good"), b"abcdefghijkl")?;
        let dst = dir.path().join("output");
        let snapshot = Snapshot::new(&dst, vec![0..4, 8..12]);

        let prepared = snapshot.select_source([bad.clone(), partial.clone(), good.clone()])?;
        assert!(matches!(
            (&prepared.source, &partial),
            (Source::Raw(selected), Source::Raw(expected)) if selected == expected
        ));
        assert!(!dst.exists());
        let mut output = Vec::new();
        assert!(matches!(
            prepared.write_to(&mut output),
            Err(Error::UnableToCreateMemorySnapshot(_))
        ));
        assert_eq!(
            output,
            encoded_blocks(b"abcd", &[Range { start: 0, end: 4 }], Format::Lime)?
        );

        assert!(matches!(
            snapshot.select_source([bad.clone(), bad.clone(), bad]),
            Err(Error::AllSourcesFailed { .. })
        ));
        let snapshot = snapshot.source(Some(partial));
        assert!(matches!(
            snapshot.create(),
            Err(Error::UnableToCreateSnapshotFromSource { .. })
        ));
        assert_eq!(fs::read(&dst)?, output);
        let mut streamed = Vec::new();
        assert!(matches!(
            snapshot.create_to_writer(&mut streamed),
            Err(Error::UnableToCreateSnapshotFromSource { .. })
        ));
        assert_eq!(streamed, output);
        Ok(())
    }

    #[test]
    fn initial_chunk_is_bounded_and_continues_without_duplicate_bytes() -> TestResult {
        let dir = tempfile::tempdir()?;
        let len = usize::try_from(MAX_BLOCK_SIZE)?.saturating_add(17);
        let bytes = vec![0x51; len];
        let source = raw_source(&dir.path().join("source"), &bytes)?;
        let dst = dir.path().join("output");
        let range = 0..u64::try_from(len)?;
        for format in [Format::Lime, Format::AvmlCompressed] {
            let snapshot = Snapshot::new(&dst, vec![range.clone()])
                .source(Some(source.clone()))
                .format(format);
            let prepared = snapshot.prepare()?;
            assert_eq!(
                prepared.blocks,
                vec![Block {
                    offset: MAX_BLOCK_SIZE,
                    range: MAX_BLOCK_SIZE..range.end,
                }]
            );
            let mut output = Vec::new();
            prepared.write_to(&mut output)?;
            assert_eq!(
                output,
                encoded_blocks(&bytes, std::slice::from_ref(&range), format)?
            );
        }
        Ok(())
    }

    #[test]
    fn zero_initial_chunk_still_selects_source_and_preserves_following_ranges() -> TestResult {
        let dir = tempfile::tempdir()?;
        let bytes = b"\0\0\0\0abcd";
        let source = raw_source(&dir.path().join("source"), bytes)?;
        let dst = dir.path().join("output");
        let snapshot = Snapshot::new(&dst, vec![0..4, 4..8]).source(Some(source));
        let mut output = Vec::new();
        snapshot.create_to_writer(&mut output)?;
        assert_eq!(output, encoded_blocks(bytes, &[0..4, 4..8], Format::Lime)?);
        snapshot.create()?;
        assert_eq!(fs::read(&dst)?, output);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn acquisition_retains_initial_bytes_and_open_source_after_path_replacement() -> TestResult {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("source");
        let source = raw_source(&path, b"abcdefgh")?;
        let dst = dir.path().join("output");
        let snapshot = Snapshot::new(&dst, vec![0..4, 4..8]).source(Some(source));
        let prepared = snapshot.prepare()?;
        let retained = dir.path().join("retained");
        fs::rename(&path, &retained)?;
        fs::write(&retained, b"xxxxefgh")?;
        fs::write(&path, b"replaced")?;
        let mut output = Vec::new();
        prepared.write_to(&mut output)?;
        assert_eq!(
            output,
            encoded_blocks(b"abcdefgh", &[0..4, 4..8], Format::Lime)?
        );
        Ok(())
    }

    #[test]
    fn output_failures_do_not_restart_acquisition() -> TestResult {
        let dir = tempfile::tempdir()?;
        let source = raw_source(&dir.path().join("source"), b"abcdefgh")?;
        let dst = dir.path().join("output");
        let snapshot = Snapshot::new(&dst, vec![0..4, 4..8]).source(Some(source));
        fs::write(&dst, b"existing evidence")?;
        assert!(matches!(
            snapshot.create(),
            Err(Error::UnableToCreateSnapshotFromSource { .. })
        ));
        assert_eq!(fs::read(&dst)?, b"existing evidence");
        let mut output = [0_u8; 1];
        assert!(matches!(
            snapshot.create_to_writer(output.as_mut_slice()),
            Err(Error::UnableToCreateSnapshotFromSource { .. })
        ));
        assert_eq!(output, [0x45]);
        Ok(())
    }

    fn kcore_fixture() -> TestResult<Vec<u8>> {
        use byteorder::{NativeEndian as ByteOrder, WriteBytesExt as _};
        let mut bytes = b"\x7fELF".to_vec();
        bytes.extend_from_slice(&[2, if cfg!(target_endian = "little") { 1 } else { 2 }, 1]);
        bytes.resize(16, 0);
        bytes.write_u16::<ByteOrder>(elf::abi::ET_CORE)?;
        bytes.write_u16::<ByteOrder>(elf::abi::EM_X86_64)?;
        bytes.write_u32::<ByteOrder>(1)?;
        bytes.write_u64::<ByteOrder>(0)?;
        bytes.write_u64::<ByteOrder>(64)?;
        bytes.write_u64::<ByteOrder>(0)?;
        bytes.write_u32::<ByteOrder>(0)?;
        bytes.write_u16::<ByteOrder>(64)?;
        bytes.write_u16::<ByteOrder>(56)?;
        bytes.write_u16::<ByteOrder>(1)?;
        bytes.write_u16::<ByteOrder>(0)?;
        bytes.write_u16::<ByteOrder>(0)?;
        bytes.write_u16::<ByteOrder>(0)?;
        bytes.write_u32::<ByteOrder>(PT_LOAD)?;
        bytes.write_u32::<ByteOrder>(0)?;
        bytes.write_u64::<ByteOrder>(0x1000)?;
        bytes.write_u64::<ByteOrder>(0)?;
        bytes.write_u64::<ByteOrder>(0x2000)?;
        bytes.write_u64::<ByteOrder>(4)?;
        bytes.write_u64::<ByteOrder>(4)?;
        bytes.write_u64::<ByteOrder>(0x1000)?;
        bytes.resize(0x1000, 0);
        bytes.extend_from_slice(b"abcd");
        Ok(bytes)
    }

    #[test]
    fn kcore_validation_requires_valid_metadata_and_overlapping_ranges() -> TestResult {
        let bytes = kcore_fixture()?;
        let range = 0x2000..0x2004;
        let blocks =
            Snapshot::kcore_blocks(&mut Cursor::new(&bytes), std::slice::from_ref(&range))?;
        assert_eq!(
            blocks,
            vec![Block {
                offset: 0x1000,
                range: 0x2000..0x2004,
            }]
        );
        let missing = 0x3000..0x3004;
        assert!(matches!(
            Snapshot::kcore_blocks(&mut Cursor::new(&bytes), std::slice::from_ref(&missing)),
            Err(Error::KcoreParse(_))
        ));
        assert!(matches!(
            Snapshot::kcore_blocks(
                &mut Cursor::new(b"invalid ELF"),
                std::slice::from_ref(&range)
            ),
            Err(Error::Elf(_))
        ));
        Ok(())
    }

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

    #[test]
    fn source_blocks_preserve_exclusive_range_end() {
        let range = 0x1000..0x2000;
        let ranges = std::slice::from_ref(&range);
        let expected = vec![Block {
            offset: 0x1000,
            range: 0x1000..0x2000,
        }];

        assert_eq!(
            Snapshot::phys_blocks(Path::new("/dev/mem"), ranges),
            expected
        );
        assert_eq!(
            Snapshot::phys_blocks(Path::new("/dev/crash"), ranges),
            expected
        );
        assert_eq!(
            Snapshot::phys_blocks(Path::new("/tmp/memory.raw"), ranges),
            expected
        );
        assert_eq!(Snapshot::find_kcore_blocks(ranges, &expected), expected);
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
