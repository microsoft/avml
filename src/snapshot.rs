// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::{
    format_error,
    image::{Block, Image},
};
use std::{
    fs::{metadata, OpenOptions},
    ops::Range,
    path::{Path, PathBuf},
    str::FromStr,
};

#[derive(thiserror::Error)]
pub enum Error {
    #[error("unsupported memory source: {0}")]
    UnsupportedMemorySource(String),

    #[error("unable to parse elf structures")]
    Elf(elf::ParseError),

    #[error("locked down /proc/kcore")]
    LockedDownKcore,

    #[error("unable to find memory range: {0:?}")]
    UnableToFindMemoryRange(Range<u64>),

    #[error("unable to create memory snapshot")]
    UnableToCreateMemorySnapshot(#[from] crate::image::Error),

    #[error("unable to create memory snapshot from source: {1}")]
    UnableToCreateSnapshotFromSource(#[source] Box<dyn std::error::Error>, Source),

    #[error("unable to create memory snapshot: {0}")]
    UnableToCreateSnapshot(String),
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_error(self, f)
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub enum Source {
    DevCrash,
    DevMem,
    ProcKcore,
    Raw(PathBuf),
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::DevCrash => write!(f, "/dev/crash"),
            Source::DevMem => write!(f, "/dev/mem"),
            Source::ProcKcore => write!(f, "/proc/kcore"),
            Source::Raw(path) => write!(f, "{}", path.display()),
        }
    }
}

impl FromStr for Source {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let x = match s {
            "/dev/crash" => Self::DevCrash,
            "/dev/mem" => Self::DevMem,
            "/proc/kcore" => Self::ProcKcore,
            // Source::Raw isn't listed here, as FromStr is intended to be used
            // by the base CLI, where we don't want arbitrary file sources.
            _ => return Err(Error::UnsupportedMemorySource(s.to_string())),
        };
        Ok(x)
    }
}

#[must_use]
fn can_open(src: &Path) -> bool {
    OpenOptions::new().read(true).open(src).is_ok()
}

// The file /proc/kcore is a psuedo-file in ELF core format that is 4KB+physical
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

macro_rules! try_method {
    ($func:expr) => {{
        match $func {
            Ok(x) => return Ok(x),
            Err(err) => crate::indent(format!("{:?}", err), 4),
        }
    }};
}

pub struct Snapshot<'a, 'b> {
    source: Option<&'b Source>,
    destination: &'a Path,
    memory_ranges: Vec<Range<u64>>,
    version: u32,
}

impl<'a, 'b> Snapshot<'a, 'b> {
    #[must_use]
    pub fn new(destination: &'a Path, memory_ranges: Vec<Range<u64>>) -> Self {
        Self {
            source: None,
            destination,
            memory_ranges,
            version: 1,
        }
    }

    /// Specify the source for creating the snapshot
    #[must_use]
    pub fn source(self, source: Option<&'b Source>) -> Self {
        Self { source, ..self }
    }

    /// Specify the source for creating the snapshot
    #[must_use]
    pub fn version(self, version: u32) -> Self {
        Self { version, ..self }
    }

    fn from_source(&self, src: &Source) -> Result<()> {
        match src {
            Source::ProcKcore => self.kcore(),
            Source::DevCrash => self.phys(Path::new("/dev/crash")),
            Source::DevMem => self.phys(Path::new("/dev/mem")),
            Source::Raw(s) => self.phys(s),
        }
        .map_err(|e| Error::UnableToCreateSnapshotFromSource(Box::new(e), src.clone()))
    }

    pub fn create(&self) -> Result<()> {
        if let Some(src) = self.source {
            self.from_source(src)?;
        } else if self.destination == Path::new("/dev/stdout") {
            // If we're writing to stdout, we can't start over if reading from a
            // source fails.  As such, we need to do more work to pick a source
            // rather than just trying all available options.
            if is_kcore_ok() {
                self.from_source(&Source::ProcKcore)?;
            } else if can_open(Path::new("/dev/crash")) {
                self.from_source(&Source::DevCrash)?;
            } else if can_open(Path::new("/dev/mem")) {
                self.from_source(&Source::DevMem)?;
            } else {
                return Err(Error::UnableToCreateSnapshot(
                    "no source available".to_string(),
                ));
            }
        } else {
            let crash_err = try_method!(self.from_source(&Source::DevCrash));
            let kcore_err = try_method!(self.from_source(&Source::ProcKcore));
            let devmem_err = try_method!(self.from_source(&Source::DevMem));

            let reason = vec!["".to_string(), crash_err, kcore_err, devmem_err].join("\n");

            return Err(Error::UnableToCreateSnapshot(crate::indent(reason, 4)));
        }

        Ok(())
    }

    fn kcore(&self) -> Result<()> {
        if !is_kcore_ok() {
            return Err(Error::LockedDownKcore);
        }

        let mut image = Image::new(self.version, Path::new("/proc/kcore"), self.destination)?;

        let mut file = elf::File::open_stream(&mut image.src).map_err(Error::Elf)?;
        file.phdrs.retain(|&x| x.progtype == elf::types::PT_LOAD);
        file.phdrs.sort_by(|a, b| a.vaddr.cmp(&b.vaddr));
        let start = file.phdrs[0].vaddr - self.memory_ranges[0].start;

        let mut blocks = vec![];
        'outer: for range in &self.memory_ranges {
            for phdr in &file.phdrs {
                if range.start == phdr.vaddr - start {
                    let size = u64::min(phdr.memsz, range.end - range.start);
                    blocks.push(Block {
                        offset: phdr.offset,
                        range: range.start..range.start + size,
                    });
                    continue 'outer;
                }
            }
            return Err(Error::UnableToFindMemoryRange(range.clone()));
        }

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

        let mut image = Image::new(self.version, mem, self.destination)?;

        image.write_blocks(&blocks)?;

        Ok(())
    }
}
