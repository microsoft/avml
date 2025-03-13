// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::manual_assert)]
#![deny(clippy::indexing_slicing)]
#![deny(clippy::std_instead_of_core)]

#[cfg(target_family = "unix")]
mod disk_usage;
pub mod image;
pub mod iomem;
mod snapshot;
mod upload;
mod write_counter;

pub use crate::snapshot::{Snapshot, Source};
#[cfg(feature = "blobstore")]
pub use crate::upload::blobstore::{BlobUploader, DEFAULT_CONCURRENCY};
#[cfg(feature = "put")]
pub use crate::upload::http::put;
use core::fmt::{Debug as FmtDebug, Formatter, Result as FmtResult};
use std::{error::Error as StdError, io::Error as IoError};

pub const ONE_MB: usize = 1024 * 1024;

#[derive(thiserror::Error)]
pub enum Error {
    #[error("unable to create snapshot")]
    Image(#[from] crate::image::Error),

    #[error("unable to read memory")]
    Memory(#[from] crate::snapshot::Error),

    #[error("unable to parse /proc/iomem")]
    Iomem(#[from] crate::iomem::Error),

    #[cfg(feature = "put")]
    #[error("unable to upload file via PUT")]
    Upload(#[from] crate::upload::http::Error),

    #[cfg(feature = "blobstore")]
    #[error("unable to upload file to Azure Storage")]
    Blob(#[from] crate::upload::blobstore::Error),

    #[cfg(any(feature = "blobstore", feature = "put"))]
    #[error("tokio runtime error: {0}")]
    Tokio(#[source] IoError),

    #[cfg(any(feature = "blobstore", feature = "put"))]
    #[error("unable to remove snapshot")]
    RemoveSnapshot(#[source] IoError),

    #[error("no conversion required")]
    NoConversionRequired,
}

pub type Result<T> = core::result::Result<T, Error>;

pub(crate) fn format_error(e: &impl StdError, f: &mut Formatter) -> FmtResult {
    write!(f, "error: {e}")?;

    let mut source = e.source();

    if e.source().is_some() {
        writeln!(f, "\ncaused by:")?;
        let mut i: usize = 0;
        while let Some(inner) = source {
            writeln!(f, "{i: >5}: {inner}")?;
            source = inner.source();
            i += 1;
        }
    }

    Ok(())
}

impl FmtDebug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        format_error(self, f)
    }
}

pub(crate) fn indent<T: AsRef<str>>(data: T, indent: usize) -> String {
    data.as_ref()
        .split('\n')
        .map(|line| format!("{:indent$}{line}", ""))
        .collect::<Vec<_>>()
        .join("\n")
}
