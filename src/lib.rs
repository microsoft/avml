// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::manual_assert)]
#![deny(clippy::indexing_slicing)]

pub mod image;
pub mod iomem;
mod snapshot;
mod upload;
mod write_counter;

#[cfg(feature = "blobstore")]
pub use crate::upload::blobstore::BlobUploader;

#[cfg(feature = "put")]
pub use crate::upload::http::put;

pub use crate::snapshot::{Snapshot, Source};

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

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[cfg(feature = "blobstore")]
    #[error("unable to upload file to Azure Storage")]
    Blob(#[from] crate::upload::blobstore::Error),

    #[error("no conversion required")]
    NoConversionRequired,
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn format_error(
    e: &impl std::error::Error,
    f: &mut std::fmt::Formatter,
) -> std::fmt::Result {
    write!(f, "error: {}", e)?;

    let mut source = e.source();

    if e.source().is_some() {
        writeln!(f, "\ncaused by:")?;
        let mut i: usize = 0;
        while let Some(inner) = source {
            writeln!(f, "{: >5}: {}", i, inner)?;
            source = inner.source();
            i += 1;
        }
    }

    Ok(())
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_error(self, f)
    }
}

pub(crate) fn indent<T: AsRef<str>>(data: T, indent: usize) -> String {
    data.as_ref()
        .split('\n')
        .map(|line| format!("{:indent$}{}", "", line, indent = indent))
        .collect::<Vec<_>>()
        .join("\n")
}
