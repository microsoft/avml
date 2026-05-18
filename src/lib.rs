// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#[cfg(target_family = "unix")]
mod disk_usage;
pub mod errors;
pub mod image;
pub mod io;
pub mod iomem;
mod snapshot;
mod upload;

#[cfg(feature = "blobstore")]
pub use crate::upload::blobstore::{BlobUploader, DEFAULT_CONCURRENCY};
#[cfg(feature = "put")]
pub use crate::upload::http::put;
pub use crate::{
    errors::Error,
    snapshot::{Snapshot, Source},
};

pub const ONE_MB: usize = 1024 * 1024;

pub type Result<T> = core::result::Result<T, crate::errors::Error>;

pub(crate) fn indent<T: AsRef<str>>(data: T, indent: usize) -> String {
    data.as_ref()
        .split('\n')
        .map(|line| format!("{:indent$}{line}", ""))
        .collect::<Vec<_>>()
        .join("\n")
}
