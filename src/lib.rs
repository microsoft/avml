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
pub use crate::upload::blobstore::{BlobUploader, DEFAULT_CONCURRENCY, Error as BlobError};
#[cfg(feature = "put")]
pub use crate::upload::http::put;
#[cfg(feature = "blobstore")]
pub use crate::upload::stream::{BLOB_MAX_BLOCKS, BlockBlobStream};
pub use crate::{
    errors::Error,
    image::Format,
    snapshot::{Snapshot, Source},
};

pub const ONE_MB: usize = 1024 * 1024;

pub type Result<T> = core::result::Result<T, crate::errors::Error>;
