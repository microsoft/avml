// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#![deny(clippy::undocumented_unsafe_blocks)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::manual_assert)]
#![deny(clippy::indexing_slicing)]
#![deny(clippy::redundant_pub_crate)]
#![deny(clippy::if_then_some_else_none)]
#![deny(clippy::shadow_unrelated)]
#![deny(clippy::std_instead_of_core)]
#![warn(clippy::assertions_on_result_states)]
#![warn(clippy::if_then_some_else_none)]
#![warn(clippy::needless_continue)]
#![warn(clippy::redundant_pub_crate)]
#![warn(clippy::shadow_unrelated)]
#![warn(clippy::std_instead_of_core)]
#![warn(clippy::undocumented_unsafe_blocks)]
#![warn(clippy::unused_trait_names)]
#![warn(clippy::verbose_file_reads)]
// #![warn(clippy::arithmetic_side_effects)]
// #![warn(clippy::as_conversions)]
// #![warn(clippy::missing_errors_doc)]
// #![warn(clippy::pattern_type_mismatch)]
// #![warn(clippy::std_instead_of_alloc)]

#[cfg(target_family = "unix")]
mod disk_usage;
pub mod errors;
pub mod image;
pub mod iomem;
mod snapshot;
mod upload;
mod write_counter;

pub use crate::errors::Error;
pub use crate::snapshot::{Snapshot, Source};
#[cfg(feature = "blobstore")]
pub use crate::upload::blobstore::{BlobUploader, DEFAULT_CONCURRENCY};
#[cfg(feature = "put")]
pub use crate::upload::http::put;

pub const ONE_MB: usize = 1024 * 1024;

pub type Result<T> = core::result::Result<T, crate::errors::Error>;

pub(crate) fn indent<T: AsRef<str>>(data: T, indent: usize) -> String {
    data.as_ref()
        .split('\n')
        .map(|line| format!("{:indent$}{line}", ""))
        .collect::<Vec<_>>()
        .join("\n")
}
