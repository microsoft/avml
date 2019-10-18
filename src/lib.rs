extern crate byteorder;
extern crate elf;
extern crate snap;

#[cfg(feature = "put")]
extern crate reqwest;

#[cfg(feature = "blobstore")]
extern crate azure;
#[cfg(feature = "blobstore")]
extern crate azure_sdk_core;
#[cfg(feature = "blobstore")]
extern crate azure_sdk_storage_core;
#[cfg(feature = "blobstore")]
extern crate retry;
#[cfg(feature = "blobstore")]
extern crate tokio_core;
#[cfg(feature = "blobstore")]
extern crate url;

pub mod image;
pub mod iomem;

#[cfg(feature = "blobstore")]
pub mod blobstore;

#[cfg(feature = "put")]
pub mod upload;
