// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::manual_assert)]
#![deny(clippy::indexing_slicing)]

use avml::{iomem, Result, Snapshot, Source};
use clap::Parser;
use std::{
    io::{stdout, Write},
    path::PathBuf,
};
#[cfg(any(feature = "blobstore", feature = "put"))]
use tokio::{fs::remove_file, runtime::Runtime};
#[cfg(any(feature = "blobstore", feature = "put"))]
use url::Url;

#[derive(Parser)]
/// A portable volatile memory acquisition tool for Linux
#[clap(version)]
struct Config {
    /// display license information
    #[clap(long, value_parser)]
    license: bool,

    /// compress via snappy
    #[clap(long, value_parser)]
    compress: bool,

    /// specify input source
    #[clap(long, arg_enum, value_parser)]
    source: Option<Source>,

    /// upload via HTTP PUT upon acquisition
    #[cfg(feature = "put")]
    #[clap(long, value_parser)]
    url: Option<Url>,

    /// delete upon successful upload
    #[cfg(any(feature = "blobstore", feature = "put"))]
    #[clap(long, value_parser)]
    delete: bool,

    /// upload via Azure Blob Store upon acquisition
    #[cfg(feature = "blobstore")]
    #[clap(long, value_parser)]
    sas_url: Option<Url>,

    /// specify maximum block size in MiB
    #[cfg(feature = "blobstore")]
    #[clap(long, value_parser)]
    sas_block_size: Option<usize>,

    /// specify blob upload concurrency
    #[cfg(feature = "blobstore")]
    #[clap(long, value_parser)]
    sas_block_concurrency: Option<usize>,

    /// name of the file to write to on local system
    #[clap(value_parser)]
    filename: PathBuf,
}

#[cfg(any(feature = "blobstore", feature = "put"))]
async fn upload(config: &Config) -> Result<()> {
    let mut delete = false;

    #[cfg(feature = "put")]
    {
        if let Some(url) = &config.url {
            avml::put(&config.filename, url).await?;
            delete = true;
        }
    }

    #[cfg(feature = "blobstore")]
    {
        if let Some(sas_url) = &config.sas_url {
            let uploader = avml::BlobUploader::new(sas_url)?
                .block_size(config.sas_block_size)
                .concurrency(config.sas_block_concurrency);
            uploader.upload_file(&config.filename).await?;
            delete = true;
        }
    }

    if delete && config.delete {
        remove_file(&config.filename).await?;
    }

    Ok(())
}

fn main() -> Result<()> {
    let config = Config::parse();

    if config.license {
        stdout().write_all(include_bytes!("../../eng/licenses.json"))?;
        return Ok(());
    }

    let version = if config.compress { 2 } else { 1 };

    let ranges = iomem::parse()?;
    let snapshot = Snapshot::new(&config.filename, ranges)
        .source(config.source.as_ref())
        .version(version);
    snapshot.create()?;

    #[cfg(any(feature = "blobstore", feature = "put"))]
    {
        let rt = Runtime::new()?;
        rt.block_on(upload(&config))?;
    }

    Ok(())
}
