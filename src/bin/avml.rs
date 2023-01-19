// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::manual_assert)]
#![deny(clippy::indexing_slicing)]

#[cfg(any(feature = "blobstore", feature = "put"))]
use avml::Error;
use avml::{iomem, Result, Snapshot, Source};
use clap::Parser;
use std::{num::NonZeroU64, ops::Range, path::PathBuf};
#[cfg(any(feature = "blobstore", feature = "put"))]
use tokio::{fs::remove_file, runtime::Runtime};
#[cfg(any(feature = "blobstore", feature = "put"))]
use url::Url;

#[derive(Parser)]
/// A portable volatile memory acquisition tool for Linux
#[command(author, version, about, long_about = None)]
struct Config {
    /// compress via snappy
    #[arg(long)]
    compress: bool,

    /// specify input source
    #[arg(long, value_enum)]
    source: Option<Source>,

    /// Specify the maximum estimated disk usage (in MB)
    #[arg(long)]
    max_disk_usage: Option<NonZeroU64>,

    /// Specify the maximum estimated disk usage to stay under
    #[arg(long, value_parser = disk_usage_percentage)]
    max_disk_usage_percentage: Option<f64>,

    /// upload via HTTP PUT upon acquisition
    #[cfg(feature = "put")]
    #[arg(long)]
    url: Option<Url>,

    /// delete upon successful upload
    #[cfg(any(feature = "blobstore", feature = "put"))]
    #[arg(long)]
    delete: bool,

    /// upload via Azure Blob Store upon acquisition
    #[cfg(feature = "blobstore")]
    #[arg(long)]
    sas_url: Option<Url>,

    /// specify maximum block size in MiB
    #[cfg(feature = "blobstore")]
    #[arg(long)]
    sas_block_size: Option<usize>,

    /// specify blob upload concurrency
    #[cfg(feature = "blobstore")]
    #[arg(long, default_value_t=avml::DEFAULT_CONCURRENCY)]
    sas_block_concurrency: usize,

    /// name of the file to write to on local system
    filename: PathBuf,
}

const PERCENTAGE: Range<f64> = 0.01..100.0;

fn disk_usage_percentage(s: &str) -> std::result::Result<f64, String> {
    let value = s
        .parse()
        .map_err(|_| format!("`{s}` isn't a valid value"))?;
    if PERCENTAGE.contains(&value) {
        Ok(value)
    } else {
        Err(format!(
            "value is not a valid percentage in range {}-{}",
            PERCENTAGE.start, PERCENTAGE.end
        ))
    }
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
        remove_file(&config.filename)
            .await
            .map_err(Error::RemoveSnapshot)?;
    }

    Ok(())
}

fn main() -> Result<()> {
    let config = Config::parse();

    let version = if config.compress { 2 } else { 1 };

    let ranges = iomem::parse()?;
    let snapshot = Snapshot::new(&config.filename, ranges)
        .source(config.source.as_ref())
        .max_disk_usage_percentage(config.max_disk_usage_percentage)
        .max_disk_usage(config.max_disk_usage)
        .version(version);
    snapshot.create()?;

    #[cfg(any(feature = "blobstore", feature = "put"))]
    {
        let rt = Runtime::new().map_err(Error::Tokio)?;
        rt.block_on(upload(&config))?;
    }

    Ok(())
}
