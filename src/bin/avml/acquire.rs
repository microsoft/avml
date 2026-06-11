// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use avml::{Format, Result, Snapshot, Source, iomem};
use clap::Parser;
#[cfg(feature = "upload")]
use core::num::NonZeroUsize;
use core::{num::NonZeroU64, ops::Range};
use std::path::PathBuf;
#[cfg(feature = "upload")]
use {avml::Error, tokio::fs::remove_file, url::Url};

#[derive(Parser)]
pub struct Args {
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

    /// upload via HTTP PUT upon acquisition; mutually exclusive with --sas-url
    #[cfg(feature = "upload")]
    #[arg(long, conflicts_with = "sas_url")]
    url: Option<Url>,

    /// delete upon successful upload
    #[cfg(feature = "upload")]
    #[arg(long)]
    delete: bool,

    /// upload via Azure Blob Store upon acquisition; mutually exclusive with --url
    #[cfg(feature = "upload")]
    #[arg(long)]
    sas_url: Option<Url>,

    /// specify maximum block size in MiB; must be greater than 0
    #[cfg(feature = "upload")]
    #[arg(long)]
    sas_block_size: Option<NonZeroU64>,

    /// specify blob upload concurrency; must be greater than 0
    #[cfg(feature = "upload")]
    #[arg(long)]
    sas_block_concurrency: Option<NonZeroUsize>,

    /// name of the file to write to on local system
    filename: PathBuf,
}

const PERCENTAGE: Range<f64> = 0.01..100.0;

fn disk_usage_percentage(s: &str) -> core::result::Result<f64, String> {
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

pub fn run(args: &Args) -> Result<()> {
    let format = Format::from(args.compress);

    let ranges = iomem::parse()?;
    let snapshot = Snapshot::new(&args.filename, ranges)
        .source(args.source.clone())
        .max_disk_usage_percentage(args.max_disk_usage_percentage)
        .max_disk_usage(args.max_disk_usage)
        .format(format);
    snapshot.create()?;
    Ok(())
}

#[cfg(feature = "upload")]
pub async fn upload_after_acquire(args: &Args) -> Result<()> {
    let did_upload = if let Some(ref url) = args.url {
        avml::put(&args.filename, url).await?;
        true
    } else if let Some(ref sas_url) = args.sas_url {
        let uploader = avml::BlobUploader::new(sas_url)?
            .block_size(args.sas_block_size)
            .concurrency(args.sas_block_concurrency);
        uploader.upload_file(&args.filename).await?;
        true
    } else {
        false
    };

    if did_upload && args.delete {
        remove_file(&args.filename)
            .await
            .map_err(|source| Error::Io {
                context: "unable to remove snapshot",
                source,
            })?;
    }

    Ok(())
}
