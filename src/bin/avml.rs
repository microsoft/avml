// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use argh::FromArgs;
#[cfg(any(feature = "blobstore", feature = "put"))]
use avml::Error;
use avml::{iomem, Result, Snapshot, Source};
use std::path::PathBuf;
#[cfg(any(feature = "blobstore", feature = "put"))]
use tokio::{fs::remove_file, runtime::Runtime};
#[cfg(any(feature = "blobstore", feature = "put"))]
use url::Url;

#[derive(FromArgs)]
/// A portable volatile memory acquisition tool for Linux
struct Config {
    /// compress via snappy
    #[argh(switch)]
    compress: bool,

    /// specify input source [possible values: /proc/kcore, /dev/crash, /dev/mem]
    #[argh(option)]
    source: Option<Source>,

    /// upload via HTTP PUT upon acquisition
    #[cfg(feature = "put")]
    #[argh(option)]
    url: Option<Url>,

    /// delete upon successful upload
    #[cfg(any(feature = "blobstore", feature = "put"))]
    #[argh(switch)]
    delete: bool,

    /// upload via Azure Blob Store upon acquisition
    #[cfg(feature = "blobstore")]
    #[argh(option)]
    sas_url: Option<Url>,

    /// specify maximum block size in MiB
    #[cfg(feature = "blobstore")]
    #[argh(option)]
    sas_block_size: Option<usize>,

    /// specify blob upload concurrency
    #[cfg(feature = "blobstore")]
    #[argh(option)]
    sas_block_concurrency: Option<usize>,

    /// name of the file to write to on local system
    #[argh(positional)]
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
        remove_file(&config.filename)
            .await
            .map_err(Error::RemoveSnapshot)?;
    }

    Ok(())
}

fn main() -> Result<()> {
    let config: Config = argh::from_env();

    let version = if config.compress { 2 } else { 1 };

    let ranges = iomem::parse()?;
    let snapshot = Snapshot::new(&config.filename, ranges)
        .source(config.source.as_ref())
        .version(version);
    snapshot.create()?;

    #[cfg(any(feature = "blobstore", feature = "put"))]
    {
        let rt = Runtime::new().map_err(Error::Tokio)?;
        rt.block_on(upload(&config))?;
    }

    Ok(())
}
