// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use avml::{BlobUploader, Result, put};
use clap::Subcommand;
use core::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use url::Url;

#[derive(Subcommand)]
pub enum Commands {
    /// Upload a local file via HTTP PUT.
    Put {
        /// name of the file to upload on the local system
        filename: PathBuf,
        /// url to upload via HTTP PUT
        url: Url,
    },

    /// Upload a local file to Azure Block Blob Storage.
    Blob {
        /// name of the file to upload on the local system
        filename: PathBuf,
        /// SAS URL identifying the destination Block Blob
        url: Url,
        /// specify blob upload concurrency; must be greater than 0
        #[arg(long)]
        sas_block_concurrency: Option<NonZeroUsize>,
        /// specify maximum block size in MiB; must be greater than 0
        #[arg(long)]
        sas_block_size: Option<NonZeroU64>,
    },
}

pub async fn run(cmd: Commands) -> Result<()> {
    match cmd {
        Commands::Put { filename, url } => put(&filename, &url).await?,
        Commands::Blob {
            filename,
            url,
            sas_block_size,
            sas_block_concurrency,
        } => {
            let uploader = BlobUploader::new(&url)?
                .block_size(sas_block_size)
                .concurrency(sas_block_concurrency);
            uploader.upload_file(&filename).await?;
        }
    }
    Ok(())
}
