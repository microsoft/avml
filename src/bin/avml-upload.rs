// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use avml::{BlobUploader, Result, put};
use clap::{Parser, Subcommand};
use core::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use url::Url;

#[derive(Parser)]
#[command(version)]
/// AVML upload tool
struct Cmd {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Put {
        /// name of the file to upload on the local system
        filename: PathBuf,

        /// url to upload via HTTP PUT
        url: Url,
    },
    UploadBlob {
        /// name of the file to upload on the local system
        filename: PathBuf,

        /// url to upload via Azure Blob Storage
        url: Url,

        /// specify blob upload concurrency; must be greater than 0
        #[arg(long)]
        sas_block_concurrency: Option<NonZeroUsize>,

        /// specify maximum block size in MiB; must be greater than 0
        #[arg(long)]
        sas_block_size: Option<NonZeroU64>,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cmd = Cmd::parse();
    match cmd.command {
        Commands::Put { filename, url } => put(&filename, &url).await?,
        Commands::UploadBlob {
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
