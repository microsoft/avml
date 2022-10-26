// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::manual_assert)]
#![deny(clippy::indexing_slicing)]

use avml::{put, BlobUploader, Error, DEFAULT_CONCURRENCY};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tokio::runtime::Runtime;
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

        /// specify blob upload concurrency
        #[arg(long, default_value_t=DEFAULT_CONCURRENCY)]
        sas_block_concurrency: usize,

        /// specify maximum block size in MiB
        #[arg(long)]
        sas_block_size: Option<usize>,
    },
}

async fn run(cmd: Cmd) -> avml::Result<()> {
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

fn main() -> avml::Result<()> {
    let cmd = Cmd::parse();
    Runtime::new().map_err(Error::Tokio)?.block_on(run(cmd))?;
    Ok(())
}
