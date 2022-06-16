// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use avml::{put, BlobUploader, Error};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tokio::runtime::Runtime;
use url::Url;

#[derive(Parser)]
#[clap(version)]
struct Cmd {
    #[clap(subcommand)]
    subcommand: SubCommands,
}

#[derive(Subcommand)]
enum SubCommands {
    Put {
        /// name of the file to upload on the local system
        #[clap(value_parser)]
        filename: PathBuf,

        // url to upload via HTTP PUT
        #[clap(value_parser)]
        url: Url,
    },
    UploadBlob {
        /// name of the file to upload on the local system
        #[clap(value_parser)]
        filename: PathBuf,

        // url to upload via Azure Blob Storage
        #[clap(value_parser)]
        url: Url,

        /// specify blob upload concurrency
        #[clap(long, value_parser)]
        sas_block_concurrency: Option<usize>,

        /// specify maximum block size in MiB
        #[clap(long, value_parser)]
        sas_block_size: Option<usize>,
    },
}

async fn run(cmd: Cmd) -> avml::Result<()> {
    match cmd.subcommand {
        SubCommands::Put { filename, url } => put(&filename, &url).await?,
        SubCommands::UploadBlob {
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
