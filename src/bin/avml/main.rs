// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use avml::Result;
use clap::{Parser, Subcommand};

mod acquire;
#[cfg(feature = "convert")]
mod convert;
#[cfg(feature = "stream")]
mod stream;
#[cfg(feature = "upload")]
mod upload;

/// A portable volatile memory acquisition tool for Linux.
#[derive(Parser)]
#[command(author, version, long_about = None)]
struct Cmd {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Acquire a memory snapshot to a local file (and optionally upload it).
    Acquire(acquire::Args),

    /// Convert between AVML and `LiME` snapshot formats and a raw memory image.
    #[cfg(feature = "convert")]
    Convert(convert::Args),

    /// Upload an already-acquired snapshot file to remote storage.
    #[cfg(feature = "upload")]
    #[command(subcommand)]
    Upload(upload::Commands),

    /// Stream a memory snapshot directly to remote storage, without
    /// writing it to a local file.
    #[cfg(feature = "stream")]
    #[command(subcommand)]
    Stream(stream::Commands),
}

#[cfg(not(any(feature = "stream", feature = "upload")))]
fn main() -> Result<()> {
    let cmd = Cmd::parse();
    match cmd.command {
        Commands::Acquire(args) => acquire::run(&args),
        #[cfg(feature = "convert")]
        Commands::Convert(args) => convert::run(&args),
    }
}

#[cfg(any(feature = "stream", feature = "upload"))]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cmd = Cmd::parse();
    match cmd.command {
        Commands::Acquire(args) => {
            acquire::run(&args)?;
            #[cfg(feature = "upload")]
            acquire::upload_after_acquire(&args).await?;
            Ok(())
        }
        #[cfg(feature = "convert")]
        Commands::Convert(args) => convert::run(&args),
        #[cfg(feature = "upload")]
        Commands::Upload(sub) => upload::run(sub).await,
        #[cfg(feature = "stream")]
        Commands::Stream(sub) => stream::run(sub).await,
    }
}
