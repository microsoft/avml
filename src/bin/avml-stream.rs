// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use avml::{BLOB_MAX_BLOCKS, BlobError, BlockBlobStream, Format, Result, Snapshot, Source, iomem};
use azure_storage_blob::BlobClient;
use clap::{Parser, Subcommand};
use core::{
    num::{NonZeroU64, NonZeroUsize},
    ops::Range,
};
use std::path::PathBuf;
use url::Url;

/// Stream a memory snapshot directly to remote storage without writing
/// to a local file.
///
/// Subcommands name the destination protocol. Only Azure Block Blob is
/// supported today.
#[derive(Parser)]
#[command(author, version, long_about = None)]
struct Cmd {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Stream to Azure Block Blob Storage via `stage_block` + `commit_block_list`.
    Blob(BlobArgs),
}

#[derive(Parser)]
struct BlobArgs {
    /// compress via snappy
    #[arg(long)]
    compress: bool,

    /// specify input source. If unset, the source is probed once at
    /// start (kcore, then /dev/crash, then /dev/mem); the choice cannot
    /// be changed once any bytes have been written.
    #[arg(long, value_enum)]
    source: Option<Source>,

    /// SAS URL identifying the destination Block Blob.
    sas_url: Url,

    /// minimum block size in MiB. The actual block size may be larger
    /// if needed to keep the total block count below Azure's 50,000
    /// limit.
    #[arg(long)]
    sas_block_size: Option<NonZeroU64>,

    /// maximum number of in-flight `stage_block` calls.
    #[arg(long)]
    sas_block_concurrency: Option<NonZeroUsize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cmd = Cmd::parse();
    match cmd.command {
        Commands::Blob(args) => stream_blob(args).await,
    }
}

async fn stream_blob(args: BlobArgs) -> Result<()> {
    let ranges = iomem::parse()?;
    let block_size = derive_block_size(&ranges, args.sas_block_size)?;
    let concurrency = args
        .sas_block_concurrency
        .unwrap_or(avml::DEFAULT_CONCURRENCY);

    let block_client = BlobClient::new(args.sas_url, None, None)
        .map_err(BlobError::from)?
        .block_blob_client();
    let format = Format::from(args.compress);

    let source = match args.source {
        Some(s) => s,
        None => Snapshot::probe_single_source().map_err(avml::Error::from)?,
    };

    let stream = BlockBlobStream::new(block_client, block_size, concurrency);

    let (stream, result) = tokio::task::spawn_blocking(
        move || -> (BlockBlobStream, core::result::Result<(), avml::Error>) {
            let mut stream = stream;
            // Snapshot::create_to_writer never inspects `destination`;
            // any in-scope path satisfies the &Path borrow.
            let dummy = PathBuf::from("/dev/null");
            let snapshot = Snapshot::new(&dummy, ranges)
                .source(Some(source))
                .format(format);
            let r: core::result::Result<(), avml::Error> = snapshot
                .create_to_writer(stream.writer())
                .map_err(avml::Error::from)
                .and_then(|()| {
                    stream.finish_writes().map_err(|io_err| avml::Error::Io {
                        context: "unable to finish blob stream",
                        source: io_err,
                    })
                });
            (stream, r)
        },
    )
    .await
    .map_err(|e| avml::Error::Io {
        context: "spawn_blocking join failed",
        source: std::io::Error::other(e.to_string()),
    })?;

    match result {
        Ok(()) => stream.finalize().await.map_err(avml::Error::from),
        Err(e) => {
            drop(stream.abort().await);
            Err(e)
        }
    }
}

fn derive_block_size(
    ranges: &[Range<u64>],
    user_floor_mib: Option<NonZeroU64>,
) -> Result<NonZeroUsize> {
    /// 5 MiB — Azure's recommended minimum for high-throughput block blobs.
    const STREAM_MIN_BLOCK_SIZE: u64 = 5 * 1024 * 1024;
    /// 4000 MiB — Azure's documented per-block maximum.
    const STREAM_MAX_BLOCK_SIZE: u64 = 4000 * 1024 * 1024;
    /// Leave headroom below Azure's 50,000-block hard cap so we don't
    /// trip the limit on a slightly-over-estimate.
    const BLOCK_COUNT_HEADROOM: u64 = 1000;

    let estimate = ranges
        .iter()
        .map(|r| r.end.saturating_sub(r.start))
        .fold(0_u64, u64::saturating_add)
        .saturating_add(100 * 1024 * 1024); // overhead for headers + worst-case compression

    let target_block_count = BLOB_MAX_BLOCKS.saturating_sub(BLOCK_COUNT_HEADROOM).max(1);
    let derived_min = estimate.div_ceil(target_block_count);

    let user_floor_bytes =
        user_floor_mib.map_or(0, |mib| mib.get().saturating_mul(1024).saturating_mul(1024));

    let block_size = derived_min
        .max(STREAM_MIN_BLOCK_SIZE)
        .max(user_floor_bytes)
        .min(STREAM_MAX_BLOCK_SIZE);

    if estimate > STREAM_MAX_BLOCK_SIZE.saturating_mul(target_block_count) {
        return Err(avml::Error::Blob(BlobError::TooLarge));
    }

    NonZeroUsize::new(usize::try_from(block_size).map_err(|_| avml::Error::Io {
        context: "block size doesn't fit in usize",
        source: std::io::Error::other("block size overflow"),
    })?)
    .ok_or_else(|| avml::Error::Io {
        context: "block size derivation produced zero",
        source: std::io::Error::other("derived zero block size"),
    })
}
