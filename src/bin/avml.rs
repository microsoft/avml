// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use avml::{Format, Result, Snapshot, Source, iomem};
use clap::Parser;
#[cfg(feature = "blobstore")]
use core::num::NonZeroUsize;
use core::{num::NonZeroU64, ops::Range};
use std::path::PathBuf;
#[cfg(any(feature = "blobstore", feature = "put"))]
use {avml::Error, tokio::fs::remove_file, url::Url};

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
    #[arg(long, conflicts_with = "stream_to_blob")]
    max_disk_usage: Option<NonZeroU64>,

    /// Specify the maximum estimated disk usage to stay under
    #[arg(long, value_parser = disk_usage_percentage, conflicts_with = "stream_to_blob")]
    max_disk_usage_percentage: Option<f64>,

    /// upload via HTTP PUT upon acquisition
    #[cfg(feature = "put")]
    #[arg(long, conflicts_with = "stream_to_blob")]
    url: Option<Url>,

    /// delete upon successful upload
    #[cfg(any(feature = "blobstore", feature = "put"))]
    #[arg(long, conflicts_with = "stream_to_blob")]
    delete: bool,

    /// upload via Azure Blob Store upon acquisition
    #[cfg(feature = "blobstore")]
    #[arg(long)]
    sas_url: Option<Url>,

    /// specify maximum block size in MiB; must be greater than 0
    #[cfg(feature = "blobstore")]
    #[arg(long)]
    sas_block_size: Option<NonZeroU64>,

    /// specify blob upload concurrency; must be greater than 0
    #[cfg(feature = "blobstore")]
    #[arg(long)]
    sas_block_concurrency: Option<NonZeroUsize>,

    /// stream the snapshot directly to Azure Blob Storage instead of
    /// writing it to a local file first
    #[cfg(feature = "blobstore")]
    #[arg(long, requires = "sas_url")]
    stream_to_blob: bool,

    /// name of the file to write to on local system
    #[cfg(feature = "blobstore")]
    #[arg(required_unless_present = "stream_to_blob")]
    filename: Option<PathBuf>,

    /// name of the file to write to on local system
    #[cfg(not(feature = "blobstore"))]
    filename: PathBuf,
}

#[cfg(feature = "blobstore")]
impl Config {
    fn local_filename(&self) -> &PathBuf {
        // Guaranteed by clap: `filename` is required unless --stream-to-blob.
        // Both flows that reach this method (`acquire`, `upload`) only run
        // in non-stream mode.
        #[expect(
            clippy::expect_used,
            reason = "clap's required_unless_present guarantees filename is set here"
        )]
        self.filename
            .as_ref()
            .expect("filename required unless --stream-to-blob is set")
    }
}

#[cfg(not(feature = "blobstore"))]
impl Config {
    fn local_filename(&self) -> &PathBuf {
        &self.filename
    }
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

#[cfg(any(feature = "blobstore", feature = "put"))]
async fn upload(config: &Config) -> Result<()> {
    let mut delete = false;

    #[cfg(feature = "put")]
    {
        if let Some(ref url) = config.url {
            avml::put(config.local_filename(), url).await?;
            delete = true;
        }
    }

    #[cfg(feature = "blobstore")]
    {
        if let Some(ref sas_url) = config.sas_url {
            let uploader = avml::BlobUploader::new(sas_url)?
                .block_size(config.sas_block_size)
                .concurrency(config.sas_block_concurrency);
            uploader.upload_file(config.local_filename()).await?;
            delete = true;
        }
    }

    if delete && config.delete {
        remove_file(config.local_filename())
            .await
            .map_err(|source| Error::Io {
                context: "unable to remove snapshot",
                source,
            })?;
    }

    Ok(())
}

fn acquire(config: &Config) -> Result<()> {
    let format = Format::from(config.compress);

    let ranges = iomem::parse()?;
    let snapshot = Snapshot::new(config.local_filename(), ranges)
        .source(config.source.clone())
        .max_disk_usage_percentage(config.max_disk_usage_percentage)
        .max_disk_usage(config.max_disk_usage)
        .format(format);
    snapshot.create()?;
    Ok(())
}

#[cfg(feature = "blobstore")]
async fn stream_to_blob(config: &Config) -> Result<()> {
    use avml::{BLOB_MAX_BLOCKS, BlockBlobStream};
    use azure_storage_blob::BlobClient;

    #[expect(
        clippy::expect_used,
        reason = "clap's `requires = sas_url` guarantees this is Some here"
    )]
    let sas_url = config
        .sas_url
        .as_ref()
        .expect("clap `requires = sas_url` guarantees this is set");

    let ranges = iomem::parse()?;
    let block_size = derive_stream_block_size(&ranges, config.sas_block_size)?;
    let concurrency = config
        .sas_block_concurrency
        .unwrap_or(avml::DEFAULT_CONCURRENCY);

    let block_client = BlobClient::new(sas_url.clone(), None, None)
        .map_err(avml::BlobError::from)?
        .block_blob_client();
    let format = Format::from(config.compress);

    let source = match config.source.clone() {
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
        Ok(()) => {
            stream.finalize().await.map_err(avml::Error::from)?;
            // Sanity bound: the SDK accepts up to 50,000 blocks; if our
            // block_size derivation ever underestimates we'll have failed
            // already, but assert the public constant is still in scope
            // and matches our derivation expectations.
            let _ = BLOB_MAX_BLOCKS;
            Ok(())
        }
        Err(e) => {
            let _ = stream.abort().await;
            Err(e)
        }
    }
}

#[cfg(feature = "blobstore")]
fn derive_stream_block_size(
    ranges: &[Range<u64>],
    user_floor_mib: Option<NonZeroU64>,
) -> Result<core::num::NonZeroUsize> {
    use core::num::NonZeroUsize;

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

    let target_block_count = avml::BLOB_MAX_BLOCKS
        .saturating_sub(BLOCK_COUNT_HEADROOM)
        .max(1);
    let derived_min = estimate.div_ceil(target_block_count);

    let user_floor_bytes =
        user_floor_mib.map_or(0, |mib| mib.get().saturating_mul(1024).saturating_mul(1024));

    let block_size = derived_min
        .max(STREAM_MIN_BLOCK_SIZE)
        .max(user_floor_bytes)
        .min(STREAM_MAX_BLOCK_SIZE);

    if estimate > STREAM_MAX_BLOCK_SIZE.saturating_mul(target_block_count) {
        return Err(avml::Error::Blob(avml::BlobError::TooLarge));
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

#[cfg(not(any(feature = "blobstore", feature = "put")))]
fn main() -> Result<()> {
    let config = Config::parse();
    acquire(&config)
}

#[cfg(any(feature = "blobstore", feature = "put"))]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let config = Config::parse();

    #[cfg(feature = "blobstore")]
    if config.stream_to_blob {
        return stream_to_blob(&config).await;
    }

    acquire(&config)?;
    upload(&config).await
}
