// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::{upload::status::Status, ONE_MB};
use async_channel::{bounded, Receiver, Sender};
use azure_core::error::Error as AzureError;
use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use futures::future::try_join_all;
use std::{cmp, marker::Unpin, path::Path};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt},
};
use url::Url;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("file is too large")]
    TooLarge,

    #[error("unable to queue block for upload")]
    QueueBlock(#[from] async_channel::SendError<UploadBlock>),

    #[error("uploading blocks failed")]
    UploadFromQueue(#[source] tokio::task::JoinError),

    #[error("error reading file")]
    Io(#[from] std::io::Error),

    #[error("error uploading file")]
    Azure(#[from] AzureError),

    #[error("size conversion error")]
    SizeConversion,
}

type Result<T> = std::result::Result<T, Error>;

/// Maximum number of blocks
///
///  <https://docs.microsoft.com/en-us/azure/storage/blobs/scalability-targets#scale-targets-for-blob-storage>
const BLOB_MAX_BLOCKS: usize = 50_000;

/// Maximum size of any single block
///
///  <https://docs.microsoft.com/en-us/azure/storage/blobs/scalability-targets#scale-targets-for-blob-storage>
const BLOB_MAX_BLOCK_SIZE: usize = ONE_MB.saturating_mul(4000);

/// Maximum total size of a file
///
/// <https://docs.microsoft.com/en-us/azure/storage/blobs/scalability-targets#scale-targets-for-blob-storage>
const BLOB_MAX_FILE_SIZE: usize = BLOB_MAX_BLOCKS.saturating_mul(BLOB_MAX_BLOCK_SIZE);

/// Minimum block size, which is required to trigger the "high-throughput block
/// blobs" feature on all storage accounts
///
/// <https://azure.microsoft.com/en-us/blog/high-throughput-with-azure-blob-storage/>
const BLOB_MIN_BLOCK_SIZE: usize = ONE_MB.saturating_mul(5);

/// Azure's default max request rate for a storage account is 20,000 per second.
/// By keeping to 10 or fewer concurrent upload threads, AVML can be used to
/// simultaneously upload images from 1000 different hosts concurrently (a full
/// VM scaleset) to a single default storage account.
///
/// <https://docs.microsoft.com/en-us/azure/storage/common/scalability-targets-standard-account#scale-targets-for-standard-storage-accounts>
const MAX_CONCURRENCY: usize = 10;

/// Azure's default max request rate for a storage account is 20,000 per second.
/// By keeping to 10 or fewer concurrent upload threads, AVML can be used to
/// simultaneously upload images from 1000 different hosts concurrently (a full
/// VM scaleset) to a single default storage account.
///
/// <https://docs.microsoft.com/en-us/azure/storage/common/scalability-targets-standard-account#scale-targets-for-standard-storage-accounts>
pub const DEFAULT_CONCURRENCY: usize = 10;

/// As chunks stay in memory until the upload is complete, as to enable
/// automatic retries in the case of TCP or HTTP errors, chunks sizes for huge
/// files is capped to 100MB each
const REASONABLE_BLOCK_SIZE: usize = ONE_MB.saturating_mul(100);

/// try to keep under 500MB in flight.  If that's not possible due to block
/// size, concurrency will get disabled.
const MEMORY_THRESHOLD: usize = 500 * ONE_MB;

/// When uploading a file without a size, such as when uploading a stream of an
/// unknown size, use a 1TB stream
const DEFAULT_FILE_SIZE: usize = 1024 * 1024 * 1024 * 1024;

pub struct UploadBlock {
    id: Bytes,
    data: Bytes,
}

fn calc_concurrency(
    file_size: usize,
    block_size: Option<usize>,
    upload_concurrency: usize,
) -> Result<(usize, usize)> {
    if file_size > BLOB_MAX_FILE_SIZE {
        return Err(Error::TooLarge);
    }

    let block_size = match block_size {
        // if the user specifies a block size of 0 or doesn't specify a block size,
        // calculate the block size based on the file size
        Some(0) | None => {
            match file_size {
                // if the file is small enough to fit with 5MB blocks, use that
                // to reduce impact for failure retries and increase
                // concurrency.
                x if (x < BLOB_MIN_BLOCK_SIZE * BLOB_MAX_BLOCKS) => BLOB_MIN_BLOCK_SIZE,
                // if the file is large enough that we can fit with 100MB blocks, use that.
                x if (x < REASONABLE_BLOCK_SIZE * BLOB_MAX_BLOCKS) => REASONABLE_BLOCK_SIZE,
                // otherwise, just use the smallest block size that will fit
                // within MAX BLOCKS to reduce memory pressure
                x => (x / BLOB_MAX_BLOCKS) + 1,
            }
        }
        // minimum required to hit high-throughput block blob performance thresholds
        Some(x) if (x <= BLOB_MIN_BLOCK_SIZE) => BLOB_MIN_BLOCK_SIZE,
        // otherwise use the user specified value
        Some(x) => x,
    };

    // if the block size is larger than the max block size, use the max block size
    let block_size = usize::min(block_size, BLOB_MAX_BLOCK_SIZE);

    let upload_concurrency = match upload_concurrency {
        // manually specifying concurrency of 0 will disable concurrency
        0 | 1 => 1,
        _ => match (MEMORY_THRESHOLD).saturating_div(block_size) {
            0 => 1,
            // cap the number of concurrent threads to reduce concurrency issues
            // at the server end.
            x => cmp::min(MAX_CONCURRENCY, x),
        },
    };

    Ok((block_size, upload_concurrency))
}

/// Concurrently upload a Stream/File to an Azure Blob Store using a SAS URL.
///
/// ```rust,no_run
/// use avml::BlobUploader;
/// # use url::Url;
/// # use avml::Result;
/// # use std::path::Path;
/// # async fn upload() -> Result<()> {
/// let sas_url = Url::parse("https://contoso.com/container_name/blob_name?sas_token_here=1")
///     .expect("url parsing failed");
/// let path = Path::new("/tmp/image.lime");
/// let uploader = BlobUploader::new(&sas_url)?
///     .block_size(Some(100))
///     .concurrency(5);
/// uploader.upload_file(&path).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct BlobUploader {
    client: BlobClient,
    size: usize,
    block_size: Option<usize>,
    concurrency: usize,
    sender: Sender<UploadBlock>,
    receiver: Receiver<UploadBlock>,
}

impl BlobUploader {
    pub fn new(sas: &Url) -> Result<Self> {
        let blob_client = BlobClient::from_sas_url(sas)?;
        Ok(Self::with_blob_client(blob_client))
    }

    /// Create a ``BlobUploader`` with a ``BlobClient`` from ``azure_storage_blobs``.
    ///
    /// Ref: <https://docs.rs/azure_storage_blobs/latest/azure_storage_blobs/prelude/struct.BlobClient.html>
    #[must_use]
    pub fn with_blob_client(client: BlobClient) -> Self {
        let (sender, receiver) = bounded::<UploadBlock>(1);

        Self {
            client,
            size: DEFAULT_FILE_SIZE,
            block_size: None,
            concurrency: DEFAULT_CONCURRENCY,
            sender,
            receiver,
        }
    }

    /// Specify the size of the file to upload (in bytes)
    ///
    /// If the anticipated upload size is not specified, the maximum file
    /// uploaded will be approximately 5TB.
    #[must_use]
    pub fn size(self, size: usize) -> Self {
        Self { size, ..self }
    }

    /// Specify the block size in multiples of 1MB
    ///
    /// If the block size is not specified and the size of the content to be
    /// uploaded is provided, the default block size will be calculated to fit
    /// within the bounds of the allowed number of blocks and the minimum
    /// minimum required to hit high-throughput block blob performance
    /// thresholds.
    #[must_use]
    pub fn block_size(self, block_size: Option<usize>) -> Self {
        Self { block_size, ..self }
    }

    #[must_use]
    pub fn concurrency(self, concurrency: usize) -> Self {
        Self {
            concurrency,
            ..self
        }
    }

    /// Upload a file to Azure Blob Store using a fully qualified SAS token
    pub async fn upload_file(mut self, filename: &Path) -> Result<()> {
        let file = File::open(filename).await?;

        let file_size = file
            .metadata()
            .await?
            .len()
            .try_into()
            .map_err(|_| Error::SizeConversion)?;

        self.size = file_size;

        self.upload_stream(file).await
    }

    async fn finalize(self, block_ids: Vec<Bytes>) -> Result<()> {
        let blocks = block_ids
            .into_iter()
            .map(|x| BlobBlockType::Uncommitted(BlockId::new(x)))
            .collect::<Vec<_>>();

        let block_list = BlockList { blocks };

        self.client.put_block_list(block_list).await?;

        Ok(())
    }

    /// upload a stream to Azure Blob Store using a fully qualified SAS token
    async fn upload_stream<R>(self, handle: R) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
    {
        let block_size = self.block_size.map(|x| x.saturating_mul(ONE_MB));

        let (block_size, uploaders_count) =
            calc_concurrency(self.size, block_size, self.concurrency)?;

        let uploaders = self.uploaders(uploaders_count);
        let queue_handle = self.block_reader(handle, block_size);

        let (block_list, ()) = futures::try_join!(queue_handle, uploaders)?;

        self.finalize(block_list).await
    }

    async fn uploaders(&self, count: usize) -> Result<()> {
        let status = Status::new(Some(
            self.size.try_into().map_err(|_| Error::SizeConversion)?,
        ));

        let uploaders: Vec<_> = (0..usize::max(1, count))
            .map(|_| {
                Self::block_uploader(self.client.clone(), self.receiver.clone(), status.clone())
            })
            .collect();

        try_join_all(uploaders).await?;

        Ok(())
    }

    async fn block_reader<R>(&self, mut handle: R, block_size: usize) -> Result<Vec<Bytes>>
    where
        R: AsyncRead + Unpin + Send,
    {
        let mut block_list = vec![];

        for i in 0..usize::MAX {
            let mut data = Vec::with_capacity(block_size);

            let mut take_handle = handle.take(block_size as u64);
            let read_data = take_handle.read_to_end(&mut data).await?;
            if read_data == 0 {
                break;
            }
            handle = take_handle.into_inner();

            if data.is_empty() {
                break;
            }

            let data = data.into();

            let id = Bytes::from(format!("{i:032x}"));

            block_list.push(id.clone());

            self.sender.send(UploadBlock { id, data }).await?;
        }
        self.sender.close();

        Ok(block_list)
    }

    async fn block_uploader(
        client: BlobClient,
        receiver: Receiver<UploadBlock>,
        status: Status,
    ) -> Result<()> {
        // the channel will respond with an Err to indicate the channel is closed
        while let Ok(upload_chunk) = receiver.recv().await {
            let hash = md5::compute(upload_chunk.data.clone());

            let chunk_len = upload_chunk.data.len();

            let result = client
                .put_block(upload_chunk.id, upload_chunk.data)
                .hash(hash)
                .await;

            // as soon as any error is seen (after retrying), bail out and stop other uploaders
            if result.is_err() {
                receiver.close();
                result?;
            }

            status.inc(chunk_len);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_GB: usize = ONE_MB.saturating_mul(1024);
    const ONE_TB: usize = ONE_GB.saturating_mul(1024);

    #[test]
    fn test_calc_concurrency() -> Result<()> {
        assert_eq!(
            (BLOB_MIN_BLOCK_SIZE, 10),
            calc_concurrency(ONE_MB * 300, Some(1), DEFAULT_CONCURRENCY)?,
            "specified blocksize would overflow block count, so we use the minimum block size"
        );

        assert_eq!(
            (BLOB_MIN_BLOCK_SIZE, 10),
            calc_concurrency(ONE_GB * 30, Some(ONE_MB), DEFAULT_CONCURRENCY)?,
            "30GB file, 1MB blocks"
        );

        assert_eq!(
            (ONE_MB * 100, 5),
            calc_concurrency(ONE_GB * 30, Some(ONE_MB * 100), DEFAULT_CONCURRENCY)?,
            "30GB file, 100MB block size"
        );

        assert_eq!(
            (5 * ONE_MB, 10),
            calc_concurrency(ONE_MB * 400, None, DEFAULT_CONCURRENCY)?,
            "400MB file, no block size"
        );

        assert_eq!(
            (5 * ONE_MB, 10),
            calc_concurrency(ONE_GB * 16, None, DEFAULT_CONCURRENCY)?,
            "16GB file, no block size"
        );

        assert_eq!(
            (5 * ONE_MB, 10),
            calc_concurrency(ONE_GB * 32, None, DEFAULT_CONCURRENCY)?,
            "32GB file, no block size",
        );

        assert_eq!(
            (ONE_MB * 100, 5),
            calc_concurrency(ONE_TB, None, DEFAULT_CONCURRENCY)?,
            "1TB file, no block size"
        );

        assert_eq!(
            (100 * ONE_MB, 5),
            calc_concurrency(ONE_TB * 4, None, DEFAULT_CONCURRENCY)?,
            "4TB file, no block size"
        );

        assert_eq!(
            (100 * ONE_MB, 5),
            calc_concurrency(ONE_TB * 4, Some(0), DEFAULT_CONCURRENCY)?,
            "4TB file, zero block size"
        );

        let (block_size, uploaders_count) =
            calc_concurrency(ONE_TB.saturating_mul(32), None, DEFAULT_CONCURRENCY)?;
        assert!(block_size > REASONABLE_BLOCK_SIZE && block_size < BLOB_MAX_BLOCK_SIZE);
        assert_eq!(uploaders_count, 1);

        assert!(
            calc_concurrency((BLOB_MAX_BLOCKS * BLOB_MAX_BLOCK_SIZE) + 1, None, 10).is_err(),
            "files beyond max size should fail"
        );
        Ok(())
    }
}
