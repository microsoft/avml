// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::{upload::status::Status, ONE_MB};
use async_channel::{bounded, Receiver, Sender};
use azure_core::{
    error::{Error as AzureError, ErrorKind},
    new_http_client,
};
use azure_storage::core::prelude::*;
use azure_storage_blobs::prelude::*;
use backoff::{future::retry, ExponentialBackoff};
use bytes::Bytes;
use futures::future::try_join_all;
use http::StatusCode;
use std::{cmp, convert::TryFrom, marker::Unpin, path::Path, sync::Arc};
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

    #[error("Invalid SAS token: {0}")]
    InvalidSasToken(&'static str),

    #[error("size conversion error")]
    SizeConversion,
}

type Result<T> = std::result::Result<T, Error>;

// https://docs.microsoft.com/en-us/azure/storage/blobs/scalability-targets#scale-targets-for-blob-storage
const BLOB_MAX_BLOCKS: usize = 50_000;
const BLOB_MAX_BLOCK_SIZE: usize = ONE_MB.saturated_mul(4000);
const BLOB_MAX_FILE_SIZE: usize = BLOB_MAX_BLOCKS.saturated_mul(BLOB_MAX_BLOCK_SIZE);

// trigger's the "high-throughput block blobs" on all storage accounts
// https://azure.microsoft.com/en-us/blog/high-throughput-with-azure-blob-storage/
const BLOB_MIN_BLOCK_SIZE: usize = ONE_MB.saturated_mul(5);

// Azure's default max request rate for a storage account is 20,000 per second.
// By keeping to 10 or fewer concurrent upload threads, AVML can be used to
// simultaneously upload images from 1000 different hosts concurrently (a full
// VM scaleset) to a single default storage account.
//
// https://docs.microsoft.com/en-us/azure/storage/common/scalability-targets-standard-account#scale-targets-for-standard-storage-accounts
const MAX_CONCURRENCY: usize = 10;

// if we're uploading *huge* files, use 100MB chunks
const REASONABLE_BLOCK_SIZE: usize = ONE_MB.saturated_mul(100);

pub struct UploadBlock {
    id: Bytes,
    data: Bytes,
}

struct SasToken {
    account: String,
    container: String,
    path: String,
    token: String,
}

fn check_transient(err: AzureError) -> backoff::Error<Error> {
    if let ErrorKind::HttpResponse { status, .. } = err.kind() {
        if let Ok(status) = StatusCode::from_u16(*status) {
            if !(status.is_redirection()
                || status.is_server_error()
                || status == StatusCode::TOO_MANY_REQUESTS)
            {
                return backoff::Error::permanent(err.into());
            }
        }
    }
    eprintln!("transient error: {}", err);
    backoff::Error::transient(err.into())
}

impl TryFrom<&Url> for SasToken {
    type Error = Error;

    fn try_from(url: &Url) -> Result<Self> {
        let account = url
            .host_str()
            .ok_or(Error::InvalidSasToken("missing host"))?
            .split_terminator('.')
            .next()
            .ok_or(Error::InvalidSasToken("unable to determine account name"))?
            .to_string();

        let token = url
            .query()
            .ok_or(Error::InvalidSasToken("missing token"))?
            .to_string();

        let path = url.path();
        let mut v: Vec<&str> = path.split_terminator('/').collect();
        v.remove(0);
        let container = v.remove(0).to_string();
        let path = v.join("/");

        if path.is_empty() {
            return Err(Error::InvalidSasToken("missing blob name"));
        }

        Ok(Self {
            account,
            container,
            path,
            token,
        })
    }
}

fn calc_concurrency(
    file_size: Option<usize>,
    block_size: Option<usize>,
    upload_concurrency: Option<usize>,
) -> Result<(usize, usize)> {
    if let Some(file_size) = file_size {
        if file_size > BLOB_MAX_FILE_SIZE {
            return Err(Error::TooLarge);
        }
    }

    let block_size = match block_size {
        // if the user specifies a block size of 0 or doesn't specify a block size,
        // calculate the block size based on the file size
        Some(0) | None => {
            match file_size {
                // if the file is small enough to fit with 5MB blocks, use that
                // to reduce impact for failure retries and increase
                // concurrency.
                Some(x) if (x < BLOB_MIN_BLOCK_SIZE * BLOB_MAX_BLOCKS) => BLOB_MIN_BLOCK_SIZE,
                // if the file is large enough that we can fit with 100MB blocks, use that.
                Some(x) if (x < REASONABLE_BLOCK_SIZE * BLOB_MAX_BLOCKS) => REASONABLE_BLOCK_SIZE,
                // otherwise, just use the smallest block size that will fit
                // within MAX BLOCKS to reduce memory pressure
                Some(x) => (x / BLOB_MAX_BLOCKS) + 1,
                None => REASONABLE_BLOCK_SIZE,
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
        Some(0) => 1,
        Some(x) => x,
        // if the user didn't specify concurrency, always try to keep under
        // 200MB in flight.  If that's not possible due to block size, disable
        // concurrency.
        None => match (200 * ONE_MB).saturating_div(block_size) {
            0 => 1,
            // cap the number of concurrent threads to reduce concurrency issues
            // at the server end.
            x => cmp::min(MAX_CONCURRENCY, x),
        },
    };

    Ok((block_size, upload_concurrency))
}

#[derive(Clone)]
pub struct BlobUploader {
    client: Arc<BlobClient>,
    size: Option<usize>,
    block_size: Option<usize>,
    concurrency: Option<usize>,
    sender: Sender<UploadBlock>,
    receiver: Receiver<UploadBlock>,
}

impl BlobUploader {
    pub fn new(sas: &Url) -> Result<Self> {
        let sas: SasToken = sas.try_into()?;

        let http_client = new_http_client();
        let blob_client =
            StorageAccountClient::new_sas_token(http_client, &sas.account, &sas.token)?
                .as_storage_client()
                .as_container_client(sas.container)
                .as_blob_client(sas.path);

        Ok(Self::with_blob_client(blob_client))
    }

    /// Create a ``BlobUploader`` with a ``BlobClient`` from ``azure_storage_blobs``.
    ///
    /// Ref: <https://docs.rs/azure_storage_blobs/latest/azure_storage_blobs/prelude/struct.BlobClient.html>
    #[must_use]
    pub fn with_blob_client(client: Arc<BlobClient>) -> Self {
        let (sender, receiver) = bounded::<UploadBlock>(1);

        Self {
            client,
            size: None,
            block_size: None,
            concurrency: None,
            sender,
            receiver,
        }
    }

    /// Specify the size of the file to upload (in bytes)
    ///
    /// If the anticipated upload size is not specified, the maximum file
    /// uploaded will be approximately 5TB.
    #[must_use]
    pub fn size(self, size: Option<usize>) -> Self {
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
    pub fn concurrency(self, concurrency: Option<usize>) -> Self {
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
            .map(Some)
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

        retry(ExponentialBackoff::default(), || async {
            self.client
                .put_block_list(&block_list)
                .execute()
                .await
                .map_err(check_transient)
        })
        .await?;

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
        let status = Status::new(self.size.map(|x| x as u64));

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

            let id = Bytes::from(format!("{:032x}", i));

            block_list.push(id.clone());

            self.sender.send(UploadBlock { id, data }).await?;
        }
        self.sender.close();

        Ok(block_list)
    }

    async fn block_uploader(
        client: Arc<BlobClient>,
        receiver: Receiver<UploadBlock>,
        status: Status,
    ) -> Result<()> {
        // the channel will respond with an Err to indicate the channel is closed
        while let Ok(upload_chunk) = receiver.recv().await {
            let hash = md5::compute(upload_chunk.data.clone()).into();

            let result = retry(ExponentialBackoff::default(), || async {
                let data_for_req = upload_chunk.data.clone();
                let block_id_for_req = upload_chunk.id.clone();

                client
                    .put_block(block_id_for_req, data_for_req)
                    .hash(&hash)
                    .execute()
                    .await
                    .map_err(check_transient)
            })
            .await;

            status.inc(upload_chunk.data.len());

            // as soon as any error is seen (after retrying), bail out and stop other uploaders
            if result.is_err() {
                receiver.close();
                result?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_GB: usize = ONE_MB.saturated_mul(1024);
    const ONE_TB: usize = ONE_GB.saturated_mul(1024);

    #[test]
    fn test_parse_sas_url() -> Result<()> {
        let url = &reqwest::Url::parse(
            "https://myaccount.blob.core.windows.net/mycontainer/myblob?sas=data&here=1",
        )
        .map_err(|_| Error::InvalidSasToken("unable to parse url"))?;
        let _token: SasToken = url.try_into()?;

        let url = &reqwest::Url::parse(
            "https://myaccount.blob.core.windows.net/mycontainer?sas=data&here=1",
        )
        .map_err(|_| Error::InvalidSasToken("unable to parse url"))?;
        let result: Result<SasToken> = url.try_into();
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_calc_concurrency() -> Result<()> {
        assert_eq!(
            (BLOB_MIN_BLOCK_SIZE, 1),
            calc_concurrency(Some(ONE_MB * 300), Some(1), Some(1))?,
            "specified blocksize would overflow block count, so we use the minimum block size"
        );

        assert_eq!(
            (BLOB_MIN_BLOCK_SIZE, 10),
            calc_concurrency(Some(ONE_GB * 30), Some(ONE_MB), None)?,
            "specifying block size of ONE_MB"
        );

        assert_eq!(
            (ONE_MB * 100, 2),
            calc_concurrency(Some(ONE_GB * 30), Some(ONE_MB * 100), None)?,
            "specifying block size of 100MB but no concurrency"
        );

        assert_eq!(
            (5 * ONE_MB, 10),
            calc_concurrency(Some(ONE_MB * 400), None, None)?,
            "uploading 400MB file, 5MB chunks, 10 uploaders",
        );

        assert_eq!(
            (5 * ONE_MB, 10),
            calc_concurrency(Some(ONE_GB * 16), None, None)?,
            "uploading 50,000 MB file.   5MB chunks, 10 uploaders",
        );

        assert_eq!(
            (5 * ONE_MB, 10),
            calc_concurrency(Some(ONE_GB * 32), None, None)?,
            "uploading 32GB file"
        );

        assert_eq!(
            (ONE_MB * 100, 2),
            calc_concurrency(Some(ONE_TB), None, None)?,
            "uploading 1TB file"
        );

        assert_eq!(
            (100 * ONE_MB, 2),
            calc_concurrency(Some(ONE_TB * 4), None, None)?,
            "uploading 5TB file.  100MB chunks, 2 uploaders"
        );

        assert_eq!(
            (100 * ONE_MB, 2),
            calc_concurrency(Some(ONE_TB * 4), Some(0), None)?,
            "uploading 5TB file with zero blocksize.  100MB chunks, 2 uploaders"
        );

        assert_eq!(
            (100 * ONE_MB, 1),
            calc_concurrency(Some(ONE_TB * 4), None, Some(0))?,
            "uploading 5TB file with zero concurrency.  100MB chunks, 1 uploader"
        );

        let (block_size, uploaders_count) =
            calc_concurrency(Some(ONE_TB.saturated_mul(32)), None, None)?;
        assert!(block_size > REASONABLE_BLOCK_SIZE && block_size < BLOB_MAX_BLOCK_SIZE);
        assert_eq!(uploaders_count, 1);

        assert!(
            calc_concurrency(
                Some((BLOB_MAX_BLOCKS * BLOB_MAX_BLOCK_SIZE) + 1),
                None,
                None
            )
            .is_err(),
            "files beyond max size should fail"
        );
        Ok(())
    }
}
