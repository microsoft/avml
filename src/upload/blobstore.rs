// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::upload::status::Status;
use azure_core::{
    Bytes,
    error::Error as AzureError,
    http::{Body, NoFormat, RequestContent},
    stream::SeekableStream,
};
use azure_storage_blob::{BlobClient, models::BlobClientUploadOptions, stream::tokio::FileStream};
use core::{
    cmp,
    num::{NonZeroU64, NonZeroUsize},
    pin::Pin,
    task::{Context, Poll},
};
use std::{path::Path, sync::Arc};
use tokio::fs::File;
use url::Url;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("file is too large")]
    TooLarge,

    #[error("error reading file")]
    Io(#[from] std::io::Error),

    #[error("error uploading file")]
    Azure(#[from] AzureError),

    #[error(transparent)]
    IntConversion(#[from] core::num::TryFromIntError),

    #[error(transparent)]
    InvalidUrl(#[from] url::ParseError),
}

type Result<T> = core::result::Result<T, Error>;

#[allow(clippy::expect_used)]
const ONE_MB_NZ: NonZeroU64 = NonZeroU64::new(1024 * 1024).expect("ONE_MB must be non-zero");

/// Maximum number of blocks
///
/// <https://docs.microsoft.com/en-us/azure/storage/blobs/scalability-targets#scale-targets-for-blob-storage>
#[allow(clippy::expect_used)]
const BLOB_MAX_BLOCKS: NonZeroU64 =
    NonZeroU64::new(50_000).expect("blob max blocks must be non-zero");

/// Maximum size of any single block
///
/// <https://docs.microsoft.com/en-us/azure/storage/blobs/scalability-targets#scale-targets-for-blob-storage>
#[allow(clippy::expect_used)]
const BLOB_MAX_BLOCK_SIZE: NonZeroU64 = ONE_MB_NZ.saturating_mul(
    NonZeroU64::new(4000).expect("blob max block size multiplier must be non-zero"),
);

/// Maximum total size of a file
///
/// <https://docs.microsoft.com/en-us/azure/storage/blobs/scalability-targets#scale-targets-for-blob-storage>
#[allow(clippy::expect_used)]
const BLOB_MAX_FILE_SIZE: NonZeroU64 = BLOB_MAX_BLOCKS.saturating_mul(BLOB_MAX_BLOCK_SIZE);

/// Minimum block size, which is required to trigger the "high-throughput block
/// blobs" feature on all storage accounts
///
/// <https://azure.microsoft.com/en-us/blog/high-throughput-with-azure-blob-storage/>
#[allow(clippy::expect_used)]
const BLOB_MIN_BLOCK_SIZE: NonZeroU64 = ONE_MB_NZ
    .saturating_mul(NonZeroU64::new(5).expect("blob min block size multiplier must be non-zero"));

/// Azure's default max request rate for a storage account is 20,000 per second.
/// By keeping to 10 or fewer concurrent upload threads, AVML can be used to
/// simultaneously upload images from 1000 different hosts concurrently (a full
/// VM scaleset) to a single default storage account.
///
/// <https://docs.microsoft.com/en-us/azure/storage/common/scalability-targets-standard-account#scale-targets-for-standard-storage-accounts>
#[allow(clippy::expect_used)]
const MAX_CONCURRENCY: NonZeroUsize =
    NonZeroUsize::new(10).expect("max concurrency must be non-zero");

/// Azure's default max request rate for a storage account is 20,000 per second.
/// By keeping to 10 or fewer concurrent upload threads, AVML can be used to
/// simultaneously upload images from 1000 different hosts concurrently (a full
/// VM scaleset) to a single default storage account.
///
/// <https://docs.microsoft.com/en-us/azure/storage/common/scalability-targets-standard-account#scale-targets-for-standard-storage-accounts>
#[allow(clippy::expect_used)]
pub const DEFAULT_CONCURRENCY: NonZeroUsize =
    NonZeroUsize::new(10).expect("default concurrency must be non-zero");

/// Keep at most 500MB of block data in flight across all uploaders.
#[allow(clippy::expect_used)]
const MEMORY_THRESHOLD: NonZeroU64 = ONE_MB_NZ
    .saturating_mul(NonZeroU64::new(500).expect("memory threshold multiplier must be non-zero"));

fn calc_block_size(file_size: NonZeroU64, block_size: Option<NonZeroU64>) -> Result<NonZeroU64> {
    let block_size = match block_size {
        Some(block_size) => block_size,
        None => NonZeroU64::new(file_size.get().div_ceil(BLOB_MAX_BLOCKS.get()))
            .ok_or(Error::TooLarge)?,
    };

    Ok(cmp::min(
        cmp::max(block_size, BLOB_MIN_BLOCK_SIZE),
        BLOB_MAX_BLOCK_SIZE,
    ))
}

fn calc_concurrency(
    file_size: NonZeroU64,
    block_size: Option<NonZeroU64>,
    upload_concurrency: Option<NonZeroUsize>,
) -> Result<(NonZeroU64, NonZeroUsize)> {
    if file_size > BLOB_MAX_FILE_SIZE {
        return Err(Error::TooLarge);
    }
    let block_size = calc_block_size(file_size, block_size)?;

    let memory_limited_concurrency = NonZeroUsize::new(
        usize::try_from(
            MEMORY_THRESHOLD
                .get()
                .checked_div(block_size.get())
                .unwrap_or(0),
        )
        .unwrap_or(usize::MAX),
    )
    .unwrap_or(NonZeroUsize::MIN);
    let upload_concurrency = cmp::min(
        cmp::min(
            upload_concurrency.unwrap_or(DEFAULT_CONCURRENCY),
            memory_limited_concurrency,
        ),
        MAX_CONCURRENCY,
    );

    Ok((block_size, upload_concurrency))
}

fn upload_parameters(
    file_size: NonZeroU64,
    block_size: Option<NonZeroU64>,
    upload_concurrency: Option<NonZeroUsize>,
) -> Result<(NonZeroU64, NonZeroUsize)> {
    let block_size = block_size.map(|x| x.saturating_mul(ONE_MB_NZ));
    calc_concurrency(file_size, block_size, upload_concurrency)
}

/// A [`SeekableStream`] wrapper that delegates to
/// [`azure_storage_blob::stream::tokio::FileStream`] and reports upload
/// progress via [`Status`] as bytes are read by the Azure SDK.
#[derive(Debug, Clone)]
struct ProgressStream {
    inner: FileStream,
    status: Status,
}

impl ProgressStream {
    async fn new(file: File, file_size: u64) -> Result<Self> {
        let inner = FileStream::builder(file).build().await?;
        Ok(Self {
            inner,
            status: Status::new(Some(file_size)),
        })
    }
}

#[async_trait::async_trait]
impl SeekableStream for ProgressStream {
    async fn reset(&mut self) -> azure_core::Result<()> {
        self.inner.reset().await?;
        self.status.reset();
        Ok(())
    }

    fn len(&self) -> Option<u64> {
        self.inner.len()
    }

    fn buffer_size(&self) -> usize {
        self.inner.buffer_size()
    }
}

impl futures::io::AsyncRead for ProgressStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slice: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_read(cx, slice) {
            Poll::Ready(Ok(n)) => {
                this.status.inc(n);
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }
}

/// Upload a file to Azure Blob Store using a SAS URL.
///
/// ```rust,no_run
/// use avml::BlobUploader;
/// # use avml::Result;
/// # use std::{num::{NonZeroU64, NonZeroUsize}, path::Path};
/// # use url::Url;
/// # async fn upload() -> Result<()> {
/// let sas_url = Url::parse("https://contoso.com/container_name/blob_name?sas_token_here=1")
///     .expect("url parsing failed");
/// let path = Path::new("/tmp/image.lime");
/// let uploader = BlobUploader::new(&sas_url)?
///     .block_size(NonZeroU64::new(100))
///     .concurrency(NonZeroUsize::new(5));
/// uploader.upload_file(&path).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct BlobUploader {
    client: Arc<BlobClient>,
    block_size: Option<NonZeroU64>,
    concurrency: Option<NonZeroUsize>,
}

impl BlobUploader {
    /// Create a new ``BlobUploader`` from a SAS URL.
    ///
    /// The URL must point at a specific blob (e.g. `https://<account>.blob.core.windows.net/<container>/<blob>?<sas-token>`).
    /// SAS authentication is carried inline in the URL's query string; this
    /// constructor does not attach a separate credential.
    ///
    /// # Errors
    /// Propagates any error returned by
    /// [`BlobClient::from_url`](azure_storage_blob::BlobClient::from_url),
    /// for example if the URL shape is not supported by the Azure SDK.
    pub fn new(sas: &Url) -> Result<Self> {
        let blob_client = BlobClient::from_url(sas.clone(), None, None)?;
        Ok(Self::with_blob_client(blob_client))
    }

    /// Create a ``BlobUploader`` with a ``BlobClient`` from ``azure_storage_blob``.
    ///
    /// Ref: <https://docs.rs/azure_storage_blob/latest/azure_storage_blob/struct.BlobClient.html>
    #[must_use]
    pub fn with_blob_client(client: BlobClient) -> Self {
        Self {
            client: Arc::new(client),
            block_size: None,
            concurrency: None,
        }
    }

    /// Specify a positive block size in multiples of 1MB.
    #[must_use]
    pub fn block_size(self, block_size: Option<NonZeroU64>) -> Self {
        Self { block_size, ..self }
    }

    /// Specify a positive upload concurrency.
    #[must_use]
    pub fn concurrency(self, concurrency: Option<NonZeroUsize>) -> Self {
        Self {
            concurrency,
            ..self
        }
    }

    /// Upload a file to Azure Blob Store using a fully qualified SAS token.
    ///
    /// Empty files are uploaded as zero-length blobs.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The file cannot be opened or read
    /// - The file is too large for Azure Blob Storage
    /// - There is a failure during the upload process
    pub async fn upload_file(self, filename: &Path) -> Result<()> {
        let file = File::open(filename).await?;
        let file_size = file.metadata().await?.len();

        let stream = ProgressStream::new(file, file_size).await?;
        let stream: Box<dyn SeekableStream> = Box::new(stream);
        let content: RequestContent<Bytes, NoFormat> = Body::from(stream).into();

        let options = if let Some(file_size) = NonZeroU64::new(file_size) {
            let (block_size, uploaders_count) =
                upload_parameters(file_size, self.block_size, self.concurrency)?;
            BlobClientUploadOptions {
                parallel: Some(uploaders_count),
                partition_size: Some(block_size),
                ..Default::default()
            }
        } else {
            // Empty files: let the SDK upload a zero-length blob without
            // needing partition/parallelism parameters.
            BlobClientUploadOptions::default()
        };

        self.client.upload(content, Some(options)).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ONE_MB;
    use futures::AsyncReadExt as _;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn non_zero(value: u64) -> Result<NonZeroU64> {
        NonZeroU64::new(value).ok_or(Error::TooLarge)
    }

    fn non_zero_usize(value: usize) -> Result<NonZeroUsize> {
        NonZeroUsize::new(value).ok_or(Error::TooLarge)
    }

    fn bytes_from_mib(mebibytes: u64) -> Result<NonZeroU64> {
        non_zero(
            mebibytes
                .checked_mul(u64::try_from(ONE_MB)?)
                .ok_or(Error::TooLarge)?,
        )
    }

    fn bytes_from_gib(gibibytes: u64) -> Result<NonZeroU64> {
        bytes_from_mib(gibibytes.checked_mul(1024).ok_or(Error::TooLarge)?)
    }

    #[test]
    fn small_files_use_minimum_block_size_and_default_concurrency() -> Result<()> {
        let (block_size, concurrency) = upload_parameters(bytes_from_mib(400)?, None, None)?;

        assert_eq!(block_size, BLOB_MIN_BLOCK_SIZE);
        assert_eq!(concurrency, DEFAULT_CONCURRENCY);
        Ok(())
    }

    #[test]
    fn user_block_size_is_clamped_to_minimum() -> Result<()> {
        let (block_size, concurrency) =
            upload_parameters(bytes_from_mib(300)?, Some(NonZeroU64::MIN), None)?;

        assert_eq!(block_size, BLOB_MIN_BLOCK_SIZE);
        assert_eq!(concurrency, DEFAULT_CONCURRENCY);
        Ok(())
    }

    #[test]
    fn requested_concurrency_caps_memory_limited_uploaders() -> Result<()> {
        let (block_size, concurrency) = upload_parameters(
            bytes_from_gib(30)?,
            Some(non_zero(100)?),
            Some(non_zero_usize(3)?),
        )?;

        assert_eq!(block_size, bytes_from_mib(100)?);
        assert_eq!(concurrency, non_zero_usize(3)?);
        Ok(())
    }

    #[test]
    fn auto_block_size_grows_when_minimum_would_exceed_max_blocks() -> Result<()> {
        let max_blocks = BLOB_MAX_BLOCKS.get();
        let file_size = non_zero(
            BLOB_MIN_BLOCK_SIZE
                .get()
                .checked_mul(max_blocks)
                .ok_or(Error::TooLarge)?
                .checked_add(1)
                .ok_or(Error::TooLarge)?,
        )?;
        let expected_block_size = non_zero(file_size.get().div_ceil(max_blocks))?;
        let (block_size, concurrency) = upload_parameters(file_size, None, None)?;

        assert_eq!(block_size, expected_block_size);
        assert_eq!(concurrency, DEFAULT_CONCURRENCY);
        Ok(())
    }

    #[test]
    fn huge_blocks_still_use_at_least_one_uploader() -> Result<()> {
        let (block_size, concurrency) =
            upload_parameters(bytes_from_gib(30)?, Some(non_zero(600)?), None)?;

        assert_eq!(block_size, bytes_from_mib(600)?);
        assert_eq!(concurrency, NonZeroUsize::MIN);
        Ok(())
    }

    #[test]
    fn files_larger_than_azure_limit_are_rejected() -> Result<()> {
        let oversized_file = non_zero(
            BLOB_MAX_FILE_SIZE
                .get()
                .checked_add(1)
                .ok_or(Error::TooLarge)?,
        )?;

        assert!(matches!(
            upload_parameters(oversized_file, None, None),
            Err(Error::TooLarge)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_progress_stream_reset() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "avml-blob-upload-{}-{}.bin",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let expected = b"seekable stream content";
        tokio::fs::write(&path, expected).await?;

        let result = async {
            let file = File::open(&path).await?;
            let file_size = file.metadata().await?.len();
            let mut stream = ProgressStream::new(file, file_size).await?;

            assert_eq!(stream.len(), Some(u64::try_from(expected.len())?));

            let mut prefix = [0_u8; 8];
            stream.read_exact(&mut prefix).await?;
            assert_eq!(&prefix, b"seekable");

            stream.reset().await?;

            let mut reread = Vec::new();
            stream.read_to_end(&mut reread).await?;
            assert_eq!(reread, expected);

            Result::<()>::Ok(())
        }
        .await;

        let _ = tokio::fs::remove_file(&path).await;
        result
    }
}
