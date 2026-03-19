// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::ONE_MB;
use crate::upload::status::Status;
use azure_core::{
    Bytes,
    error::Error as AzureError,
    http::{Body, NoFormat, RequestContent},
    stream::{DEFAULT_BUFFER_SIZE, SeekableStream},
};
use azure_storage_blob::{BlobClient, models::BlobClientUploadOptions};
use core::{
    cmp,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};
use std::{io::SeekFrom, path::Path, sync::Arc};
use tokio::{
    fs::File,
    io::{AsyncSeekExt as _, ReadBuf},
    sync::{Mutex, OwnedMutexGuard},
};
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
const ONE_MB_NZ: NonZeroUsize = NonZeroUsize::new(ONE_MB).expect("ONE_MB must be non-zero");

/// Maximum number of blocks
///
/// <https://docs.microsoft.com/en-us/azure/storage/blobs/scalability-targets#scale-targets-for-blob-storage>
#[allow(clippy::expect_used)]
const BLOB_MAX_BLOCKS: NonZeroUsize =
    NonZeroUsize::new(50_000).expect("blob max blocks must be non-zero");

/// Maximum size of any single block
///
/// <https://docs.microsoft.com/en-us/azure/storage/blobs/scalability-targets#scale-targets-for-blob-storage>
#[allow(clippy::expect_used)]
const BLOB_MAX_BLOCK_SIZE: NonZeroUsize = ONE_MB_NZ.saturating_mul(
    NonZeroUsize::new(4000).expect("blob max block size multiplier must be non-zero"),
);

/// Maximum total size of a file
///
/// <https://docs.microsoft.com/en-us/azure/storage/blobs/scalability-targets#scale-targets-for-blob-storage>
#[allow(clippy::expect_used)]
const BLOB_MAX_FILE_SIZE: NonZeroUsize = BLOB_MAX_BLOCKS.saturating_mul(BLOB_MAX_BLOCK_SIZE);

/// Minimum block size, which is required to trigger the "high-throughput block
/// blobs" feature on all storage accounts
///
/// <https://azure.microsoft.com/en-us/blog/high-throughput-with-azure-blob-storage/>
#[allow(clippy::expect_used)]
const BLOB_MIN_BLOCK_SIZE: NonZeroUsize = ONE_MB_NZ
    .saturating_mul(NonZeroUsize::new(5).expect("blob min block size multiplier must be non-zero"));

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
const MEMORY_THRESHOLD: NonZeroUsize = ONE_MB_NZ
    .saturating_mul(NonZeroUsize::new(500).expect("memory threshold multiplier must be non-zero"));

fn calc_block_size(
    file_size: NonZeroUsize,
    block_size: Option<NonZeroUsize>,
) -> Result<NonZeroUsize> {
    let block_size = match block_size {
        Some(block_size) => block_size,
        None => NonZeroUsize::new(file_size.get().div_ceil(BLOB_MAX_BLOCKS.get()))
            .ok_or(Error::TooLarge)?,
    };

    Ok(cmp::min(
        cmp::max(block_size, BLOB_MIN_BLOCK_SIZE),
        BLOB_MAX_BLOCK_SIZE,
    ))
}

fn calc_concurrency(
    file_size: NonZeroUsize,
    block_size: Option<NonZeroUsize>,
    upload_concurrency: Option<NonZeroUsize>,
) -> Result<(NonZeroUsize, NonZeroUsize)> {
    if file_size > BLOB_MAX_FILE_SIZE {
        return Err(Error::TooLarge);
    }
    let block_size = calc_block_size(file_size, block_size)?;

    let memory_limited_concurrency = match NonZeroUsize::new(
        MEMORY_THRESHOLD
            .get()
            .checked_div(block_size.get())
            .unwrap_or(0),
    ) {
        Some(concurrency) => concurrency,
        None => NonZeroUsize::MIN,
    };
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
    file_size: NonZeroUsize,
    block_size: Option<NonZeroUsize>,
    upload_concurrency: Option<NonZeroUsize>,
) -> Result<(NonZeroUsize, NonZeroUsize)> {
    let block_size = block_size.map(|x| x.saturating_mul(ONE_MB_NZ));
    calc_concurrency(file_size, block_size, upload_concurrency)
}

/// A seekable file-backed stream used for blob uploads.
///
/// `FileStream` is `Clone` because the Azure core body/retry machinery may need
/// to duplicate the stream. All clones share the same underlying file handle
/// and read cursor state via `Arc<Mutex<...>>`.
///
/// Invariants:
/// - At most one clone is actively reading from the stream at any time.
/// - All reads go through the shared `read_state` so that the current cursor
///   position is coordinated between clones.
#[derive(Clone)]
struct FileStream {
    handle: Arc<Mutex<File>>,
    stream_size: u64,
    buffer_size: usize,
    read_state: Arc<Mutex<ReadState>>,
    status: Status,
}

impl core::fmt::Debug for FileStream {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FileStream")
            .field("stream_size", &self.stream_size)
            .field("buffer_size", &self.buffer_size)
            .finish_non_exhaustive()
    }
}

impl FileStream {
    async fn new(handle: File, buffer_size: usize) -> Result<Self> {
        let stream_size = handle.metadata().await?.len();
        let handle = Arc::new(Mutex::new(handle));

        Ok(Self {
            handle,
            stream_size,
            buffer_size,
            read_state: Arc::new(Mutex::new(ReadState::default())),
            status: Status::new(Some(stream_size)),
        })
    }
}

type FileLockFuture = Pin<Box<dyn Future<Output = OwnedMutexGuard<File>> + Send>>;

#[derive(Default)]
enum ReadState {
    #[default]
    Idle,
    Locking(FileLockFuture),
    Locked(OwnedMutexGuard<File>),
}

#[async_trait::async_trait]
impl SeekableStream for FileStream {
    async fn reset(&mut self) -> azure_core::Result<()> {
        *self.read_state.lock().await = ReadState::Idle;
        let mut handle = self.handle.clone().lock_owned().await;
        handle.seek(SeekFrom::Start(0)).await?;
        self.status.reset();
        Ok(())
    }

    fn len(&self) -> usize {
        self.stream_size.try_into().unwrap_or(usize::MAX)
    }

    fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}

impl futures::io::AsyncRead for FileStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slice: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let Ok(mut state) = this.read_state.try_lock() else {
            // Another task is currently holding `read_state`; yield and try again later
            cx.waker().wake_by_ref();
            return Poll::Pending;
        };

        loop {
            match *state {
                ReadState::Idle => {
                    *state = ReadState::Locking(Box::pin(this.handle.clone().lock_owned()));
                }
                ReadState::Locking(ref mut lock_future) => {
                    match Future::poll(Pin::as_mut(lock_future), cx) {
                        Poll::Ready(guard) => *state = ReadState::Locked(guard),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                ReadState::Locked(ref mut guard) => {
                    let mut read_buf = ReadBuf::new(slice);

                    return match tokio::io::AsyncRead::poll_read(
                        Pin::new(&mut **guard),
                        cx,
                        &mut read_buf,
                    ) {
                        Poll::Ready(Ok(())) => {
                            let len = read_buf.filled().len();
                            this.status.inc(len);
                            Poll::Ready(Ok(len))
                        }
                        Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                        Poll::Pending => Poll::Pending,
                    };
                }
            }
        }
    }
}

/// Upload a file to Azure Blob Store using a SAS URL.
///
/// ```rust,no_run
/// use avml::BlobUploader;
/// # use avml::Result;
/// # use std::{num::NonZeroUsize, path::Path};
/// # use url::Url;
/// # async fn upload() -> Result<()> {
/// let sas_url = Url::parse("https://contoso.com/container_name/blob_name?sas_token_here=1")
///     .expect("url parsing failed");
/// let path = Path::new("/tmp/image.lime");
/// let uploader = BlobUploader::new(&sas_url)?
///     .block_size(NonZeroUsize::new(100))
///     .concurrency(NonZeroUsize::new(5));
/// uploader.upload_file(&path).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct BlobUploader {
    client: Arc<BlobClient>,
    block_size: Option<NonZeroUsize>,
    concurrency: Option<NonZeroUsize>,
}

impl BlobUploader {
    /// Create a new ``BlobUploader`` from a SAS URL.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The URL cannot be parsed as a valid Azure SAS URL
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
    pub fn block_size(self, block_size: Option<NonZeroUsize>) -> Self {
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
    /// # Errors
    /// Returns an error if:
    /// - The file cannot be opened or read
    /// - The file size cannot be converted to a usize
    /// - The file is too large for Azure Blob Storage
    /// - There is a failure during the upload process
    pub async fn upload_file(self, filename: &Path) -> Result<()> {
        let file = File::open(filename).await?;
        let file_size = file.metadata().await?.len().try_into()?;
        let Some(file_size) = NonZeroUsize::new(file_size) else {
            return Ok(());
        };
        let (block_size, uploaders_count) =
            upload_parameters(file_size, self.block_size, self.concurrency)?;

        let stream = FileStream::new(file, DEFAULT_BUFFER_SIZE).await?;
        let stream: Box<dyn SeekableStream> = Box::new(stream);
        let content: RequestContent<Bytes, NoFormat> = Body::from(stream).into();

        let options = BlobClientUploadOptions {
            parallel: Some(uploaders_count),
            partition_size: Some(block_size),
            ..Default::default()
        };

        self.client.upload(content, Some(options)).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::AsyncReadExt as _;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn non_zero(value: usize) -> Result<NonZeroUsize> {
        NonZeroUsize::new(value).ok_or(Error::TooLarge)
    }

    fn bytes_from_mib(mebibytes: usize) -> Result<NonZeroUsize> {
        non_zero(mebibytes.checked_mul(ONE_MB).ok_or(Error::TooLarge)?)
    }

    fn bytes_from_gib(gibibytes: usize) -> Result<NonZeroUsize> {
        bytes_from_mib(gibibytes.checked_mul(1024).ok_or(Error::TooLarge)?)
    }

    #[test]
    fn small_files_use_minimum_block_size_and_default_concurrency() -> Result<()> {
        let (block_size, concurrency) = upload_parameters(bytes_from_mib(400)?, None, None)?;

        assert_eq!(block_size, bytes_from_mib(5)?);
        assert_eq!(concurrency, DEFAULT_CONCURRENCY);
        Ok(())
    }

    #[test]
    fn user_block_size_is_clamped_to_minimum() -> Result<()> {
        let (block_size, concurrency) =
            upload_parameters(bytes_from_mib(300)?, Some(NonZeroUsize::MIN), None)?;

        assert_eq!(block_size, bytes_from_mib(5)?);
        assert_eq!(concurrency, DEFAULT_CONCURRENCY);
        Ok(())
    }

    #[test]
    fn requested_concurrency_caps_memory_limited_uploaders() -> Result<()> {
        let (block_size, concurrency) = upload_parameters(
            bytes_from_gib(30)?,
            Some(non_zero(100)?),
            Some(non_zero(3)?),
        )?;

        assert_eq!(block_size, bytes_from_mib(100)?);
        assert_eq!(concurrency, non_zero(3)?);
        Ok(())
    }

    #[test]
    fn auto_block_size_grows_when_minimum_would_exceed_max_blocks() -> Result<()> {
        let file_size = non_zero(
            bytes_from_mib(5)?
                .get()
                .checked_mul(50_000)
                .ok_or(Error::TooLarge)?
                .checked_add(1)
                .ok_or(Error::TooLarge)?,
        )?;
        let expected_block_size = non_zero(file_size.get().div_ceil(50_000))?;
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
            bytes_from_mib(50_000usize.checked_mul(4000).ok_or(Error::TooLarge)?)?
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
    async fn test_file_stream_reset() -> Result<()> {
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
            let mut stream = FileStream::new(file, 8).await?;

            assert_eq!(stream.len(), expected.len());

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

    #[tokio::test]
    async fn test_upload_file_empty_is_noop() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "avml-empty-blob-upload-{}-{}.bin",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        tokio::fs::write(&path, []).await?;

        let url = Url::parse("https://127.0.0.1:9/container/blob?sig=test")?;
        BlobUploader::new(&url)?.upload_file(&path).await?;
        let _ = tokio::fs::remove_file(&path).await;
        Ok(())
    }
}
