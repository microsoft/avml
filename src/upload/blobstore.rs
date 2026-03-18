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
}

type Result<T> = core::result::Result<T, Error>;

/// Maximum number of blocks
///
/// <https://docs.microsoft.com/en-us/azure/storage/blobs/scalability-targets#scale-targets-for-blob-storage>
const BLOB_MAX_BLOCKS: usize = 50_000;

/// Maximum size of any single block
///
/// <https://docs.microsoft.com/en-us/azure/storage/blobs/scalability-targets#scale-targets-for-blob-storage>
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
/// automatic retries in the case of TCP or HTTP errors, chunk sizes for huge
/// files are capped to 100MB each.
const REASONABLE_BLOCK_SIZE: usize = ONE_MB.saturating_mul(100);

/// Try to keep under 500MB in flight. If that's not possible due to block size,
/// concurrency will get disabled.
const MEMORY_THRESHOLD: usize = 500 * ONE_MB;

/// Heuristic "very large file" size used when the actual file size is not yet
/// known (for example, before probing a file or when size discovery fails).
/// This value feeds into block-size and concurrency calculations so that, in
/// the worst case, we behave as if we are uploading a large image while still
/// staying well below `BLOB_MAX_FILE_SIZE`.
///
/// 1 TB is chosen as a conservative upper-bound estimate:
/// - It is large enough to exercise the "large upload" code paths, ensuring
///   that concurrency and block sizing are not overly optimistic when size
///   information is missing.
/// - It is small enough compared to Azure's maximum blob size that the
///   resulting configuration will not violate service limits.
///
/// Once the real file size is known, it is validated against
/// `BLOB_MAX_FILE_SIZE` and used for the final concurrency/block-size
/// decisions; this constant only affects the initial tuning in the absence of
/// reliable size information.
const DEFAULT_FILE_SIZE: usize = 1024 * 1024 * 1024 * 1024;

fn calc_concurrency(
    file_size: usize,
    block_size: Option<usize>,
    upload_concurrency: usize,
) -> Result<(usize, usize)> {
    if file_size > BLOB_MAX_FILE_SIZE {
        return Err(Error::TooLarge);
    }

    let block_size = match block_size {
        Some(0) | None => match file_size {
            x if x < BLOB_MIN_BLOCK_SIZE * BLOB_MAX_BLOCKS => BLOB_MIN_BLOCK_SIZE,
            x if x < REASONABLE_BLOCK_SIZE * BLOB_MAX_BLOCKS => REASONABLE_BLOCK_SIZE,
            x => (x / BLOB_MAX_BLOCKS)
                .checked_add(1)
                .ok_or(Error::TooLarge)?,
        },
        Some(x) if x <= BLOB_MIN_BLOCK_SIZE => BLOB_MIN_BLOCK_SIZE,
        Some(x) => x,
    };

    let block_size = usize::min(block_size, BLOB_MAX_BLOCK_SIZE);

    let upload_concurrency = match upload_concurrency {
        0 | 1 => 1,
        _ => match MEMORY_THRESHOLD.checked_div(block_size) {
            None | Some(0) => 1,
            Some(x) => cmp::min(MAX_CONCURRENCY, x),
        },
    };

    Ok((block_size, upload_concurrency))
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
/// # use std::path::Path;
/// # use url::Url;
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
    client: Arc<BlobClient>,
    size: usize,
    block_size: Option<usize>,
    concurrency: usize,
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
            size: DEFAULT_FILE_SIZE,
            block_size: None,
            concurrency: DEFAULT_CONCURRENCY,
        }
    }

    /// Specify the size of the file to upload (in bytes)
    #[must_use]
    pub fn size(self, size: usize) -> Self {
        Self { size, ..self }
    }

    /// Specify the block size in multiples of 1MB
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

    /// Upload a file to Azure Blob Store using a fully qualified SAS token.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The file cannot be opened or read
    /// - The file size cannot be converted to a usize
    /// - The file is too large for Azure Blob Storage
    /// - There is a failure during the upload process
    pub async fn upload_file(mut self, filename: &Path) -> Result<()> {
        let file = File::open(filename).await?;
        let file_size = file.metadata().await?.len().try_into()?;
        self.size = file_size;

        let block_size = self.block_size.map(|x| x.saturating_mul(ONE_MB));
        let (block_size, uploaders_count) =
            calc_concurrency(self.size, block_size, self.concurrency)?;

        let stream = FileStream::new(file, DEFAULT_BUFFER_SIZE).await?;
        let stream: Box<dyn SeekableStream> = Box::new(stream);
        let content: RequestContent<Bytes, NoFormat> = Body::from(stream).into();

        let options = BlobClientUploadOptions {
            parallel: NonZeroUsize::new(uploaders_count),
            partition_size: NonZeroUsize::new(block_size),
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
}
