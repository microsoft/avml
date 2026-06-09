// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Stream a memory snapshot directly to an Azure Block Blob.
//!
//! Bytes are buffered into fixed-size blocks. Each full block is staged via
//! [`BlockBlobClient::stage_block`]. Concurrency across staged blocks is
//! bounded by a [`Semaphore`]. After the snapshot writer is finished, the
//! caller invokes [`BlockBlobStream::finalize`] which awaits any in-flight
//! stage operations and commits the block list. On failure, the caller
//! invokes [`BlockBlobStream::abort`], which awaits in-flight tasks but
//! does not commit; uncommitted blocks are discarded by Azure on its own
//! timeline.

use crate::upload::blobstore::Error;
use async_trait::async_trait;
use azure_core::{
    Bytes,
    http::{NoFormat, RequestContent, XmlFormat},
};
use azure_storage_blob::{
    BlockBlobClient,
    models::{
        BlockBlobClientCommitBlockListOptions, BlockBlobClientStageBlockOptions, BlockLookupList,
    },
};
use core::num::NonZeroUsize;
use core::{
    pin::Pin,
    task::{Context, Poll},
};
use std::{
    io::{Result as IoResult, Write},
    sync::{Arc, Mutex},
};
use tokio::{
    io::AsyncWrite,
    runtime::Handle,
    sync::{Semaphore, mpsc},
    task::JoinHandle,
};
use tokio_util::io::SyncIoBridge;

type Result<T> = core::result::Result<T, Error>;

/// Block IDs as a fixed 8-byte big-endian representation of a u64 counter.
/// Azure requires all block IDs within a single commit to have identical
/// byte length; using the raw `to_be_bytes()` representation guarantees
/// that and produces an ordering that matches the staging order when
/// compared lexicographically.
fn block_id(index: u64) -> Vec<u8> {
    index.to_be_bytes().to_vec()
}

/// Abstraction over the two `BlockBlobClient` methods this module uses,
/// so tests can substitute an in-memory fake without standing up Azure.
#[expect(
    clippy::redundant_pub_crate,
    reason = "appears in the signature of pub(crate) BlockBlobStream::with_stager"
)]
#[async_trait]
pub(crate) trait BlockStager: Send + Sync + 'static {
    async fn stage_block(&self, block_id: Vec<u8>, body: Bytes) -> Result<()>;
    async fn commit_block_list(&self, block_ids: Vec<Vec<u8>>) -> Result<()>;
}

/// Live `BlockStager` backed by `azure_storage_blob`.
struct SdkStager {
    client: Arc<BlockBlobClient>,
}

#[async_trait]
impl BlockStager for SdkStager {
    async fn stage_block(&self, block_id: Vec<u8>, body: Bytes) -> Result<()> {
        let len = u64::try_from(body.len())?;
        let content: RequestContent<Bytes, NoFormat> = body.into();
        self.client
            .stage_block(
                &block_id,
                len,
                content,
                Option::<BlockBlobClientStageBlockOptions<'_>>::None,
            )
            .await?;
        Ok(())
    }

    async fn commit_block_list(&self, block_ids: Vec<Vec<u8>>) -> Result<()> {
        let list = BlockLookupList {
            latest: Some(block_ids),
            ..Default::default()
        };
        let content: RequestContent<BlockLookupList, XmlFormat> = list.try_into()?;
        self.client
            .commit_block_list(
                content,
                Option::<BlockBlobClientCommitBlockListOptions<'_>>::None,
            )
            .await?;
        Ok(())
    }
}

/// Messages from the writer to the uploader task.
enum UploaderMsg {
    Stage { index: u64, data: Bytes },
}

/// Final result returned by the uploader task once the writer side closes
/// the channel.
struct UploaderResult {
    /// Successfully staged block indices, in arbitrary order.
    completed: Vec<u64>,
    /// First error observed across all `stage_block` calls.
    first_error: Option<Error>,
}

type ReservationFuture = Pin<
    Box<
        dyn Future<
                Output = core::result::Result<
                    mpsc::OwnedPermit<UploaderMsg>,
                    mpsc::error::SendError<()>,
                >,
            > + Send,
    >,
>;

/// Sync writer side: implements [`AsyncWrite`] by buffering up to
/// `block_size` bytes, then handing the buffer off to the uploader task
/// via a bounded mpsc.
///
/// `poll_write` returns `Pending` when the channel is full, which gives
/// the producer real backpressure: the bound is `concurrency`, matching
/// the semaphore inside the uploader.
struct BlockBlobAsyncWriter {
    sender: Option<mpsc::Sender<UploaderMsg>>,
    buf: Vec<u8>,
    block_size: usize,
    max_blocks: u64,
    next_index: u64,
    /// `Some` once the uploader has observed (or the writer has observed
    /// via a closed channel) that the receiver is gone. After that point
    /// `poll_write` returns the captured error.
    error_slot: Arc<Mutex<Option<Error>>>,
    /// Pending reservation across `poll_write` invocations.
    pending_reservation: Option<ReservationFuture>,
}

impl BlockBlobAsyncWriter {
    fn new(
        sender: mpsc::Sender<UploaderMsg>,
        block_size: NonZeroUsize,
        max_blocks: u64,
        error_slot: Arc<Mutex<Option<Error>>>,
    ) -> Self {
        Self {
            sender: Some(sender),
            buf: Vec::with_capacity(block_size.get()),
            block_size: block_size.get(),
            max_blocks,
            next_index: 0,
            error_slot,
            pending_reservation: None,
        }
    }

    fn first_error_io(&self) -> Option<std::io::Error> {
        if let Ok(slot) = self.error_slot.lock() {
            slot.as_ref()
                .map(ToString::to_string)
                .map(std::io::Error::other)
        } else {
            None
        }
    }

    fn record_error_and_close(&mut self, err: Error) -> std::io::Error {
        let message = err.to_string();
        if let Ok(mut slot) = self.error_slot.lock()
            && slot.is_none()
        {
            *slot = Some(err);
        }
        self.sender = None;
        std::io::Error::other(message)
    }

    fn allocate_index(&mut self) -> Result<u64> {
        if self.next_index >= self.max_blocks {
            return Err(Error::TooLarge);
        }

        let index = self.next_index;
        // `max_blocks` enforces the practical cap; `checked_add` covers the
        // theoretical u64 counter overflow without reusing a block ID.
        self.next_index = self.next_index.checked_add(1).ok_or(Error::TooLarge)?;
        Ok(index)
    }

    /// Try to dispatch the current buffer if it is full. Returns
    /// `Poll::Pending` if a permit can't be acquired without waiting,
    /// or `Poll::Ready(Ok(()))` if the buffer was either not full or
    /// successfully dispatched.
    fn try_dispatch(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        if self.buf.len() < self.block_size {
            return Poll::Ready(Ok(()));
        }

        if self.pending_reservation.is_none() {
            let Some(sender) = self.sender.as_ref() else {
                return Poll::Ready(Err(std::io::Error::other(
                    "blob writer was already shut down",
                )));
            };
            let sender = sender.clone();
            self.pending_reservation = Some(Box::pin(sender.reserve_owned()));
        }

        let mut reservation = self
            .pending_reservation
            .take()
            .ok_or_else(|| std::io::Error::other("missing reservation slot"))?;
        match reservation.as_mut().poll(cx) {
            Poll::Pending => {
                self.pending_reservation = Some(reservation);
                Poll::Pending
            }
            Poll::Ready(Err(_send)) => {
                // Receiver dropped -> uploader exited (probably due to error).
                self.sender = None;
                let err = self
                    .first_error_io()
                    .unwrap_or_else(|| std::io::Error::other("uploader exited early"));
                Poll::Ready(Err(err))
            }
            Poll::Ready(Ok(permit)) => {
                let index = match self.allocate_index() {
                    Ok(index) => index,
                    Err(err) => return Poll::Ready(Err(self.record_error_and_close(err))),
                };
                let block_size = self.block_size;
                let data = core::mem::replace(&mut self.buf, Vec::with_capacity(block_size));
                permit.send(UploaderMsg::Stage {
                    index,
                    data: Bytes::from(data),
                });
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl AsyncWrite for BlockBlobAsyncWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        if let Some(err) = self.first_error_io() {
            return Poll::Ready(Err(err));
        }

        if self.buf.len() >= self.block_size {
            match self.try_dispatch(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {}
            }
        }

        let take = self
            .block_size
            .saturating_sub(self.buf.len())
            .min(buf.len());
        self.buf.extend_from_slice(buf.get(..take).unwrap_or(&[]));
        Poll::Ready(Ok(take))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        // Partial flush would emit a short block in the middle of the
        // blob, which Azure forbids. Flush is a no-op; the trailing
        // partial buffer is staged only by poll_shutdown.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        if let Some(err) = self.first_error_io() {
            return Poll::Ready(Err(err));
        }

        // Stage the trailing partial buffer, if any, as the final block.
        if !self.buf.is_empty() {
            if self.pending_reservation.is_none() {
                let Some(sender) = self.sender.as_ref() else {
                    return Poll::Ready(Ok(()));
                };
                let sender = sender.clone();
                self.pending_reservation = Some(Box::pin(sender.reserve_owned()));
            }
            let mut reservation = self
                .pending_reservation
                .take()
                .ok_or_else(|| std::io::Error::other("missing reservation slot"))?;
            match reservation.as_mut().poll(cx) {
                Poll::Pending => {
                    self.pending_reservation = Some(reservation);
                    return Poll::Pending;
                }
                Poll::Ready(Err(_)) => {
                    self.sender = None;
                    let err = self
                        .first_error_io()
                        .unwrap_or_else(|| std::io::Error::other("uploader exited early"));
                    return Poll::Ready(Err(err));
                }
                Poll::Ready(Ok(permit)) => {
                    let index = match self.allocate_index() {
                        Ok(index) => index,
                        Err(err) => return Poll::Ready(Err(self.record_error_and_close(err))),
                    };
                    let block_size = self.block_size;
                    let data = core::mem::replace(&mut self.buf, Vec::with_capacity(block_size));
                    permit.send(UploaderMsg::Stage {
                        index,
                        data: Bytes::from(data),
                    });
                }
            }
        }

        // Drop the sender so the uploader task can finish.
        self.sender = None;
        Poll::Ready(Ok(()))
    }
}

/// Public handle for streaming a memory snapshot into a Block Blob.
///
/// Construct with [`BlockBlobStream::new`]. Drive the sync writer
/// (returned by [`Self::writer`]) from a blocking context — typically
/// inside [`tokio::task::spawn_blocking`]. After writing finishes,
/// call [`Self::finish_writes`] from the same blocking context to flush
/// the trailing partial block; then call [`Self::finalize`] (or
/// [`Self::abort`]) from async context to await uploads and commit (or
/// discard) the block list.
///
/// # Runtime requirements
///
/// `new` and `finalize`/`abort` must be invoked from inside a tokio
/// runtime. The sync writer must be invoked from a thread that is *not*
/// a runtime worker (i.e., from `spawn_blocking`), otherwise the
/// internal `block_on` deadlocks the current-thread runtime that the
/// avml binary uses.
pub struct BlockBlobStream {
    bridge: SyncIoBridge<BlockBlobAsyncWriter>,
    uploader: Option<JoinHandle<UploaderResult>>,
    stager: Arc<dyn BlockStager>,
}

/// Azure's per-blob block count limit. Public for callers (e.g. the
/// binary) that want to derive a safe block size up front.
pub const BLOB_MAX_BLOCKS: u64 = 50_000;

impl BlockBlobStream {
    /// Construct a streaming uploader against a live block blob.
    #[must_use]
    pub fn new(
        client: BlockBlobClient,
        block_size: NonZeroUsize,
        concurrency: NonZeroUsize,
    ) -> Self {
        Self::with_stager(
            Arc::new(SdkStager {
                client: Arc::new(client),
            }),
            block_size,
            concurrency,
        )
    }

    pub(crate) fn with_stager(
        stager: Arc<dyn BlockStager>,
        block_size: NonZeroUsize,
        concurrency: NonZeroUsize,
    ) -> Self {
        Self::with_stager_and_max_blocks(stager, block_size, concurrency, BLOB_MAX_BLOCKS)
    }

    pub(crate) fn with_stager_and_max_blocks(
        stager: Arc<dyn BlockStager>,
        block_size: NonZeroUsize,
        concurrency: NonZeroUsize,
        max_blocks: u64,
    ) -> Self {
        let handle = Handle::current();
        let error_slot = Arc::new(Mutex::new(None));
        let (tx, rx) = mpsc::channel::<UploaderMsg>(concurrency.get());

        let uploader = handle.spawn(run_uploader(
            stager.clone(),
            rx,
            Arc::new(Semaphore::new(concurrency.get())),
            error_slot.clone(),
        ));

        let writer = BlockBlobAsyncWriter::new(tx, block_size, max_blocks, error_slot);
        let bridge = SyncIoBridge::new_with_handle(writer, handle);

        Self {
            bridge,
            uploader: Some(uploader),
            stager,
        }
    }

    /// Returns the sync writer to feed into the snapshot pipeline.
    /// Must be driven from a blocking thread.
    pub fn writer(&mut self) -> &mut dyn Write {
        &mut self.bridge
    }

    /// Flush any partial trailing block. Must be called from the same
    /// blocking thread that drove the writer.
    ///
    /// # Errors
    /// Returns an error if the underlying [`AsyncWrite::poll_shutdown`]
    /// returns one (e.g., the uploader task already exited due to an
    /// upload failure).
    pub fn finish_writes(&mut self) -> IoResult<()> {
        self.bridge.shutdown()
    }

    /// Await all in-flight `stage_block` calls and commit the block list.
    /// Consumes `self` because no further writes are valid after commit.
    ///
    /// # Errors
    /// Returns any captured `stage_block` error or any error from
    /// `commit_block_list`.
    pub async fn finalize(mut self) -> Result<()> {
        let result = self.await_uploader().await?;
        if let Some(err) = result.first_error {
            return Err(err);
        }
        let mut indices = result.completed;
        indices.sort_unstable();
        let block_ids: Vec<Vec<u8>> = indices.into_iter().map(block_id).collect();
        self.stager.commit_block_list(block_ids).await
    }

    /// Close the writer side and await all in-flight `stage_block` calls
    /// without committing. Staged but uncommitted blocks are discarded by
    /// Azure on its own timeline.
    ///
    /// # Errors
    /// Best-effort; returns the first error seen, but does not call
    /// `commit_block_list`.
    pub async fn abort(self) -> Result<()> {
        let result = Self::await_uploader_handle(self.close_for_abort()).await?;
        if let Some(err) = result.first_error {
            return Err(err);
        }
        Ok(())
    }

    async fn await_uploader(&mut self) -> Result<UploaderResult> {
        Self::await_uploader_handle(self.uploader.take()).await
    }

    fn close_for_abort(self) -> Option<JoinHandle<UploaderResult>> {
        let Self {
            bridge: _closed_bridge,
            uploader,
            stager: _,
        } = self;
        uploader
    }

    async fn await_uploader_handle(
        uploader: Option<JoinHandle<UploaderResult>>,
    ) -> Result<UploaderResult> {
        let Some(uploader) = uploader else {
            return Ok(UploaderResult {
                completed: Vec::new(),
                first_error: None,
            });
        };
        uploader
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))
    }
}

async fn run_uploader(
    stager: Arc<dyn BlockStager>,
    mut rx: mpsc::Receiver<UploaderMsg>,
    semaphore: Arc<Semaphore>,
    error_slot: Arc<Mutex<Option<Error>>>,
) -> UploaderResult {
    let mut in_flight: Vec<JoinHandle<core::result::Result<u64, (u64, Error)>>> = Vec::new();

    while let Some(msg) = rx.recv().await {
        match msg {
            UploaderMsg::Stage { index, data } => {
                // Acquire a permit (bounded in-flight). Held by the worker
                // task until stage_block completes.
                let Ok(permit) = semaphore.clone().acquire_owned().await else {
                    break;
                };
                let stager = stager.clone();
                let id = block_id(index);
                let worker = tokio::spawn(async move {
                    let _permit = permit;
                    stager
                        .stage_block(id, data)
                        .await
                        .map(|()| index)
                        .map_err(|e| (index, e))
                });
                in_flight.push(worker);
            }
        }
    }

    // Channel closed; await all in-flight stages.
    let mut completed = Vec::with_capacity(in_flight.len());
    for handle in in_flight {
        match handle.await {
            Ok(Ok(index)) => completed.push(index),
            Ok(Err((_index, err))) => {
                if let Ok(mut slot) = error_slot.lock()
                    && slot.is_none()
                {
                    *slot = Some(err);
                }
            }
            Err(join_err) => {
                if let Ok(mut slot) = error_slot.lock()
                    && slot.is_none()
                {
                    *slot = Some(Error::Io(std::io::Error::other(join_err.to_string())));
                }
            }
        }
    }

    let first_error = error_slot.lock().ok().and_then(|mut s| s.take());

    UploaderResult {
        completed,
        first_error,
    }
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::similar_names,
        reason = "tests assert on pre-known shapes and value counts"
    )]

    use super::*;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    /// In-memory `BlockStager` used by tests. Records every staged block
    /// and every commit call. Optionally fails a specific block index.
    struct FakeStager {
        staged: StdMutex<Vec<(Vec<u8>, Bytes)>>,
        commits: StdMutex<Vec<Vec<Vec<u8>>>>,
        fail_index: Option<u64>,
        stage_call_count: AtomicUsize,
        max_concurrent_stages: AtomicUsize,
        current_stages: AtomicUsize,
    }

    impl FakeStager {
        fn new() -> Self {
            Self {
                staged: StdMutex::new(Vec::new()),
                commits: StdMutex::new(Vec::new()),
                fail_index: None,
                stage_call_count: AtomicUsize::new(0),
                max_concurrent_stages: AtomicUsize::new(0),
                current_stages: AtomicUsize::new(0),
            }
        }

        fn failing(index: u64) -> Self {
            Self {
                fail_index: Some(index),
                ..Self::new()
            }
        }

        fn locked_staged(&self) -> Vec<(Vec<u8>, Bytes)> {
            self.staged
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        fn locked_commits(&self) -> Vec<Vec<Vec<u8>>> {
            self.commits
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    #[async_trait]
    impl BlockStager for FakeStager {
        async fn stage_block(&self, block_id: Vec<u8>, body: Bytes) -> Result<()> {
            self.stage_call_count.fetch_add(1, Ordering::SeqCst);
            let now = self
                .current_stages
                .fetch_add(1, Ordering::SeqCst)
                .saturating_add(1);
            self.max_concurrent_stages.fetch_max(now, Ordering::SeqCst);

            // Give other in-flight tasks a chance to overlap so the
            // concurrency test can observe parallelism (or its absence).
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;

            let should_fail = self.fail_index.is_some_and(|target| {
                if block_id.len() == 8 {
                    let mut buf = [0_u8; 8];
                    buf.copy_from_slice(&block_id);
                    u64::from_be_bytes(buf) == target
                } else {
                    false
                }
            });

            self.current_stages.fetch_sub(1, Ordering::SeqCst);
            if should_fail {
                return Err(Error::Io(std::io::Error::other("simulated failure")));
            }

            self.staged
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((block_id, body));
            Ok(())
        }

        async fn commit_block_list(&self, block_ids: Vec<Vec<u8>>) -> Result<()> {
            self.commits
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(block_ids);
            Ok(())
        }
    }

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("test constant non-zero")
    }

    fn build_stream(
        stager: Arc<FakeStager>,
        block_size: usize,
        concurrency: usize,
    ) -> BlockBlobStream {
        BlockBlobStream::with_stager(stager, nz(block_size), nz(concurrency))
    }

    fn build_stream_with_max_blocks(
        stager: Arc<FakeStager>,
        block_size: usize,
        concurrency: usize,
        max_blocks: u64,
    ) -> BlockBlobStream {
        BlockBlobStream::with_stager_and_max_blocks(
            stager,
            nz(block_size),
            nz(concurrency),
            max_blocks,
        )
    }

    async fn run_write<F>(stream: BlockBlobStream, write: F) -> (BlockBlobStream, IoResult<()>)
    where
        F: FnOnce(&mut dyn Write) -> IoResult<()> + Send + 'static,
    {
        // SyncIoBridge requires us to be off the runtime thread; spawn_blocking
        // models the real usage from the binary.
        tokio::task::spawn_blocking(move || {
            let mut stream = stream;
            let result = write(stream.writer());
            let shutdown = stream.finish_writes();
            let combined = result.and(shutdown);
            (stream, combined)
        })
        .await
        .expect("spawn_blocking join")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn finalize_with_zero_writes_commits_empty_block_list() {
        let stager = Arc::new(FakeStager::new());
        let stream = build_stream(stager.clone(), 8, 2);

        let (stream, result) = run_write(stream, |_w| Ok(())).await;
        result.expect("writer shutdown succeeds");
        stream.finalize().await.expect("finalize succeeds");

        let commits = stager.locked_commits();
        assert_eq!(commits.len(), 1, "exactly one commit");
        assert!(
            commits[0].is_empty(),
            "empty snapshot commits empty block list"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rotation_at_exact_block_size_emits_uniform_ids() {
        let stager = Arc::new(FakeStager::new());
        let stream = build_stream(stager.clone(), 4, 3);

        let payload: Vec<u8> = (0..12).collect();
        let (stream, result) = run_write(stream, move |w| w.write_all(&payload)).await;
        result.expect("write + shutdown");
        stream.finalize().await.expect("finalize");

        let staged = stager.locked_staged();
        assert_eq!(staged.len(), 3, "three full blocks staged");
        for entry in &staged {
            assert_eq!(entry.0.len(), 8, "block ids uniform width");
        }

        let commits = stager.locked_commits();
        assert_eq!(commits.len(), 1);
        let ids = &commits[0];
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, &sorted, "committed ids are sorted ascending");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn trailing_partial_block_is_staged_on_shutdown() {
        let stager = Arc::new(FakeStager::new());
        let stream = build_stream(stager.clone(), 4, 3);

        let payload: Vec<u8> = (0..6).collect(); // 4 + 2
        let (stream, result) = run_write(stream, move |w| w.write_all(&payload)).await;
        result.expect("write + shutdown");
        stream.finalize().await.expect("finalize");

        let mut staged = stager.locked_staged();
        // Stages may complete out of order with concurrency>1; sort by id.
        staged.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(staged.len(), 2);
        assert_eq!(staged[0].1.len(), 4);
        assert_eq!(staged[1].1.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn max_blocks_cap_is_enforced_before_commit() {
        let stager = Arc::new(FakeStager::new());
        let stream = build_stream_with_max_blocks(stager.clone(), 1, 2, 3);

        let payload: Vec<u8> = (0..4).collect();
        let (stream, result) = run_write(stream, move |w| w.write_all(&payload)).await;
        let write_err = result.expect_err("four single-byte blocks exceed max_blocks=3");
        assert_eq!(write_err.to_string(), Error::TooLarge.to_string());

        let err = stream
            .finalize()
            .await
            .expect_err("finalize reports the assignment-time cap failure");
        assert!(matches!(err, Error::TooLarge), "got: {err:?}");
        assert!(
            stager.locked_commits().is_empty(),
            "cap failure must not commit a partial block list"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exactly_max_blocks_full_blocks_succeeds() {
        let stager = Arc::new(FakeStager::new());
        let stream = build_stream_with_max_blocks(stager.clone(), 2, 2, 3);

        let payload: Vec<u8> = (0..6).collect();
        let (stream, result) = run_write(stream, move |w| w.write_all(&payload)).await;
        result.expect("exactly three two-byte blocks are within the cap");
        stream
            .finalize()
            .await
            .expect("finalize commits capped write");

        let commits = stager.locked_commits();
        assert_eq!(commits.len(), 1);
        assert_eq!(
            commits[0].len(),
            3,
            "exactly max_blocks block IDs are committed"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn byte_past_max_blocks_fails_on_trailing_partial() {
        let stager = Arc::new(FakeStager::new());
        let stream = build_stream_with_max_blocks(stager.clone(), 2, 2, 3);

        let payload: Vec<u8> = (0..7).collect();
        let (stream, result) = run_write(stream, move |w| w.write_all(&payload)).await;
        let write_err = result.expect_err("seventh byte requires a fourth block during shutdown");
        assert_eq!(write_err.to_string(), Error::TooLarge.to_string());

        let err = stream
            .finalize()
            .await
            .expect_err("finalize preserves the shutdown cap failure");
        assert!(matches!(err, Error::TooLarge), "got: {err:?}");
        assert!(
            stager.locked_commits().is_empty(),
            "no commit after the boundary failure"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stage_failure_surfaces_and_skips_commit() {
        let stager = Arc::new(FakeStager::failing(1));
        let stream = build_stream(stager.clone(), 4, 2);

        let payload: Vec<u8> = (0..12).collect();
        let (stream, _result) = run_write(stream, move |w| {
            // The write may or may not succeed depending on timing;
            // either way the error surfaces in finalize.
            drop(w.write_all(&payload));
            Ok(())
        })
        .await;
        let err = stream
            .finalize()
            .await
            .expect_err("finalize must report stage failure");
        assert!(matches!(err, Error::Io(_)), "got: {err:?}");

        let commits = stager.locked_commits();
        assert!(commits.is_empty(), "no commit after stage failure");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abort_does_not_commit() {
        let stager = Arc::new(FakeStager::new());
        let stream = build_stream(stager.clone(), 4, 2);

        let payload: Vec<u8> = (0..8).collect();
        let (stream, result) = run_write(stream, move |w| w.write_all(&payload)).await;
        result.expect("write + shutdown");
        stream.abort().await.expect("abort succeeds");

        let commits = stager.locked_commits();
        assert!(commits.is_empty(), "abort skips commit");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrency_bound_is_respected() {
        let stager = Arc::new(FakeStager::new());
        let stream = build_stream(stager.clone(), 1, 1);

        // 6 single-byte blocks; with concurrency=1, max in-flight must be 1.
        let payload: Vec<u8> = (0..6).collect();
        let (stream, result) = run_write(stream, move |w| w.write_all(&payload)).await;
        result.expect("write + shutdown");
        stream.finalize().await.expect("finalize");

        let observed = stager.max_concurrent_stages.load(Ordering::SeqCst);
        assert_eq!(observed, 1, "concurrency=1 caps in-flight at 1");
        assert_eq!(stager.stage_call_count.load(Ordering::SeqCst), 6);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn higher_concurrency_allows_overlap() {
        let stager = Arc::new(FakeStager::new());
        let stream = build_stream(stager.clone(), 1, 4);

        // 8 single-byte blocks with concurrency=4: max in-flight may be up to 4.
        let payload: Vec<u8> = (0..8).collect();
        let (stream, result) = run_write(stream, move |w| w.write_all(&payload)).await;
        result.expect("write + shutdown");
        stream.finalize().await.expect("finalize");

        let observed = stager.max_concurrent_stages.load(Ordering::SeqCst);
        assert!(observed >= 2, "expected some overlap, got {observed}");
        assert!(observed <= 4, "must not exceed configured concurrency");
    }

    #[test]
    fn block_id_round_trip() {
        for i in [0_u64, 1, 100, u64::MAX] {
            let id = block_id(i);
            assert_eq!(id.len(), 8);
            let mut buf = [0_u8; 8];
            buf.copy_from_slice(&id);
            assert_eq!(u64::from_be_bytes(buf), i);
        }
    }
}
