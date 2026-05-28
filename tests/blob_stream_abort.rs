// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#![cfg(feature = "stream")]

use avml::BlockBlobStream;
use azure_storage_blob::BlobClient;
use core::{error::Error, num::NonZeroUsize, time::Duration};
use std::{io::Result as IoResult, time::Instant};
use tokio::task::JoinHandle;

async fn run_failed_write_without_shutdown(
    stream: BlockBlobStream,
) -> Result<(BlockBlobStream, IoResult<()>), Box<dyn Error>> {
    Ok(tokio::task::spawn_blocking(move || {
        let mut stream = stream;
        let result = stream
            .writer()
            .write_all(&[1, 2, 3])
            .and_then(|()| Err(std::io::Error::other("simulated snapshot failure")));
        (stream, result)
    })
    .await?)
}

async fn wait_until_finished<T>(handle: &JoinHandle<T>) -> bool {
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(100))
        .unwrap_or_else(Instant::now);
    while Instant::now() < deadline {
        if handle.is_finished() {
            return true;
        }
        tokio::task::yield_now().await;
    }
    handle.is_finished()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_after_snapshot_error_without_shutdown_completes() -> Result<(), Box<dyn Error>> {
    let url = "http://127.0.0.1/devstoreaccount1/container/blob?sig=fake".parse()?;
    let client = BlobClient::new(url, None, None)?.block_blob_client();
    let block_size = NonZeroUsize::new(4).ok_or("block size must be nonzero")?;
    let concurrency = NonZeroUsize::new(2).ok_or("concurrency must be nonzero")?;
    let stream = BlockBlobStream::new(client, block_size, concurrency);

    let (stream, result) = run_failed_write_without_shutdown(stream).await?;
    assert!(result.is_err(), "test must simulate a snapshot error");

    let abort_task = tokio::spawn(stream.abort());
    let finished = wait_until_finished(&abort_task).await;
    if finished {
        abort_task.await??;
    } else {
        abort_task.abort();
        drop(abort_task.await);
    }

    assert!(finished, "abort should not wait forever for the uploader");
    Ok(())
}
