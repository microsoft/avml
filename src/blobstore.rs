// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::ONE_MB;
use anyhow::{anyhow, bail, Context, Result};
use async_channel::{bounded, Receiver, Sender};
use azure_core::new_http_client;
use azure_storage::core::prelude::*;
use azure_storage_blobs::prelude::*;
use backoff::{future::retry, ExponentialBackoff};
use bytes::{Bytes, BytesMut};
use futures::future::try_join_all;
use std::{cmp, convert::TryFrom, path::Path, sync::Arc};
use tokio::{fs::File, io::AsyncReadExt};
use url::Url;

// https://docs.microsoft.com/en-us/azure/storage/blobs/scalability-targets#scale-targets-for-blob-storage
const BLOB_MAX_BLOCKS: usize = 50_000;
const BLOB_MAX_BLOCK_SIZE: usize = ONE_MB * 4000;
const BLOB_MAX_FILE_SIZE: usize = BLOB_MAX_BLOCKS * BLOB_MAX_BLOCK_SIZE;

// trigger's the "high-throughput block blobs" on all storage accounts
// https://azure.microsoft.com/en-us/blog/high-throughput-with-azure-blob-storage/
const BLOB_MIN_BLOCK_SIZE: usize = ONE_MB * 5;

// Azure's default max request rate for a storage account is 20,000 per second.
// By keeping to 10 or fewer concurrent upload threads, AVML can be used to
// simultaneously upload images from 1000 different hosts concurrently (a full
// VM scaleset) to a single default storage account.
//
// https://docs.microsoft.com/en-us/azure/storage/common/scalability-targets-standard-account#scale-targets-for-standard-storage-accounts
const MAX_CONCURRENCY: usize = 10;

// if we're uploading *huge* files, use 100MB chunks
const REASONABLE_BLOCK_SIZE: usize = ONE_MB * 100;

struct UploadChunk {
    id: Bytes,
    data: Bytes,
}

struct SasToken {
    account: String,
    container: String,
    path: String,
    token: String,
}

impl TryFrom<&Url> for SasToken {
    type Error = anyhow::Error;

    fn try_from(url: &Url) -> Result<Self> {
        let account = if let Some(host) = url.host_str() {
            let v: Vec<&str> = host.split_terminator('.').collect();
            v[0].to_string()
        } else {
            bail!("invalid sas token (no account)");
        };

        let token = if let Some(token) = url.query() {
            token.to_string()
        } else {
            bail!("invalid SAS token");
        };

        let path = url.path();
        let mut v: Vec<&str> = path.split_terminator('/').collect();
        v.remove(0);
        let container = v.remove(0).to_string();
        let path = v.join("/");
        Ok(Self {
            account,
            container,
            path,
            token,
        })
    }
}

async fn upload_blocks(client: Arc<BlobClient>, r: Receiver<UploadChunk>) -> Result<()> {
    // the channel will respond with an Err to indicate the channel is closed
    while let Ok(upload_chunk) = r.recv().await {
        let hash = md5::compute(upload_chunk.data.clone()).into();
        retry(ExponentialBackoff::default(), || async {
            let data_for_req = upload_chunk.data.clone();
            let block_id_for_req = upload_chunk.id.clone();

            let result = client
                .put_block(block_id_for_req, data_for_req)
                .hash(&hash)
                .execute()
                .await;
            match result {
                Ok(x) => Ok(x),
                Err(e) => {
                    eprintln!("put block failed: {:?}", e);
                    Err(e.into())
                }
            }
        })
        .await
        .map_err(|e| anyhow!("block upload failed after retry: {:?}", e))?;
    }

    Ok(())
}

async fn queue_blocks(
    mut file: File,
    s: Sender<UploadChunk>,
    file_size: usize,
    block_size: usize,
) -> Result<BlockList> {
    let mut block_list = BlockList::default();
    let mut sent = 0;

    for i in 0..usize::MAX {
        if sent >= file_size {
            break;
        }

        let send_size = cmp::min(block_size, file_size - sent);
        let mut data = BytesMut::new();
        data.resize(send_size, 0);
        file.read_exact(&mut data)
            .await
            .context("unable to read from file")?;
        let block_id = Bytes::from(format!("{:032x}", i));
        block_list
            .blocks
            .push(BlobBlockType::Uncommitted(BlockId::new(block_id.clone())));

        let data = data.freeze();

        s.send(UploadChunk {
            id: block_id.clone(),
            data,
        })
        .await?;

        sent += send_size;
    }
    s.close();

    Ok(block_list)
}

async fn spawn_uploaders(
    count: usize,
    blob_client: Arc<BlobClient>,
    r: Receiver<UploadChunk>,
) -> Result<()> {
    let uploaders: Vec<_> = (0..usize::max(1, count))
        .map(|_| tokio::spawn(upload_blocks(blob_client.clone(), r.clone())))
        .collect();

    try_join_all(uploaders)
        .await
        .context("uploading blocks failed")?;

    Ok(())
}

fn calc_concurrency(
    file_size: usize,
    block_size: Option<usize>,
    upload_concurrency: Option<usize>,
) -> Result<(usize, usize)> {
    if file_size > BLOB_MAX_FILE_SIZE {
        bail!("file is too large to upload");
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
                _ => (file_size / BLOB_MAX_BLOCKS) + 1,
            }
        }
        // minimum required to hit high throughput performance thresholds
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

fn get_client(sas: &Url) -> Result<Arc<BlobClient>> {
    let sas: SasToken = sas.try_into()?;

    let http_client = new_http_client();
    let blob_client = StorageAccountClient::new_sas_token(http_client, &sas.account, &sas.token)?
        .as_storage_client()
        .as_container_client(sas.container)
        .as_blob_client(sas.path);

    Ok(blob_client)
}

/// Upload a file to Azure Blob Store using a fully qualified SAS token
pub async fn upload_sas(
    filename: &Path,
    sas: &Url,
    block_size: Option<usize>,
    upload_concurrency: Option<usize>,
) -> Result<()> {
    let file = File::open(filename)
        .await
        .context("unable to open file for upload")?;

    let file_size: usize = file
        .metadata()
        .await?
        .len()
        .try_into()
        .context("unable to convert file size")?;

    // block sizes are multiples of ONE_MB.
    let block_size = block_size.map(|x| x.saturating_mul(ONE_MB));

    let (block_size, uploaders_count) =
        calc_concurrency(file_size, block_size, upload_concurrency)?;

    let (s, r) = bounded::<UploadChunk>(1);

    let blob_client = get_client(&sas)?;

    let uploaders = spawn_uploaders(uploaders_count, blob_client.clone(), r);
    let queue_handle = queue_blocks(file, s, file_size, block_size);

    let (block_list, ()) = futures::try_join!(queue_handle, uploaders)?;

    retry(ExponentialBackoff::default(), || async {
        let result = blob_client.put_block_list(&block_list).execute().await?;
        Ok(result)
    })
    .await
    .map_err(|e| anyhow!("block upload failed: {:?}", e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_GB: usize = ONE_MB * 1024;
    const ONE_TB: usize = ONE_GB * 1024;

    #[test]
    fn test_calc_concurrency() -> Result<()> {
        assert_eq!(
            (BLOB_MIN_BLOCK_SIZE, 1),
            calc_concurrency(ONE_MB * 300, Some(1), Some(1))?,
            "specified blocksize would overflow block count, so we use the minimum block size"
        );

        assert_eq!(
            (BLOB_MIN_BLOCK_SIZE, 10),
            calc_concurrency(ONE_GB * 30, Some(ONE_MB), None)?,
            "specifying block size of ONE_MB"
        );

        assert_eq!(
            (ONE_MB * 100, 2),
            calc_concurrency(ONE_GB * 30, Some(ONE_MB * 100), None)?,
            "specifying block size of 100MB but no concurrency"
        );

        assert_eq!(
            (5 * ONE_MB, 10),
            calc_concurrency(ONE_MB * 400, None, None)?,
            "uploading 400MB file, 5MB chunks, 10 uploaders",
        );

        assert_eq!(
            (5 * ONE_MB, 10),
            calc_concurrency(ONE_GB * 16, None, None)?,
            "uploading 50,000 MB file.   5MB chunks, 10 uploaders",
        );

        assert_eq!(
            (5 * ONE_MB, 10),
            calc_concurrency(ONE_GB * 32, None, None)?,
            "uploading 32GB file"
        );

        assert_eq!(
            (ONE_MB * 100, 2),
            calc_concurrency(ONE_TB, None, None)?,
            "uploading 1TB file"
        );

        assert_eq!(
            (100 * ONE_MB, 2),
            calc_concurrency(ONE_TB * 4, None, None)?,
            "uploading 5TB file.  100MB chunks, 2 uploaders"
        );

        assert_eq!(
            (100 * ONE_MB, 2),
            calc_concurrency(ONE_TB * 4, Some(0), None)?,
            "uploading 5TB file with zero blocksize.  100MB chunks, 2 uploaders"
        );

        assert_eq!(
            (100 * ONE_MB, 1),
            calc_concurrency(ONE_TB * 4, None, Some(0))?,
            "uploading 5TB file with zero concurrency.  100MB chunks, 1 uploader"
        );

        let (block_size, uploaders_count) = calc_concurrency(ONE_TB * 32, None, None)?;
        assert!(block_size > REASONABLE_BLOCK_SIZE && block_size < BLOB_MAX_BLOCK_SIZE);
        assert_eq!(uploaders_count, 1);

        assert!(
            calc_concurrency((BLOB_MAX_BLOCKS * BLOB_MAX_BLOCK_SIZE) + 1, None, None).is_err(),
            "files beyond max size should fail"
        );
        Ok(())
    }
}
