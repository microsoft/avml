// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::ONE_MB;
use anyhow::{anyhow, bail, Context, Result};
use azure_core::new_http_client;
use azure_storage::core::prelude::*;
use azure_storage_blobs::prelude::*;
use backoff::{future::retry, ExponentialBackoff};
use bytes::{Bytes, BytesMut};
use std::{cmp, convert::TryFrom, path::Path};
use tokio::{fs::File, io::AsyncReadExt};
use url::Url;

const MAX_BLOCK_SIZE: usize = ONE_MB * 100;

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

/// Upload a file to Azure Blob Store using a fully qualified SAS token
pub async fn upload_sas(filename: &Path, sas: &Url, block_size: usize) -> Result<()> {
    let block_size = cmp::min(block_size * ONE_MB, MAX_BLOCK_SIZE);

    let sas: SasToken = sas.try_into()?;
    let http_client = new_http_client();
    let storage_account_client =
        StorageAccountClient::new_sas_token(http_client, &sas.account, &sas.token)?;
    let storage_client = storage_account_client.as_storage_client();
    let container_client = storage_client.as_container_client(sas.container);
    let blob_client = container_client.as_blob_client(sas.path);

    let mut file = File::open(filename)
        .await
        .context("unable to open file for upload")?;

    let size: usize = file
        .metadata()
        .await?
        .len()
        .try_into()
        .context("unable to convert file size")?;

    let mut block_list = BlockList::default();
    let mut sent = 0;
    for i in 0..usize::MAX {
        if sent >= size {
            break;
        }

        let send_size = cmp::min(block_size, size - sent);
        let mut data = BytesMut::new();
        data.resize(send_size, 0);
        file.read_exact(&mut data)
            .await
            .context("unable to read from file")?;
        let block_id = Bytes::from(format!("{:032x}", i));
        let hash = md5::compute(data.clone()).into();
        block_list
            .blocks
            .push(BlobBlockType::Uncommitted(BlockId::new(block_id.clone())));

        let data = data.freeze();

        retry(ExponentialBackoff::default(), || async {
            let data_for_req = (&data.clone()).to_owned();
            let block_id_for_req = (&block_id.clone()).to_owned();

            let result = blob_client
                .put_block(block_id_for_req, data_for_req)
                .hash(&hash)
                .execute()
                .await;
            match result {
                Ok(x) => Ok(x),
                Err(e) => {
                    eprintln!("put block failed: {:?}", e);
                    Err(e.into())
                    // Err(e)?
                }
            }
        })
        .await
        .map_err(|e| anyhow!("block upload failed after retry: {:?}", e))?;

        sent += send_size;
    }

    retry(ExponentialBackoff::default(), || async {
        let result = blob_client.put_block_list(&block_list).execute().await?;
        Ok(result)
    })
    .await
    .map_err(|e| anyhow!("block upload failed: {:?}", e))?;

    Ok(())
}
