// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::ONE_MB;
use anyhow::{anyhow, bail, Context, Result};
use azure::prelude::*;
use azure_sdk_core::{errors::AzureError, prelude::*};
use azure_sdk_storage_core::prelude::*;
use byteorder::{LittleEndian, WriteBytesExt};
use retry::{delay::jitter, delay::Exponential, retry, OperationResult};
use std::{cmp, convert::TryFrom, fs::File, io::prelude::*};
use tokio_core::reactor::Core;
use url::Url;

const BACKOFF: u64 = 100;
const BACKOFF_COUNT: usize = 100;
const MAX_BLOCK_SIZE: usize = ONE_MB * 100;

/// Converts the block index into an block_id
fn to_id(count: u64) -> Result<Vec<u8>> {
    let mut bytes = vec![];
    bytes
        .write_u64::<LittleEndian>(count)
        .with_context(|| format!("unable to create block_id: {}", count))?;
    Ok(bytes)
}

/// Parse a SAS token into the relevant components
fn parse(sas: &str) -> Result<(String, String, String)> {
    let parsed = Url::parse(sas).context("unable to parse url")?;
    let account = if let Some(host) = parsed.host_str() {
        let v: Vec<&str> = host.split_terminator('.').collect();
        v[0]
    } else {
        bail!("invalid sas token (no account)");
    };

    let path = parsed.path();
    let mut v: Vec<&str> = path.split_terminator('/').collect();
    v.remove(0);
    let container = v.remove(0);
    let blob_path = v.join("/");
    Ok((account.to_string(), container.to_string(), blob_path))
}

/// Upload a file to Azure Blob Store using a fully qualified SAS token
pub fn upload_sas(filename: &str, sas: &str, block_size: usize) -> Result<()> {
    let block_size = cmp::min(block_size, MAX_BLOCK_SIZE);
    let (account, container, path) = parse(sas).context("unable to parse SAS url")?;
    let client = Client::azure_sas(&account, sas)
        .map_err(|e| anyhow!("creating blob client failed: {:?}", e))?;

    let mut core = Core::new().context("unable to create tokio context")?;
    let mut file = File::open(filename).context("unable to open snapshot")?;
    let size = usize::try_from(
        file.metadata()
            .context("unable to get file metadata")?
            .len(),
    )
    .context("unable to convert file size")?;
    let mut sent = 0;
    let mut blocks = BlockList { blocks: Vec::new() };
    let mut data = vec![0; block_size];
    while sent < size {
        let send_size = cmp::min(block_size, size - sent);
        let block_id = to_id(sent as u64)?;
        data.resize(send_size, 0);
        file.read_exact(&mut data)
            .context("unable to read image block")?;

        retry(
            Exponential::from_millis(BACKOFF)
                .map(jitter)
                .take(BACKOFF_COUNT),
            || {
                let response = core.run(
                    client
                        .put_block()
                        .with_container_name(&container)
                        .with_blob_name(&path)
                        .with_body(&data)
                        .with_block_id(&block_id)
                        .finalize(),
                );

                match response {
                    Ok(x) => OperationResult::Ok(x),
                    Err(x) => match x {
                        AzureError::HyperError(_) => OperationResult::Retry(x),
                        _ => OperationResult::Err(x),
                    },
                }
            },
        )
        .map_err(|x| anyhow!("put_block_list failed: {:?}", x))?;

        blocks.blocks.push(BlobBlockType::Uncommitted(block_id));
        sent += send_size;
    }

    retry(
        Exponential::from_millis(BACKOFF)
            .map(jitter)
            .take(BACKOFF_COUNT),
        || {
            let response = core.run(
                client
                    .put_block_list()
                    .with_container_name(&container)
                    .with_blob_name(&path)
                    .with_block_list(&blocks)
                    .finalize(),
            );

            match response {
                Ok(x) => OperationResult::Ok(x),
                Err(x) => match x {
                    AzureError::HyperError(_) => OperationResult::Retry(x),
                    _ => OperationResult::Err(x),
                },
            }
        },
    )
    .map_err(|x| anyhow!("put_block_list failed: {:?}", x))?;

    Ok(())
}
