// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use azure::prelude::*;
use azure_sdk_core::errors::AzureError;
use azure_sdk_core::prelude::*;
use azure_sdk_storage_core::prelude::*;

use byteorder::{LittleEndian, WriteBytesExt};
use retry::{delay::jitter, delay::Exponential, retry, OperationResult};
use std::cmp;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use tokio_core::reactor::Core;
use url::Url;

const BACKOFF: u64 = 100;
const BACKOFF_COUNT: usize = 100;
const MAX_BLOCK_SIZE: usize = 1024 * 1024 * 100;

/// Converts the block index into an block_id
fn to_id(count: u64) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut bytes = vec![];
    bytes.write_u64::<LittleEndian>(count)?;
    Ok(bytes)
}

/// Parse a SAS token into the relevant components
fn parse(sas: &str) -> Result<(String, String, String), Box<dyn Error>> {
    let parsed = Url::parse(sas)?;
    let account = if let Some(host) = parsed.host_str() {
        let v: Vec<&str> = host.split_terminator('.').collect();
        v[0]
    } else {
        return Err(From::from("invalid sas token (no account)"));
    };

    let path = parsed.path();
    let mut v: Vec<&str> = path.split_terminator('/').collect();
    v.remove(0);
    let container = v.remove(0);
    let blob_path = v.join("/");
    Ok((account.to_string(), container.to_string(), blob_path))
}

/// Upload a file to Azure Blob Store using a fully qualified SAS token
pub fn upload_sas(filename: &str, sas: &str, block_size: usize) -> Result<(), Box<dyn Error>> {
    let block_size = cmp::min(block_size, MAX_BLOCK_SIZE);
    let (account, container, path) = parse(sas)?;
    let client = Client::azure_sas(&account, sas)?;

    let mut core = Core::new()?;
    let mut file = File::open(filename)?;
    let size = file.metadata()?.len() as usize;
    let mut sent = 0;
    let mut blocks = BlockList { blocks: Vec::new() };
    let mut data = vec![0; block_size];
    while sent < size {
        let send_size = cmp::min(block_size, size - sent);
        let block_id = to_id(sent as u64)?;
        data.resize(send_size, 0);
        file.read_exact(&mut data)?;

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
        )?;

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
    )?;

    Ok(())
}
