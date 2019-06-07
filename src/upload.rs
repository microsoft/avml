// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use reqwest;
use std::error::Error;
use std::fs::File;

/// Upload a file via HTTP PUT
#[cfg(feature = "put")]
pub fn put(filename: &str, url: &str) -> Result<(), Box<dyn Error>> {
    let file = File::open(&filename)?;

    let client = reqwest::Client::new();
    let res = client
        .put(url)
        .header("x-ms-blob-type", "BlockBlob")
        .body(file)
        .send()?;
    if res.status() != reqwest::StatusCode::CREATED {
        return Err(From::from("unable to upload memory to blob store"));
    }
    Ok(())
}
