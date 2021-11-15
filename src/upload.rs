// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use anyhow::{bail, Context, Result};
use reqwest;
use std::{fs::File, path::Path};

/// Upload a file via HTTP PUT
#[cfg(feature = "put")]
pub fn put(filename: &Path, url: reqwest::Url) -> Result<()> {
    let file = File::open(&filename)
        .with_context(|| format!("unable to open image file: {}", filename.display()))?;

    let client = reqwest::Client::new();
    let res = client
        .put(url)
        .header("x-ms-blob-type", "BlockBlob")
        .body(file)
        .send()
        .context("unable to PUT file")?;
    if res.status() != reqwest::StatusCode::CREATED {
        bail!("unable to upload memory to blob store");
    }
    Ok(())
}
