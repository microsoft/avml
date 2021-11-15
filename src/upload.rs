// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use anyhow::{bail, Context, Result};
use reqwest::{Body, Client, StatusCode};
use std::path::Path;
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};
use url::Url;

/// Upload a file via HTTP PUT
#[cfg(feature = "put")]
pub async fn put(filename: &Path, url: &Url) -> Result<()> {
    let file = File::open(&filename)
        .await
        .with_context(|| format!("unable to open image file: {}", filename.display()))?;

    let size = file
        .metadata()
        .await
        .context("unable to get file size")?
        .len();

    let stream = FramedRead::new(file, BytesCodec::new());
    let body = Body::wrap_stream(stream);

    let client = Client::new();
    let res = client
        .put(url.clone())
        .header("x-ms-blob-type", "BlockBlob")
        .header("Content-Length", size)
        .body(body)
        .send()
        .await
        .context("unable to PUT file")?;

    if res.status() != StatusCode::CREATED {
        bail!("unable to upload memory to blob store");
    }
    Ok(())
}
