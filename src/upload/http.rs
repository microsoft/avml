// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use crate::upload::status::Status;
use futures::stream::StreamExt;
use reqwest::{Body, Client, StatusCode};
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};
use url::Url;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error reading file: {1}")]
    Io(#[source] std::io::Error, PathBuf),

    #[error("HTTP request error")]
    Request(#[from] reqwest::Error),

    #[error("unexpected status code: {status}")]
    UnexpectedStatusCode { status: u16 },
}

/// Upload a file via HTTP PUT
#[cfg(feature = "put")]
pub async fn put(filename: &Path, url: &Url) -> Result<(), Error> {
    let file = File::open(&filename)
        .await
        .map_err(|e| Error::Io(e, filename.to_owned()))?;

    let size = file
        .metadata()
        .await
        .map_err(|e| Error::Io(e, filename.to_owned()))?
        .len();

    let status = Status::new(Some(size));

    let stream = FramedRead::new(file, BytesCodec::new()).inspect(move |x| {
        if let Ok(x) = x {
            status.inc(x.len());
        }
    });

    let body = Body::wrap_stream(stream);

    let client = Client::new();
    let res = client
        .put(url.clone())
        .header("x-ms-blob-type", "BlockBlob")
        .header("Content-Length", size)
        .body(body)
        .send()
        .await?;

    if res.status() != StatusCode::CREATED {
        return Err(Error::UnexpectedStatusCode {
            status: res.status().as_u16(),
        });
    }

    Ok(())
}
