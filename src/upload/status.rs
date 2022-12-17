// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#[cfg(all(feature = "status", any(feature = "blobstore", feature = "put")))]
use indicatif::{ProgressBar, ProgressFinish, ProgressStyle};

#[cfg(all(feature = "status", any(feature = "blobstore", feature = "put")))]
#[derive(Clone)]
pub struct Status {
    bar: Option<ProgressBar>,
    total: Option<u64>,
}

#[cfg(all(feature = "status", any(feature = "blobstore", feature = "put")))]
impl Status {
    pub fn new(total: Option<u64>) -> Self {
        let size = total.unwrap_or(0);
        let bar = if atty::is(atty::Stream::Stdin) {
            Some(
                ProgressBar::new(size)
                    .with_style(
                        #[allow(clippy::expect_used)]
                        ProgressStyle::default_bar()
                            .template("{bytes} ({bytes_per_sec})")
                            .expect("progress bar build failed"),
                    )
                    .with_finish(ProgressFinish::AndLeave),
            )
        } else {
            None
        };
        Self { bar, total }
    }

    pub fn inc(&self, n: usize) {
        if let Some(bar) = &self.bar {
            bar.inc(n as u64);
            if self.total.is_none() {
                bar.set_length(bar.position());
            }
        }
    }
}

#[cfg(all(not(feature = "status"), any(feature = "blobstore", feature = "put")))]
#[derive(Clone)]
pub struct Status {}

#[cfg(all(not(feature = "status"), any(feature = "blobstore", feature = "put")))]
impl Status {
    pub fn new(_total: Option<u64>) -> Self {
        Self {}
    }
    #[allow(clippy::unused_self)]
    pub fn inc(&self, _n: usize) {}
}
