// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#[cfg(all(feature = "status", any(feature = "blobstore", feature = "put")))]
use indicatif::{ProgressBar, ProgressFinish, ProgressStyle};
#[cfg(all(feature = "status", any(feature = "blobstore", feature = "put")))]
use std::io::{IsTerminal as _, stdin};

#[cfg(all(feature = "status", any(feature = "blobstore", feature = "put")))]
#[derive(Clone, Debug)]
pub struct Status {
    bar: Option<ProgressBar>,
    total: Option<u64>,
}

#[cfg(all(feature = "status", any(feature = "blobstore", feature = "put")))]
impl Status {
    pub fn new(total: Option<u64>) -> Self {
        let size = total.unwrap_or(0);
        let bar = stdin().is_terminal().then(|| {
            ProgressBar::new(size)
                .with_style(
                    #[expect(
                        clippy::expect_used,
                        reason = "template string is a compile-time literal known to be valid"
                    )]
                    ProgressStyle::default_bar()
                        .template("{bytes} ({bytes_per_sec})")
                        .expect("progress bar build failed"),
                )
                .with_finish(ProgressFinish::AndLeave)
        });
        Self { bar, total }
    }

    #[cfg(feature = "blobstore")]
    pub fn reset(&self) {
        if let Some(ref bar) = self.bar {
            bar.reset();
        }
    }

    pub fn inc(&self, n: usize) {
        if let Some(ref bar) = self.bar {
            bar.inc(n.try_into().unwrap_or(u64::MAX));
            if self.total.is_none() {
                bar.set_length(bar.position());
            }
        }
    }
}

#[cfg(all(not(feature = "status"), any(feature = "blobstore", feature = "put")))]
#[derive(Clone, Debug)]
pub struct Status {}

#[cfg(all(not(feature = "status"), any(feature = "blobstore", feature = "put")))]
impl Status {
    pub fn new(_total: Option<u64>) -> Self {
        Self {}
    }
    #[expect(
        clippy::unused_self,
        reason = "API parity with the status-enabled variant"
    )]
    pub fn inc(&self, _n: usize) {}

    #[cfg(feature = "blobstore")]
    #[expect(
        clippy::unused_self,
        reason = "API parity with the status-enabled variant"
    )]
    pub fn reset(&self) {}
}
