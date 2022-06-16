// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#[cfg(all(feature = "status", any(feature = "blobstore", feature = "put")))]
#[derive(Clone)]
pub struct Status {
    bar: Option<indicatif::ProgressBar>,
    total: Option<u64>,
}

#[cfg(all(feature = "status", any(feature = "blobstore", feature = "put")))]
impl Status {
    pub fn new(total: Option<u64>) -> Self {
        let size = total.unwrap_or(0) as u64;
        let bar = if atty::is(atty::Stream::Stdin) {
            Some(
                indicatif::ProgressBar::new(size).with_style(
                    indicatif::ProgressStyle::default_bar()
                        .template("{bytes} ({bytes_per_sec})")
                        .on_finish(indicatif::ProgressFinish::AtCurrentPos),
                ),
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
