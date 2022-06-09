// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#[cfg(feature = "status")]
#[derive(Clone)]
pub struct Status {
    bar: Option<indicatif::ProgressBar>,
    total: Option<usize>,
}

#[cfg(feature = "status")]
impl Status {
    pub fn new(total: Option<usize>) -> Self {
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

#[cfg(not(feature = "status"))]
#[derive(Clone)]
pub struct Status {}

#[cfg(not(feature = "status"))]
impl Status {
    pub fn new(_total: Option<usize>) -> Self {
        Self {}
    }
    pub fn inc(&self, _n: usize) {}
}
