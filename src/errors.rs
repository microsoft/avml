use core::{
    error::Error as StdError,
    fmt::{Debug as FmtDebug, Formatter, Result as FmtResult},
};
use std::io::Error as IoError;

#[derive(thiserror::Error)]
pub enum Error {
    #[error("unable to create snapshot")]
    Image(#[from] crate::image::Error),

    #[error("unable to read memory")]
    Memory(#[from] crate::snapshot::Error),

    #[error("unable to parse /proc/iomem")]
    Iomem(#[from] crate::iomem::Error),

    #[cfg(feature = "put")]
    #[error("unable to upload file via PUT")]
    Upload(#[from] crate::upload::http::Error),

    #[cfg(feature = "blobstore")]
    #[error("unable to upload file to Azure Storage")]
    Blob(#[from] crate::upload::blobstore::Error),

    #[error("io error: {0}")]
    Io(#[source] IoError, &'static str),

    #[error("no conversion required")]
    NoConversionRequired,
}

pub(crate) fn format_error(e: &impl StdError, f: &mut Formatter) -> FmtResult {
    write!(f, "error: {e}")?;

    let mut source = e.source();

    if e.source().is_some() {
        writeln!(f, "\ncaused by:")?;
        let mut i: usize = 0;
        while let Some(inner) = source {
            writeln!(f, "{i: >5}: {inner}")?;
            source = inner.source();
            i = i.saturating_add(1);
        }
    }

    Ok(())
}

impl FmtDebug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        format_error(self, f)
    }
}
