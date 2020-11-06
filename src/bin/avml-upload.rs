// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#[macro_use]
extern crate clap;
extern crate avml;

#[cfg(feature = "blobstore")]
use avml::ONE_MB;

use clap::{App, Arg};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let args = App::new(crate_name!())
        .author(crate_authors!())
        .about(crate_description!())
        .version(crate_version!())
        .args(&[
            Arg::with_name("filename")
                .help("name of the file to upload from the local system")
                .required(true),
            Arg::with_name("sas_url")
                .long("sas_url")
                .takes_value(true)
                .help("Upload via Azure Blob Store upon acquisition")
                .conflicts_with("url"),
            Arg::with_name("sas_block_size")
                .long("sas_block_size")
                .takes_value(true)
                .help("specify maximum block size in MiB"),
            Arg::with_name("url")
                .long("url")
                .takes_value(true)
                .help("Upload via HTTP PUT upon acquisition.")
                .required(false),
        ])
        .get_matches();

    let dst = value_t!(args.value_of("filename"), String)?;

    let url = args.value_of("url");
    if let Some(url) = url {
        avml::upload::put(&dst, url)?;
    }

    let sas_url = args.value_of("sas_url");
    let sas_block_size = if args.is_present("sas_block_size") {
        value_t!(args.value_of("sas_block_size"), usize)?
    } else {
        100
    } * ONE_MB;

    if let Some(sas_url) = sas_url {
        avml::blobstore::upload_sas(&dst, sas_url, sas_block_size)?;
    }

    Ok(())
}
