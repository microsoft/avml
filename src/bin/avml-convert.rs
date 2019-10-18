// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#[macro_use]
extern crate clap;
extern crate avml;
extern crate byteorder;
extern crate elf;
extern crate snap;

use clap::{App, Arg};
use snap::Reader;
use std::error::Error;
use std::fs::metadata;
use std::io::prelude::*;
use std::io::SeekFrom;

fn convert(src: String, dst: String, compress: bool) -> Result<(), Box<dyn Error>> {
    let src_len = metadata(&src)?.len();
    let mut image = avml::image::Image::new(1, &src, &dst)?;

    loop {
        let current = image.src.seek(SeekFrom::Current(0))?;
        if current >= src_len {
            break;
        }

        let header = avml::image::Header::read(&image.src)?;
        let mut new_header = header.clone();
        new_header.version = if compress { 2 } else { 1 };

        match header.version {
            1 => {
                avml::image::copy_block(&new_header, &mut image.src, &mut image.dst)?;
            }
            2 => {
                let mut reader = Reader::new(&image.src);
                avml::image::copy_block(&new_header, &mut reader, &mut image.dst)?;
                image.src.seek(SeekFrom::Current(8))?;
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}

fn run_app() -> Result<(), Box<dyn Error>> {
    let args = App::new("avml-convert")
        .author(crate_authors!())
        .about("AVML compress/decompress tool")
        .version(crate_version!())
        .args(&[
            Arg::with_name("compress")
                .long("compress")
                .help("compress pages via snappy"),
            Arg::with_name("source")
                .help("name of the source file to read to on local system")
                .required(true),
            Arg::with_name("destination")
                .help("name of the destination file to write to on local system")
                .required(true),
        ])
        .get_matches();

    let compress = args.is_present("compress");
    let src = value_t!(args.value_of("source"), String)?;
    let dst = value_t!(args.value_of("destination"), String)?;

    convert(src, dst, compress)?;
    Ok(())
}

fn main() {
    ::std::process::exit(match run_app() {
        Ok(_) => 0,
        Err(err) => {
            eprintln!("error: {:?}", err);
            1
        }
    });
}
