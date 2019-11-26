// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#[macro_use]
extern crate clap;
extern crate avml;
extern crate byteorder;
extern crate elf;
extern crate snap;

use avml::ONE_MB;
use clap::{App, Arg};
use snap::Reader;
use std::convert::TryFrom;
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
                avml::image::copy_block(new_header, &mut image.src, &mut image.dst)?;
            }
            2 => {
                let mut reader = Reader::new(&image.src);
                avml::image::copy_block(new_header, &mut reader, &mut image.dst)?;
                image.src.seek(SeekFrom::Current(8))?;
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}

fn convert_to_raw(src: String, dst: String) -> Result<(), Box<dyn Error>> {
    let src_len = metadata(&src)?.len();
    let mut image = avml::image::Image::new(1, &src, &dst)?;

    loop {
        let current = image.src.seek(SeekFrom::Current(0))?;
        if current >= src_len {
            break;
        }
        let current_dst = image.dst.seek(SeekFrom::Current(0))?;

        let header = avml::image::Header::read(&image.src)?;
        let mut zeros = vec![0; ONE_MB];

        let mut unmapped = usize::try_from(header.range.start - current_dst)?;
        while unmapped > ONE_MB {
            image.dst.write_all(&zeros)?;
            unmapped -= ONE_MB;
        }
        if unmapped > 0 {
            zeros.resize(unmapped, 0);
            image.dst.write_all(&zeros)?;
        }

        let size = usize::try_from(header.range.end - header.range.start)?;

        match header.version {
            1 => {
                avml::image::copy(size, &mut image.src, &mut image.dst)?;
            }
            2 => {
                let mut reader = Reader::new(&image.src);
                avml::image::copy(size, &mut reader, &mut image.dst)?;
                image.src.seek(SeekFrom::Current(8))?;
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}

arg_enum! {
    #[allow(non_camel_case_types)]
    pub enum OutputFormat {
        raw,
        lime,
        lime_compressed
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let default_format = format!("{}", OutputFormat::lime);
    let args = App::new("avml-convert")
        .author(crate_authors!())
        .about("AVML compress/decompress tool")
        .version(crate_version!())
        .args(&[
            Arg::with_name("format")
                .long("format")
                .help("output format")
                .takes_value(true)
                .default_value(&default_format)
                .possible_values(&OutputFormat::variants()),
            Arg::with_name("source")
                .help("name of the source file to read to on local system")
                .required(true),
            Arg::with_name("destination")
                .help("name of the destination file to write to on local system")
                .required(true),
        ])
        .get_matches();

    let src = value_t!(args.value_of("source"), String)?;
    let dst = value_t!(args.value_of("destination"), String)?;

    let format = value_t!(args.value_of("format"), OutputFormat)?;

    match format {
        OutputFormat::raw => convert_to_raw(src, dst),
        OutputFormat::lime => convert(src, dst, false),
        OutputFormat::lime_compressed => convert(src, dst, true),
    }
}
