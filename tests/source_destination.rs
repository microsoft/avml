// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#![cfg(feature = "convert")]

use avml::image::{Format, Image};
use std::{fs, io::Cursor, path::Path, process::Command};

type Result = core::result::Result<(), Box<dyn core::error::Error>>;

fn convert(
    src: &Path,
    dst: &Path,
    input: &str,
    output: &str,
) -> std::io::Result<std::process::Output> {
    Command::new(env!("CARGO_BIN_EXE_avml"))
        .args(["convert", "--source-format", input, "--format", output])
        .arg(src)
        .arg(dst)
        .output()
}

fn source_bytes(format: &str) -> core::result::Result<Vec<u8>, avml::image::Error> {
    let bytes = b"original evidence";
    if format == "raw" {
        return Ok(bytes.to_vec());
    }
    let format = if format == "lime" {
        Format::Lime
    } else {
        Format::AvmlCompressed
    };
    let mut image = Image::from_streams(format, Cursor::new(bytes), Vec::new());
    image.copy_block(0..u64::try_from(bytes.len())?)?;
    Ok(image.dst)
}

#[test]
fn conversions_preserve_all_existing_destinations_and_source_aliases() -> Result {
    let dir = tempfile::tempdir()?;
    let src = dir.path().join("source");
    let dst = dir.path().join("output");
    for (input, output) in [
        ("raw", "lime"),
        ("raw", "lime_compressed"),
        ("lime", "raw"),
        ("lime", "lime_compressed"),
        ("lime_compressed", "raw"),
        ("lime_compressed", "lime"),
    ] {
        let bytes = source_bytes(input)?;
        fs::write(&src, &bytes)?;
        fs::write(&dst, b"existing evidence")?;
        assert!(!convert(&src, &dst, input, output)?.status.success());
        assert_eq!(fs::read(&dst)?, b"existing evidence");
        assert!(!convert(&src, &src, input, output)?.status.success());
        fs::remove_file(&dst)?;
        fs::hard_link(&src, &dst)?;
        assert!(!convert(&src, &dst, input, output)?.status.success());
        assert_eq!(fs::read(&src)?, bytes);
        fs::remove_file(&dst)?;

        let result = convert(&src, &dst, input, output)?;
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_ne!(fs::read(&dst)?, Vec::<u8>::new());
        assert_eq!(fs::read(&src)?, bytes);
        fs::remove_file(&dst)?;
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn symlinks_and_dangling_symlinks_are_rejected() -> Result {
    let dir = tempfile::tempdir()?;
    let src = dir.path().join("source");
    let dst = dir.path().join("output");
    let missing = dir.path().join("missing");
    fs::write(&src, b"evidence")?;
    for target in [&src, &missing] {
        std::os::unix::fs::symlink(target, &dst)?;
        assert!(!convert(&src, &dst, "raw", "lime")?.status.success());
        assert_eq!(fs::read(&src)?, b"evidence");
        assert!(!missing.exists());
        fs::remove_file(&dst)?;
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn only_exact_device_exceptions_allow_existing_outputs() -> Result {
    use std::io::Write as _;

    let dir = tempfile::tempdir()?;
    let src = dir.path().join("source");
    fs::write(&src, b"evidence")?;
    let null_output = convert(&src, Path::new("/dev/null"), "raw", "lime")?;
    assert!(null_output.status.success());

    let stdout_output = convert(&src, Path::new("/dev/stdout"), "raw", "lime")?;
    assert!(
        stdout_output.status.success(),
        "{}",
        String::from_utf8_lossy(&stdout_output.stderr)
    );
    let mut expected = Image::from_streams(Format::Lime, Cursor::new(b"evidence"), Vec::new());
    expected.copy_block(0..8)?;
    assert_eq!(stdout_output.stdout, expected.dst);

    let dst = dir.path().join("redirected");
    let mut file = fs::File::create(&dst)?;
    file.write_all(&[0x55; 256])?;
    let redirected_output = Command::new(env!("CARGO_BIN_EXE_avml"))
        .args(["convert", "--source-format", "raw", "--format", "lime"])
        .arg(&src)
        .arg("/dev/stdout")
        .stdout(file)
        .output()?;
    assert!(redirected_output.status.success());
    let mut expected_file = expected.dst;
    expected_file.resize(256, 0x55);
    assert_eq!(fs::read(&dst)?, expected_file);

    let alias = dir.path().join("null-alias");
    std::os::unix::fs::symlink("/dev/null", &alias)?;
    assert!(!convert(&src, &alias, "raw", "lime")?.status.success());
    assert!(
        !convert(&src, Path::new("/dev/zero"), "raw", "lime")?
            .status
            .success()
    );
    Ok(())
}
