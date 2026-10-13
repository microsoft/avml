#![cfg(feature = "convert")]

use avml::image::{Format, Header, Image};
use std::{fs, io::Cursor, process::Command};

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

fn source_bytes(format: &str) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error>> {
    let payload = b"original forensic evidence";
    if format == "raw" {
        return Ok(payload.to_vec());
    }
    let mut image = Image::from_streams(
        if format == "lime" {
            Format::Lime
        } else {
            Format::AvmlCompressed
        },
        Cursor::new(payload),
        Cursor::new(Vec::new()),
    );
    image.copy_block(0..u64::try_from(payload.len())?)?;
    Ok(image.dst.into_inner())
}

#[test]
fn conversion_rejects_same_file_without_changing_source() -> Result {
    let dir = tempfile::tempdir()?;
    for (input_format, output_format) in [
        ("raw", "lime"),
        ("raw", "lime_compressed"),
        ("lime", "raw"),
        ("lime", "lime_compressed"),
        ("lime_compressed", "raw"),
        ("lime_compressed", "lime"),
    ] {
        for hard_link in [false, true] {
            let src = dir.path().join("source");
            let dst = dir.path().join("alias");
            let original = source_bytes(input_format)?;
            fs::write(&src, &original)?;
            if hard_link {
                fs::hard_link(&src, &dst)?;
            }
            let output = Command::new(env!("CARGO_BIN_EXE_avml"))
                .args([
                    "convert",
                    "--source-format",
                    input_format,
                    "--format",
                    output_format,
                ])
                .arg(&src)
                .arg(if hard_link { &dst } else { &src })
                .output()?;
            assert_eq!(
                fs::read(&src)?,
                original,
                "source changed: {input_format} -> {output_format}, hard_link={hard_link}"
            );
            assert!(!output.status.success());
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .contains("source and destination refer to the same file")
            );
            if hard_link {
                fs::remove_file(&dst)?;
            }
        }
    }
    Ok(())
}

#[test]
fn conversion_to_distinct_file_preserves_source_and_replaces_destination() -> Result {
    let dir = tempfile::tempdir()?;
    let src = dir.path().join("source");
    let dst = dir.path().join("output");
    let original = source_bytes("raw")?;
    fs::write(&src, &original)?;
    for existing in [false, true] {
        if existing {
            fs::write(&dst, vec![0xff; 8192])?;
        }
        let output = Command::new(env!("CARGO_BIN_EXE_avml"))
            .args(["convert", "--source-format", "raw", "--format", "lime"])
            .arg(&src)
            .arg(&dst)
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(fs::read(&src)?, original);
        let encoded = fs::read(&dst)?;
        let mut cursor = Cursor::new(encoded);
        assert_eq!(
            Header::read(&mut cursor)?.range,
            0..u64::try_from(original.len())?
        );
        let mut payload = Vec::new();
        std::io::Read::read_to_end(&mut cursor, &mut payload)?;
        assert_eq!(payload, original);
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn source_symlink_cannot_bypass_same_file_check() -> Result {
    let dir = tempfile::tempdir()?;
    let original = b"original forensic evidence";
    let dst = dir.path().join("original");
    let src = dir.path().join("source-link");
    fs::write(&dst, original)?;
    std::os::unix::fs::symlink(&dst, &src)?;
    let output = Command::new(env!("CARGO_BIN_EXE_avml"))
        .args(["convert", "--source-format", "raw", "--format", "lime"])
        .arg(&src)
        .arg(&dst)
        .output()?;
    assert_eq!(fs::read(&dst)?, original);
    assert!(!output.status.success());
    Ok(())
}

#[cfg(unix)]
#[test]
fn output_symlinks_stay_rejected_and_device_output_stays_supported() -> Result {
    let dir = tempfile::tempdir()?;
    let src = dir.path().join("source");
    let dst = dir.path().join("output-link");
    let original = b"original forensic evidence";
    fs::write(&src, original)?;
    std::os::unix::fs::symlink(&src, &dst)?;
    let output = Command::new(env!("CARGO_BIN_EXE_avml"))
        .args(["convert", "--source-format", "raw", "--format", "lime"])
        .arg(&src)
        .arg(&dst)
        .output()?;
    assert!(!output.status.success());
    assert_eq!(fs::read(&src)?, original);
    let device_output = Command::new(env!("CARGO_BIN_EXE_avml"))
        .args(["convert", "--source-format", "raw", "--format", "lime"])
        .arg(&src)
        .arg("/dev/null")
        .output()?;
    assert!(
        device_output.status.success(),
        "{}",
        String::from_utf8_lossy(&device_output.stderr)
    );
    assert_eq!(fs::read(&src)?, original);
    Ok(())
}
