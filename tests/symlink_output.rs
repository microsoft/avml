#[cfg(target_family = "unix")]
mod unix_only {
    use avml::image::Image;
    use byteorder::{LittleEndian, WriteBytesExt as _};
    use std::{
        fs,
        fs::File,
        io::Write,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn unique_tmp_dir() -> PathBuf {
        let mut d = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        d.push(format!(
            "avml_symlink_test_{}_{}",
            std::process::id(),
            nanos
        ));
        d
    }

    #[test]
    fn destination_symlink_is_rejected_and_does_not_truncate_target() {
        let dir = unique_tmp_dir();
        fs::create_dir_all(&dir).unwrap();

        let victim = dir.join("victim.txt");
        fs::write(&victim, b"SAFE_TEST_FILE\n").unwrap();
        let victim_len_before = fs::metadata(&victim).unwrap().len();

        let dst = dir.join("out.lime");
        std::os::unix::fs::symlink(&victim, &dst).unwrap();

        // minimal valid LiME input: header + 1 byte payload
        let src = dir.join("in.lime");
        let mut f = File::create(&src).unwrap();
        f.write_u32::<LittleEndian>(0x4c694d45).unwrap(); // LIME_MAGIC
        f.write_u32::<LittleEndian>(1).unwrap(); // version
        f.write_u64::<LittleEndian>(0).unwrap(); // start
        f.write_u64::<LittleEndian>(0).unwrap(); // end_inclusive
        f.write_u64::<LittleEndian>(0).unwrap(); // padding
        f.write_all(b"X").unwrap();
        drop(f);

        // Should fail to open destination symlink
        let res = Image::<File, File>::new(1, &src, &dst);
        assert!(res.is_err(), "expected error when dst is a symlink");

        // Victim must remain unchanged (not truncated)
        let victim_len_after = fs::metadata(&victim).unwrap().len();
        assert_eq!(victim_len_after, victim_len_before);

        let _ = fs::remove_dir_all(&dir);
    }
}
