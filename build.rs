use std::{env, fs, path::Path};

const PACKET_BUF_LEN_VAR: &str = "CFDP_PACKET_BUF_LEN";
const DEFAULT_PACKET_BUF_LEN: usize = 2048;

fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-env-changed={PACKET_BUF_LEN_VAR}");

    let packet_buf_len = match env::var(PACKET_BUF_LEN_VAR) {
        Ok(value) => match value.trim().parse::<usize>() {
            Ok(len) if len > 0 => len,
            _ => panic!("{PACKET_BUF_LEN_VAR} must be a positive integer, found {value:?}"),
        },
        Err(env::VarError::NotPresent) => DEFAULT_PACKET_BUF_LEN,
        Err(env::VarError::NotUnicode(value)) => {
            panic!("{PACKET_BUF_LEN_VAR} must be a positive integer, found {value:?}")
        }
    };

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR is set by cargo");
    fs::write(
        Path::new(&out_dir).join("buf_len.rs"),
        format!(
            "/// Internal buffer size in bytes.\npub const PACKET_BUF_LEN: usize = {packet_buf_len};\n"
        ),
    )
    .expect("writing the generated buffer length file failed");
}
