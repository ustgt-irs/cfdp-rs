use std::{env, fs, path::Path};

const PACKET_BUF_LEN_VAR: &str = "CFDP_PACKET_BUF_LEN";
const DEFAULT_PACKET_BUF_LEN: usize = 2048;
const MAX_MSGS_TO_USER_VAR: &str = "CFDP_MAX_MSGS_TO_USER";
const DEFAULT_MAX_MSGS_TO_USER: usize = 16;
// Sanity limit. The messages are stored on the stack while the user is notified.
const MAX_MAX_MSGS_TO_USER: usize = 256;

fn read_positive_env_var(var: &str, default: usize) -> usize {
    println!("cargo::rerun-if-env-changed={var}");
    match env::var(var) {
        Ok(value) => match value.trim().parse::<usize>() {
            Ok(len) if len > 0 => len,
            _ => panic!("{var} must be a positive integer, found {value:?}"),
        },
        Err(env::VarError::NotPresent) => default,
        Err(env::VarError::NotUnicode(value)) => {
            panic!("{var} must be a positive integer, found {value:?}")
        }
    }
}

fn main() {
    println!("cargo::rerun-if-changed=build.rs");

    let packet_buf_len = read_positive_env_var(PACKET_BUF_LEN_VAR, DEFAULT_PACKET_BUF_LEN);
    let max_msgs_to_user = read_positive_env_var(MAX_MSGS_TO_USER_VAR, DEFAULT_MAX_MSGS_TO_USER);
    if max_msgs_to_user > MAX_MAX_MSGS_TO_USER {
        panic!(
            "{MAX_MSGS_TO_USER_VAR} must not exceed {MAX_MAX_MSGS_TO_USER}, found {max_msgs_to_user}"
        );
    }

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR is set by cargo");
    fs::write(
        Path::new(&out_dir).join("build_config.rs"),
        format!(
            "/// Internal buffer size in bytes.\n\
             pub const PACKET_BUF_LEN: usize = {packet_buf_len};\n\
             /// Maximum number of messages to user per metadata PDU.\n\
             pub const MAX_MSGS_TO_USER: usize = {max_msgs_to_user};\n"
        ),
    )
    .expect("writing the generated build config file failed");
}
