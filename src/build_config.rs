//! Constants set with environment variables at build time.
//!
//! - `CFDP_PACKET_BUF_LEN`: Buffer size used for PDU generation, checksum calculation and for
//!   storing received messages to user.
//! - `CFDP_MAX_MSGS_TO_USER`: Maximum number of messages to user per metadata PDU.
include!(concat!(env!("OUT_DIR"), "/build_config.rs"));
