//! Internal buffer size used for PDU generation and checksum calculation.
//!
//! The size is set with the `CFDP_PACKET_BUF_LEN` environment variable at build time.
include!(concat!(env!("OUT_DIR"), "/buf_len.rs"));
