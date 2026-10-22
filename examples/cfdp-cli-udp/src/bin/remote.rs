use std::sync::mpsc;

use cfdp_cli_udp::{LOCAL_ID, LOCAL_PORT, REMOTE_ID, REMOTE_PORT};

fn main() {
    cfdp_cli_udp::setup_logger();
    let (put_req_tx, put_req_rx) = mpsc::channel();
    let stop = cfdp_cli_udp::stop_on_ctrl_c();
    let handles = cfdp_cli_udp::run_entity(
        REMOTE_ID.into(),
        LOCAL_ID.into(),
        REMOTE_PORT,
        LOCAL_PORT,
        put_req_tx,
        put_req_rx,
        stop,
    );
    for handle in handles {
        handle.join().expect("entity thread panicked");
    }
}
