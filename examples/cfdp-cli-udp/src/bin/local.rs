use std::sync::mpsc;

use cfdp::request::PutRequestOwned;
use clap::Parser;
use spacepackets::cfdp::{
    TransmissionMode,
    lv::Lv,
    tlv::msg_to_user::{
        ProxyClosureRequestMessage, ProxyPutRequestMessageCreator, ProxyTransmissionModeMessage,
    },
};

use cfdp_cli_udp::{LOCAL_ID, LOCAL_PORT, REMOTE_ID, REMOTE_PORT, msg_to_user_owned};

#[derive(Debug, Copy, Clone, clap::ValueEnum)]
pub enum TransmissionModeCli {
    Nak,
    Ack,
}

impl From<TransmissionModeCli> for TransmissionMode {
    fn from(mode: TransmissionModeCli) -> Self {
        match mode {
            TransmissionModeCli::Nak => TransmissionMode::Unacknowledged,
            TransmissionModeCli::Ack => TransmissionMode::Acknowledged,
        }
    }
}

#[derive(clap::Parser)]
#[command(about = "CFDP local entity application")]
pub struct Cli {
    /// Source file path. With --proxy, this is a path on the remote entity.
    source: Option<String>,
    /// Destination file path. With --proxy, this is a path on the local entity.
    target: Option<String>,
    /// Send a proxy put request which instructs the remote entity to send the file back.
    #[arg(short, long)]
    proxy: bool,
    /// Transmission mode. Uses the remote entity configuration if omitted.
    #[arg(short, long)]
    mode: Option<TransmissionModeCli>,
    /// Request a Finished PDU in unacknowledged mode. Uses the remote entity configuration if
    /// omitted.
    #[arg(short, long)]
    closure_requested: Option<bool>,
}

fn build_put_request(cli: &Cli, source: &str, target: &str) -> PutRequestOwned {
    let trans_mode = cli.mode.map(TransmissionMode::from);
    if !cli.proxy {
        return PutRequestOwned::new_regular_request(
            REMOTE_ID.into(),
            source,
            target,
            trans_mode,
            cli.closure_requested,
        )
        .expect("creating put request failed");
    }
    let proxy_put_request = ProxyPutRequestMessageCreator::new(
        LOCAL_ID.into(),
        Lv::new_from_str(source).expect("source path too long"),
        Lv::new_from_str(target).expect("target path too long"),
    )
    .expect("creating proxy put request failed");
    let mut msgs_to_user = vec![msg_to_user_owned(&proxy_put_request)];
    if let Some(trans_mode) = trans_mode {
        msgs_to_user.push(msg_to_user_owned(&ProxyTransmissionModeMessage::new(
            trans_mode,
        )));
    }
    if let Some(closure_requested) = cli.closure_requested {
        msgs_to_user.push(msg_to_user_owned(&ProxyClosureRequestMessage::new(
            closure_requested,
        )));
    }
    let mut put_request = PutRequestOwned::new_msgs_to_user_only(REMOTE_ID.into(), &[])
        .expect("creating put request failed");
    put_request.msgs_to_user = Some(msgs_to_user);
    put_request
}

fn main() {
    let cli = Cli::parse();
    cfdp_cli_udp::setup_logger();
    let (put_req_tx, put_req_rx) = mpsc::channel();
    match (&cli.source, &cli.target) {
        (Some(source), Some(target)) => put_req_tx
            .send(build_put_request(&cli, source, target).into())
            .unwrap(),
        (None, None) => (),
        _ => panic!("source and target must both be specified"),
    }
    let stop = cfdp_cli_udp::stop_on_ctrl_c();
    let handles = cfdp_cli_udp::run_entity(
        LOCAL_ID.into(),
        REMOTE_ID.into(),
        LOCAL_PORT,
        REMOTE_PORT,
        put_req_tx,
        put_req_rx,
        stop,
    );
    for handle in handles {
        handle.join().expect("entity thread panicked");
    }
}
