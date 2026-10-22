use std::{
    io::{self, ErrorKind},
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU16, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use cfdp::{
    FaultInfo, IndicationConfig, LocalEntityConfig, PduOwnedWithInfo, PduProvider,
    RemoteEntityConfig, State, StdTimerCreator, TransactionId, UserFaultHook,
    dest::DestinationHandler,
    filestore::NativeFilestore,
    lost_segments::LostSegmentsList,
    request::{PutRequestOwned, ReadablePutRequest},
    source::SourceHandler,
    user::{CfdpUser, FileSegmentRecvdParams, MetadataReceivedParams, TransactionFinishedParams},
};
use log::{info, warn};
use spacepackets::{
    cfdp::{
        ChecksumType, ConditionCode, TransmissionMode,
        pdu::{
            PduError,
            finished::{DeliveryCode, FileStatus},
        },
        tlv::{
            TlvOwned, WritableTlv,
            msg_to_user::{
                MsgToUserTlv, OriginatingTransactionIdMessage, ProxyClosureRequestMessage,
                ProxyPutRequestMessageReader, ProxyPutResponseMessage,
                ProxyTransmissionModeMessage, ReservedMessageCreationError, ReservedMessageType,
            },
        },
    },
    util::{UnsignedByteField, UnsignedByteFieldU16},
};

pub const LOCAL_ID: UnsignedByteFieldU16 = UnsignedByteFieldU16::new(1);
pub const REMOTE_ID: UnsignedByteFieldU16 = UnsignedByteFieldU16::new(2);

pub const LOCAL_PORT: u16 = 5111;
pub const REMOTE_PORT: u16 = 5222;

const LOG_LEVEL: log::LevelFilter = log::LevelFilter::Info;

pub fn setup_logger() {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                std::thread::current().name().expect("thread is not named"),
                record.level(),
                message
            ))
        })
        .level(LOG_LEVEL)
        .chain(std::io::stdout())
        .apply()
        .unwrap();
}

/// Returns a flag which is set when Ctrl+C is pressed.
pub fn stop_on_ctrl_c() -> Arc<AtomicBool> {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_handler = stop.clone();
    ctrlc::set_handler(move || {
        info!("stopping entity");
        stop_handler.store(true, Ordering::Relaxed);
    })
    .expect("setting Ctrl+C handler failed");
    stop
}

pub fn msg_to_user_owned(msg: &impl WritableTlv) -> TlvOwned {
    MsgToUserTlv::from_bytes(&msg.to_vec())
        .expect("invalid message to user")
        .to_owned()
}

/// Formats a transaction ID as `<source ID>:<sequence number>`.
pub fn fmt_id(id: &TransactionId) -> String {
    format!("{}:{}", id.source_id().value(), id.seq_num().value())
}

#[derive(Default)]
pub struct ExampleFaultHandler {}

impl UserFaultHook for ExampleFaultHandler {
    fn notice_of_suspension_cb(&mut self, fault_info: FaultInfo) {
        warn!("notice of suspension: {:?}", fault_info);
    }

    fn notice_of_cancellation_cb(&mut self, fault_info: FaultInfo) {
        warn!("notice of cancellation: {:?}", fault_info);
    }

    fn abandoned_cb(&mut self, fault_info: FaultInfo) {
        warn!("abandoned: {:?}", fault_info);
    }

    fn ignore_cb(&mut self, fault_info: FaultInfo) {
        warn!("ignored fault: {:?}", fault_info);
    }
}

/// Put request for the source handler of this entity.
pub struct EntityPutRequest {
    pub request: PutRequestOwned,
    /// Set if the request was created for a proxy put request. The source user needs it to send
    /// the proxy put response once the transaction finished.
    pub originating_id: Option<TransactionId>,
}

impl From<PutRequestOwned> for EntityPutRequest {
    fn from(request: PutRequestOwned) -> Self {
        Self {
            request,
            originating_id: None,
        }
    }
}

pub struct ExampleCfdpUser {
    /// Put requests sent here are executed by the source handler of this entity.
    put_req_tx: mpsc::Sender<EntityPutRequest>,
    /// Transaction started by a proxy put request and the originating transaction. There is
    /// only one source handler, so only one such transaction can be active.
    active_proxy_put_req: Option<(TransactionId, TransactionId)>,
}

impl ExampleCfdpUser {
    pub fn new(put_req_tx: mpsc::Sender<EntityPutRequest>) -> Self {
        Self {
            put_req_tx,
            active_proxy_put_req: None,
        }
    }

    fn handle_proxy_put_request(
        &mut self,
        id: &TransactionId,
        proxy_put_request: &ProxyPutRequestMessageReader,
        trans_mode: Option<TransmissionMode>,
        closure_requested: Option<bool>,
    ) -> Result<(), ProxyPutError> {
        let (Some(Ok(src_file)), Some(Ok(dest_file))) = (
            proxy_put_request.source_file_name().value_as_str(),
            proxy_put_request.dest_file_name().value_as_str(),
        ) else {
            return Err(ProxyPutError::InvalidFileNames);
        };
        info!(
            "Proxy put request: send {} to {} on entity {}",
            src_file,
            dest_file,
            proxy_put_request.destination_entity_id().value()
        );
        let mut put_request = PutRequestOwned::new_regular_request(
            proxy_put_request.destination_entity_id(),
            src_file,
            dest_file,
            trans_mode,
            closure_requested,
        )
        .map_err(|e| ProxyPutError::FilePathTooLarge(e.0))?;
        let originating_id = OriginatingTransactionIdMessage::new(*id.source_id(), *id.seq_num())?;
        put_request.msgs_to_user = Some(vec![msg_to_user_owned(&originating_id)]);
        self.put_req_tx
            .send(EntityPutRequest {
                request: put_request,
                originating_id: Some(*id),
            })
            .map_err(|_| ProxyPutError::Send)
    }

    /// Packages a proxy put response and an originating transaction ID like specified in the
    /// standard and sends them back to the originating entity.
    fn send_proxy_put_response(
        &self,
        originating_id: &TransactionId,
        response: &ProxyPutResponseMessage,
    ) {
        let originating_id_msg = OriginatingTransactionIdMessage::new(
            *originating_id.source_id(),
            *originating_id.seq_num(),
        )
        .expect("creating originating transaction ID failed");
        let mut put_request =
            PutRequestOwned::new_msgs_to_user_only(*originating_id.source_id(), &[])
                .expect("creating put request failed");
        put_request.msgs_to_user = Some(vec![
            msg_to_user_owned(response),
            msg_to_user_owned(&originating_id_msg),
        ]);
        info!(
            "Sending proxy put response and originating transaction ID {} back to entity {}",
            fmt_id(originating_id),
            originating_id.source_id().value()
        );
        self.put_req_tx
            .send(put_request.into())
            .expect("sending put request failed");
    }
}

impl CfdpUser for ExampleCfdpUser {
    fn transaction_indication(&mut self, id: &TransactionId) {
        info!("Transaction indication for {}", fmt_id(id));
    }

    fn eof_sent_indication(&mut self, id: &TransactionId) {
        info!("EOF sent for transaction {}", fmt_id(id));
    }

    fn transaction_finished_indication(&mut self, finished_params: &TransactionFinishedParams) {
        info!(
            "Transaction {} finished: {:?}, {:?}, {:?}",
            fmt_id(&finished_params.id),
            finished_params.condition_code,
            finished_params.delivery_code,
            finished_params.file_status
        );
        // Now check whether there are any pending proxy operations that require a response.
        let Some((_, originating_id)) = self
            .active_proxy_put_req
            .take_if(|(id, _)| *id == finished_params.id)
        else {
            return;
        };
        let response = ProxyPutResponseMessage::new(
            finished_params.condition_code,
            finished_params.delivery_code,
            finished_params.file_status,
        );
        self.send_proxy_put_response(&originating_id, &response);
    }

    fn metadata_recvd_indication(&mut self, md_recvd_params: &MetadataReceivedParams) {
        info!(
            "Metadata received for transaction {}: {} -> {}, {} bytes, {} messages to user",
            fmt_id(&md_recvd_params.id),
            md_recvd_params.src_file_name,
            md_recvd_params.dest_file_name,
            md_recvd_params.file_size,
            md_recvd_params.msgs_to_user.len()
        );
        if md_recvd_params.msgs_to_user.is_empty() {
            return;
        }
        // The proxy put request and the messages modifying it are all part of the same
        // metadata PDU, so they are collected first.
        let mut proxy_put_request = None;
        let mut trans_mode = None;
        let mut closure_requested = None;
        for msg_to_user in md_recvd_params.msgs_to_user {
            match msg_to_user.reserved_cfdp_msg_type() {
                Some(Ok(ReservedMessageType::ProxyPutRequest)) => {
                    match ProxyPutRequestMessageReader::try_from(*msg_to_user) {
                        Ok(request) => proxy_put_request = Some(request),
                        Err(e) => warn!("invalid proxy put request: {e}"),
                    }
                }
                Some(Ok(ReservedMessageType::ProxyTransmissionMode)) => {
                    match ProxyTransmissionModeMessage::try_from(*msg_to_user) {
                        Ok(msg) => trans_mode = Some(msg.transmission_mode()),
                        Err(e) => warn!("invalid proxy transmission mode: {e}"),
                    }
                }
                Some(Ok(ReservedMessageType::ProxyClosureRequest)) => {
                    match ProxyClosureRequestMessage::try_from(*msg_to_user) {
                        Ok(msg) => closure_requested = Some(msg.closure_requested()),
                        Err(e) => warn!("invalid proxy closure request: {e}"),
                    }
                }
                Some(Ok(ReservedMessageType::ProxyPutResponse)) => {
                    match ProxyPutResponseMessage::try_from(*msg_to_user) {
                        Ok(msg) => info!("Proxy put response: {:?}", msg),
                        Err(e) => warn!("invalid proxy put response: {e}"),
                    }
                }
                Some(Ok(ReservedMessageType::OriginatingTransactionId)) => {
                    match OriginatingTransactionIdMessage::try_from(*msg_to_user) {
                        Ok(msg) => info!(
                            "Originating transaction ID: {}:{}",
                            msg.source_entity_id().value(),
                            msg.transaction_seq_num().value()
                        ),
                        Err(e) => warn!("invalid originating transaction ID: {e}"),
                    }
                }
                Some(Ok(msg_type)) => warn!(
                    "unsupported reserved CFDP message {:?}: {:?}",
                    msg_type,
                    msg_to_user.value()
                ),
                Some(Err(e)) => warn!("invalid reserved CFDP message: {e}"),
                None => info!("Custom message to user: {:?}", msg_to_user.value()),
            }
        }
        // Found some proxy put request and extracted it, now send it to the source entity.
        if let Some(proxy_put_request) = proxy_put_request
            && let Err(e) = self.handle_proxy_put_request(
                &md_recvd_params.id,
                &proxy_put_request,
                trans_mode,
                closure_requested,
            )
        {
            warn!("proxy put request failed: {e}");
        }
    }

    fn file_segment_recvd_indication(&mut self, segment_recvd_params: &FileSegmentRecvdParams) {
        info!(
            "File segment received for transaction {}: offset {}, length {}",
            fmt_id(&segment_recvd_params.id),
            segment_recvd_params.offset,
            segment_recvd_params.length
        );
    }

    fn report_indication(&mut self, _id: &TransactionId) {}

    fn suspended_indication(&mut self, id: &TransactionId, condition_code: ConditionCode) {
        info!("Transaction {} suspended: {:?}", fmt_id(id), condition_code);
    }

    fn resumed_indication(&mut self, id: &TransactionId, progress: u64) {
        info!(
            "Transaction {} resumed at progress {}",
            fmt_id(id),
            progress
        );
    }

    fn fault_indication(
        &mut self,
        id: &TransactionId,
        condition_code: ConditionCode,
        progress: u64,
    ) {
        warn!(
            "Fault for transaction {}: {:?}, progress {}",
            fmt_id(id),
            condition_code,
            progress
        );
    }

    fn abandoned_indication(
        &mut self,
        id: &TransactionId,
        condition_code: ConditionCode,
        progress: u64,
    ) {
        warn!(
            "Transaction {} abandoned: {:?}, progress {}",
            fmt_id(id),
            condition_code,
            progress
        );
    }

    fn eof_recvd_indication(&mut self, id: &TransactionId) {
        info!("EOF received for transaction {}", fmt_id(id));
    }
}

#[derive(Debug, thiserror::Error)]
enum ProxyPutError {
    #[error("invalid file names")]
    InvalidFileNames,
    #[error("file path too large: {0} bytes")]
    FilePathTooLarge(usize),
    #[error("invalid originating transaction ID: {0}")]
    OriginatingId(#[from] ReservedMessageCreationError),
    #[error("source handler is not running")]
    Send,
}

#[derive(Debug, thiserror::Error)]
pub enum UdpServerError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("pdu error: {0}")]
    Pdu(#[from] PduError),
    #[error("send error")]
    Send,
}

pub struct UdpServer {
    socket: UdpSocket,
    recv_buf: Vec<u8>,
    remote_addr: SocketAddr,
    source_tc_tx: mpsc::Sender<PduOwnedWithInfo>,
    dest_tc_tx: mpsc::Sender<PduOwnedWithInfo>,
    tm_rx: mpsc::Receiver<PduOwnedWithInfo>,
}

impl UdpServer {
    /// Returns [false] if no packet was received.
    pub fn try_recv_tc(&mut self) -> Result<bool, UdpServerError> {
        let len = match self.socket.recv_from(&mut self.recv_buf) {
            Ok((len, _)) => len,
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                return Ok(false);
            }
            Err(e) => return Err(e.into()),
        };
        let pdu = PduOwnedWithInfo::new_from_raw_packet(&self.recv_buf[..len])?;
        let tx = match pdu.packet_target()? {
            cfdp::PacketTarget::SourceEntity => &self.source_tc_tx,
            cfdp::PacketTarget::DestEntity => &self.dest_tc_tx,
        };
        tx.send(pdu).map_err(|_| UdpServerError::Send)?;
        Ok(true)
    }

    /// Returns the number of packets taken from the TM queue.
    pub fn send_tm(&mut self) -> u32 {
        let mut sent_packets = 0;
        while let Ok(tm) = self.tm_rx.try_recv() {
            if let Err(e) = self.socket.send_to(tm.raw_pdu(), self.remote_addr) {
                warn!("sending TM with UDP socket failed: {e}")
            }
            sent_packets += 1;
        }
        sent_packets
    }
}

/// Spawns the source handler, destination handler and UDP server threads of one entity.
/// The threads exit once `stop` is set.
pub fn run_entity(
    local_id: UnsignedByteField,
    remote_id: UnsignedByteField,
    local_port: u16,
    remote_port: u16,
    put_req_tx: mpsc::Sender<EntityPutRequest>,
    put_req_rx: mpsc::Receiver<EntityPutRequest>,
    stop: Arc<AtomicBool>,
) -> [JoinHandle<()>; 3] {
    let remote_cfg = RemoteEntityConfig::new_with_default_values(
        remote_id,
        1024,
        true,
        false,
        TransmissionMode::Unacknowledged,
        ChecksumType::Crc32,
    );
    let (tm_tx, tm_rx) = mpsc::channel::<PduOwnedWithInfo>();
    let (source_tc_tx, source_tc_rx) = mpsc::channel::<PduOwnedWithInfo>();
    let (dest_tc_tx, dest_tc_rx) = mpsc::channel::<PduOwnedWithInfo>();

    let mut source_handler = SourceHandler::new(
        LocalEntityConfig::new(
            local_id,
            IndicationConfig::default(),
            ExampleFaultHandler::default(),
        ),
        tm_tx.clone(),
        NativeFilestore::default(),
        remote_cfg,
        StdTimerCreator::default(),
        AtomicU16::default(),
    );
    let mut source_user = ExampleCfdpUser::new(put_req_tx.clone());

    let mut dest_handler = DestinationHandler::new(
        LocalEntityConfig::new(
            local_id,
            IndicationConfig::default(),
            ExampleFaultHandler::default(),
        ),
        tm_tx,
        NativeFilestore::default(),
        remote_cfg,
        StdTimerCreator::default(),
        LostSegmentsList::default(),
    );
    let mut dest_user = ExampleCfdpUser::new(put_req_tx);

    let socket = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), local_port))
        .expect("binding UDP socket failed");
    socket
        .set_nonblocking(true)
        .expect("setting socket to non-blocking failed");
    let mut udp_server = UdpServer {
        socket,
        recv_buf: vec![0; 2048],
        remote_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), remote_port),
        source_tc_tx,
        dest_tc_tx,
        tm_rx,
    };

    let stop_src = stop.clone();
    let src_handle = thread::Builder::new()
        .name("src".to_string())
        .spawn(move || {
            while !stop_src.load(Ordering::Relaxed) {
                if source_handler.state() == State::Idle
                    && let Ok(put_request) = put_req_rx.try_recv()
                {
                    handle_put_request(&mut source_handler, &mut source_user, &put_request);
                }
                let packet = source_tc_rx.try_recv().ok();
                let sent_packets =
                    match source_handler.state_machine(&mut source_user, packet.as_ref()) {
                        Ok(sent_packets) => sent_packets,
                        Err(e) => {
                            warn!("source handler error: {e}");
                            source_handler.reset();
                            0
                        }
                    };
                if packet.is_none() && sent_packets == 0 {
                    thread::sleep(Duration::from_millis(50));
                }
            }
        })
        .unwrap();

    let stop_dest = stop.clone();
    let dest_handle = thread::Builder::new()
        .name("dest".to_string())
        .spawn(move || {
            while !stop_dest.load(Ordering::Relaxed) {
                let packet = dest_tc_rx.try_recv().ok();
                let sent_packets = match dest_handler.state_machine(&mut dest_user, packet.as_ref())
                {
                    Ok(sent_packets) => sent_packets,
                    Err(e) => {
                        warn!("destination handler error: {e}");
                        dest_handler.reset();
                        0
                    }
                };
                if packet.is_none() && sent_packets == 0 {
                    thread::sleep(Duration::from_millis(50));
                }
            }
        })
        .unwrap();

    let udp_handle = thread::Builder::new()
        .name("udp".to_string())
        .spawn(move || {
            info!("UDP server listening on port {local_port}");
            while !stop.load(Ordering::Relaxed) {
                let mut received = false;
                loop {
                    match udp_server.try_recv_tc() {
                        Ok(true) => received = true,
                        Ok(false) => break,
                        // An invalid datagram should not delay the valid ones queued behind it.
                        Err(UdpServerError::Pdu(e)) => {
                            warn!("invalid PDU received: {e}");
                            received = true;
                        }
                        Err(e) => {
                            warn!("UDP server error: {e}");
                            break;
                        }
                    }
                }
                let sent_packets = udp_server.send_tm();
                if !received && sent_packets == 0 {
                    thread::sleep(Duration::from_millis(50));
                }
            }
        })
        .unwrap();

    [src_handle, dest_handle, udp_handle]
}

/// Generic out request handler function, called in the source entity thread.
fn handle_put_request(
    source_handler: &mut SourceHandler<
        mpsc::Sender<PduOwnedWithInfo>,
        ExampleFaultHandler,
        NativeFilestore,
        RemoteEntityConfig,
        StdTimerCreator,
        cfdp::StdCountdown,
        AtomicU16,
    >,
    source_user: &mut ExampleCfdpUser,
    put_request: &EntityPutRequest,
) {
    let EntityPutRequest {
        request: put_request,
        originating_id,
    } = put_request;
    info!(
        "Handling put request to entity {}: {:?} -> {:?}, {} messages to user",
        put_request.destination_id.value(),
        put_request.source_file(),
        put_request.dest_file(),
        put_request
            .msgs_to_user
            .as_ref()
            .map_or(0, |msgs| msgs.len())
    );
    if let Err(e) = source_handler.put_request(put_request) {
        warn!("put request failed: {e}");
        if let Some(originating_id) = originating_id {
            let response = ProxyPutResponseMessage::new(
                ConditionCode::FilestoreRejection,
                DeliveryCode::Incomplete,
                FileStatus::Unreported,
            );
            source_user.send_proxy_put_response(originating_id, &response);
        }
        return;
    }
    if let (Some(originating_id), Some(id)) = (originating_id, source_handler.transaction_id()) {
        source_user.active_proxy_put_req = Some((id, *originating_id));
    }
}
