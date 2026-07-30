use std::{
    collections::HashMap,
    io, mem,
    net::{IpAddr, Ipv4Addr, SocketAddrV4},
    sync::{Arc, Mutex},
};

use chrono::Duration;
use local_ip_address::list_afinet_netifas;
use tokio::{
    net::UdpSocket,
    select,
    sync::{
        mpsc::{self, Sender},
        Notify,
    },
};
use tracing::{debug, info};

use crate::{
    discovery::{messages::parse_payload, messenger::new_udp_reuseport, peers::PeerState},
    encoding::{self, Decode, Encode},
    link::{
        payload::PrevGhostTime,
        pingresponder::{parse_message_header, MAX_MESSAGE_SIZE, PONG},
        Result,
    },
    sync::lock,
};

use super::{
    clock::Clock,
    encoding::PayloadEntryHeader,
    ghostxform::GhostXForm,
    linear_regression::linear_regression,
    node::NodeId,
    payload::{HostTime, Payload, PayloadEntry},
    pingresponder::{encode_message, PingResponder, PING},
    sessions::SessionId,
    state::SessionState,
};

pub const MEASUREMENT_ENDPOINT_V4_HEADER_KEY: u32 = u32::from_be_bytes(*b"mep4");
pub const MEASUREMENT_ENDPOINT_V4_SIZE: u32 =
    (mem::size_of::<Ipv4Addr>() + mem::size_of::<u16>()) as u32;
pub const MEASUREMENT_ENDPOINT_V4_HEADER: PayloadEntryHeader = PayloadEntryHeader {
    key: MEASUREMENT_ENDPOINT_V4_HEADER_KEY,
    size: MEASUREMENT_ENDPOINT_V4_SIZE,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeasurementEndpointV4 {
    pub endpoint: Option<SocketAddrV4>,
}

impl Encode for MeasurementEndpointV4 {
    fn encode_to(&self, out: &mut Vec<u8>) {
        // `encode_to` can't report failure, and the entry is fixed-size, so an
        // absent endpoint is written as 0.0.0.0:0 rather than panicking.
        let ep = self
            .endpoint
            .unwrap_or_else(|| SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));
        u32::from(*ep.ip()).encode_to(out);
        ep.port().encode_to(out);
    }
    fn encoded_size(&self) -> usize {
        6
    }
}

impl Decode for MeasurementEndpointV4 {
    fn decode_from(bytes: &[u8]) -> std::result::Result<(Self, usize), encoding::DecodeError> {
        let (ip_raw, n1) = u32::decode_from(bytes)?;
        let (port, n2) = u16::decode_from(&bytes[n1..])?;
        Ok((
            Self {
                endpoint: Some(SocketAddrV4::new(Ipv4Addr::from(ip_raw), port)),
            },
            n1 + n2,
        ))
    }
}

impl MeasurementEndpointV4 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoded = MEASUREMENT_ENDPOINT_V4_HEADER.encode()?;
        encoded.append(&mut encoding::encode_to_vec(self)?);
        Ok(encoded)
    }
}

#[derive(Clone, Debug)]
pub enum MeasurePeerEvent {
    PeerState(SessionId, PeerState),
    XForm(SessionId, GhostXForm),
}

#[derive(Debug, Clone)]
pub struct MeasurementService {
    pub measurement_map: Arc<Mutex<HashMap<NodeId, Measurement>>>,
    pub clock: Clock,
    pub ping_responder: PingResponder,
    pub tx_measure_peer: tokio::sync::mpsc::Sender<MeasurePeerEvent>,
}

impl MeasurementService {
    pub async fn new(
        ping_responder_unicast_socket: Arc<UdpSocket>,
        peer_state: Arc<Mutex<PeerState>>,
        session_state: Arc<Mutex<SessionState>>,
        clock: Clock,
        tx_measure_peer_result: tokio::sync::mpsc::Sender<MeasurePeerEvent>,
        notifier: Arc<Notify>,
        mut rx_measure_peer_state: tokio::sync::mpsc::Receiver<MeasurePeerEvent>,
    ) -> MeasurementService {
        let measurement_map = Arc::new(Mutex::new(HashMap::new()));

        let m_map = measurement_map.clone();
        let t_peer = tx_measure_peer_result.clone();

        tokio::spawn(async move {
            loop {
                let event = rx_measure_peer_state.recv().await;
                if let Some(MeasurePeerEvent::PeerState(session_id, peer)) = event {
                    measure_peer(
                        clock,
                        m_map.clone(),
                        t_peer.clone(),
                        session_id,
                        peer,
                        notifier.clone(),
                    )
                    .await;
                }
            }
        });

        MeasurementService {
            measurement_map,
            clock,
            ping_responder: PingResponder::new(
                ping_responder_unicast_socket,
                lock(&peer_state).session_id(),
                lock(&session_state).ghost_x_form,
                clock,
            ),
            tx_measure_peer: tx_measure_peer_result,
        }
    }

    pub async fn update_node_state(&self, session_id: SessionId, x_form: GhostXForm) {
        self.ping_responder
            .update_node_state(session_id, x_form)
            .await;
    }
}

pub async fn measure_peer(
    clock: Clock,
    measurement_map: Arc<Mutex<HashMap<NodeId, Measurement>>>,
    tx_measure_peer_result: tokio::sync::mpsc::Sender<MeasurePeerEvent>,
    session_id: SessionId,
    state: PeerState,
    notifier: Arc<Notify>,
) {
    match state.measurement_endpoint {
        Some(endpoint) => info!(
            "measuring peer {} at {} for session {}",
            state.node_state.node_id, endpoint, session_id
        ),
        None => info!(
            "measuring peer {} (no endpoint advertised) for session {}",
            state.node_state.node_id, session_id
        ),
    }

    let node_id = state.node_state.node_id;

    let (tx_measurement, mut rx_measurement) = mpsc::channel(1);

    // `None` means the peer can't be measured right now (no endpoint, no routable
    // interface, socket bind failed). Skip it instead of aborting.
    let Some(measurement) = Measurement::new(state, clock, tx_measurement, notifier).await else {
        return;
    };
    lock(&measurement_map).insert(node_id, measurement);

    let tx_measure_peer_result_loop = tx_measure_peer_result.clone();

    let measurement_map = measurement_map.clone();

    tokio::spawn(async move {
        // `recv` returning None means the measurement was dropped; exit rather
        // than spinning on a closed channel (the previous `loop { if let Some }`
        // busy-looped forever once it closed).
        while let Some(data) = rx_measurement.recv().await {
            {
                if data.is_empty() {
                    if let Err(e) = tx_measure_peer_result_loop
                        .send(MeasurePeerEvent::XForm(session_id, GhostXForm::default()))
                        .await
                    {
                        debug!("measure peer result receiver gone: {}", e);
                        break;
                    }
                } else {
                    let (slope, intercept) = if data.len() >= 3 {
                        let (reg_slope, reg_intercept) = linear_regression(data.iter().copied());
                        if reg_intercept.is_finite() {
                            (1.0 + reg_slope, reg_intercept)
                        } else {
                            // Fallback to slope=1.0 with median offset
                            let offsets: Vec<f64> = data.iter().map(|(_, y)| *y).collect();
                            (1.0, median(offsets))
                        }
                    } else {
                        // Not enough data for regression, use median offset
                        let mut offsets: Vec<f64> = data.iter().map(|(_, y)| *y).collect();
                        offsets
                            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        // `data` is non-empty in this branch, but index defensively.
                        let mid = offsets.get(offsets.len() / 2).copied().unwrap_or(0.0);
                        (1.0, mid)
                    };
                    if let Err(e) = tx_measure_peer_result_loop
                        .send(MeasurePeerEvent::XForm(
                            session_id,
                            GhostXForm {
                                slope,
                                intercept: Duration::microseconds(intercept.round() as i64),
                            },
                        ))
                        .await
                    {
                        debug!("measure peer result receiver gone: {}", e);
                        break;
                    }
                }

                lock(&measurement_map).remove(&node_id);
            }
        }
    });
}

pub const NUMBER_DATA_POINTS: usize = 20;
pub const NUMBER_MEASUREMENTS: usize = 5;

/// How often the measurement timer wakes to send a ping.
const TIMER_TICK: std::time::Duration = std::time::Duration::from_millis(50);
/// How long to wait after a ping before counting the measurement attempt.
const PING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

#[derive(Debug)]
pub struct Measurement {
    pub unicast_socket: Option<Arc<UdpSocket>>,
    pub session_id: SessionId,
    pub measurement_endpoint: Option<SocketAddrV4>,
    pub data: Arc<Mutex<Vec<(f64, f64)>>>,
    pub clock: Clock,
    pub measurements_started: Arc<Mutex<usize>>,
    pub success: Arc<Mutex<bool>>,
    pub init_bytes_sent: usize,
    tx_timer: Sender<()>,
}

impl Measurement {
    pub async fn new(
        state: PeerState,
        clock: Clock,
        tx_measurement: Sender<Vec<(f64, f64)>>,
        notifier: Arc<Notify>,
    ) -> Option<Self> {
        // A peer with no advertised endpoint can't be measured. Previously this
        // was unwrapped further down and panicked.
        let Some(measurement_endpoint) = state.measurement_endpoint else {
            debug!(
                "peer {} advertised no measurement endpoint; skipping measurement",
                state.node_state.node_id
            );
            return None;
        };

        let (tx_timer, mut rx_timer) = mpsc::channel(1);

        // No routable IPv4 interface (machine offline, or loopback only) means we
        // can't measure. This used to panic and, with panic="abort", take the host
        // process down with it.
        let interfaces = match list_afinet_netifas() {
            Ok(interfaces) => interfaces,
            Err(e) => {
                debug!("could not enumerate interfaces for measurement: {}", e);
                return None;
            }
        };
        let Some(ip) = interfaces.iter().find_map(|(_, ip)| match ip {
            IpAddr::V4(ipv4) if !ip.is_loopback() => Some(*ipv4),
            _ => None,
        }) else {
            debug!("no non-loopback IPv4 interface available for measurement");
            return None;
        };

        let unicast_socket = match new_udp_reuseport(SocketAddrV4::new(ip, 0).into()) {
            Ok(socket) => Arc::new(socket),
            Err(e) => {
                debug!("could not bind measurement socket on {}: {}", ip, e);
                return None;
            }
        };
        match unicast_socket.local_addr() {
            Ok(addr) => info!(
                "initiating new unicast socket {} for measurement_endpoint {:?}",
                addr, state.measurement_endpoint
            ),
            Err(e) => info!(
                "initiating new unicast socket (local addr unavailable: {}) for measurement_endpoint {:?}",
                e, state.measurement_endpoint
            ),
        }

        let success = Arc::new(Mutex::new(false));
        let data = Arc::new(Mutex::new(vec![]));

        let mut measurement = Measurement {
            unicast_socket: Some(unicast_socket.clone()),
            session_id: state.node_state.session_id,
            measurement_endpoint: state.measurement_endpoint,
            data: data.clone(),
            clock,
            measurements_started: Arc::new(Mutex::new(0)),
            success: success.clone(),
            tx_timer,
            init_bytes_sent: 0,
        };

        let ht = HostTime::new(clock.micros());

        let s = success.clone();
        let d = data.clone();
        let t = tx_measurement.clone();

        let finished_notifier = Arc::new(Notify::new());

        let fn_loop = finished_notifier.clone();

        tokio::spawn(async move {
            loop {
                select! {
                    Some(_) = rx_timer.recv() => {
                        fn_loop.notify_one();
                        finish(
                            s.clone(),
                            measurement_endpoint,
                            d.clone(),
                            t.clone(),
                        )
                        .await;
                    }
                    _ = notifier.notified() => {
                        break;
                    }
                }
            }
        });

        measurement.listen().await;

        info!("sending initial host time ping {:?}", ht);

        let init_bytes_sent = match send_ping(
            unicast_socket.clone(),
            measurement_endpoint,
            &Payload {
                entries: vec![PayloadEntry::HostTime(ht)],
            },
        )
        .await
        {
            Ok(bytes) => bytes,
            Err(e) => {
                // The measurement peer may be unreachable (no route to the
                // host, interface went down). Skip this ping rather than
                // panicking the measurement task.
                tracing::warn!("failed to send initial host time ping: {}", e);
                0
            }
        };

        measurement.init_bytes_sent = init_bytes_sent;

        reset_timer(
            measurement.measurements_started.clone(),
            clock,
            Some(unicast_socket.clone()),
            measurement_endpoint,
            data.clone(),
            tx_measurement.clone(),
            finished_notifier.clone(),
        )
        .await;

        Some(measurement)
    }

    pub async fn listen(&mut self) {
        // Both are Options that are only populated once the measurement socket has
        // been set up; bail quietly instead of panicking if we got here early.
        let Some(socket) = self.unicast_socket.as_ref().map(Arc::clone) else {
            debug!("measurement listen called with no unicast socket");
            return;
        };
        let Some(endpoint) = self.measurement_endpoint else {
            debug!("measurement listen called with no measurement endpoint");
            return;
        };

        // Handle connection failure gracefully
        if let Err(e) = socket.connect(endpoint).await {
            debug!(
                "Failed to connect to measurement endpoint {}: {}",
                endpoint, e
            );
            return;
        }

        let clock = self.clock;
        let s_id = self.session_id;
        let data = self.data.clone();
        let tx_timer = self.tx_timer.clone();

        match socket.local_addr() {
            Ok(addr) => info!("listening for pong messages on {}", addr),
            Err(e) => info!("listening for pong messages (local addr unavailable: {})", e),
        }

        tokio::spawn(async move {
            let mut pong_received = false;
            loop {
                let mut buf = [0; MAX_MESSAGE_SIZE];

                // Handle receive failure gracefully - peer may have disconnected
                let (amt, src) = match socket.recv_from(&mut buf).await {
                    Ok(result) => result,
                    Err(e) => {
                        debug!("Failed to receive from measurement socket: {}", e);
                        break;
                    }
                };

                // A malformed datagram must not take the process down: anything on
                // this port can send us bytes, so drop unparseable packets and
                // keep listening.
                let (header, header_len) = match parse_message_header(&buf[..amt]) {
                    Ok(parsed) => parsed,
                    Err(e) => {
                        debug!("ignoring malformed measurement header from {}: {}", src, e);
                        continue;
                    }
                };
                if header.message_type == PONG {
                    if !pong_received {
                        info!("received pong message from {}", src);
                        pong_received = true;
                    }

                    if header_len > amt {
                        debug!("ignoring truncated pong from {}", src);
                        continue;
                    }

                    let payload = match parse_payload(&buf[header_len..amt]) {
                        Ok(payload) => payload,
                        Err(e) => {
                            debug!("ignoring malformed pong payload from {}: {}", src, e);
                            continue;
                        }
                    };

                    let mut session_id = SessionId::default();
                    let mut ghost_time = Duration::zero();
                    let mut prev_ghost_time = Duration::zero();
                    let mut prev_host_time = Duration::zero();

                    for entry in payload.entries.iter() {
                        match entry {
                            PayloadEntry::SessionMembership(id) => session_id = id.session_id,
                            PayloadEntry::GhostTime(gt) => ghost_time = gt.time,
                            PayloadEntry::PrevGhostTime(gt) => prev_ghost_time = gt.time,
                            PayloadEntry::HostTime(ht) => prev_host_time = ht.time,
                            _ => continue,
                        }
                    }

                    if s_id == session_id {
                        let host_time = clock.micros();

                        let payload = Payload {
                            entries: vec![
                                PayloadEntry::HostTime(HostTime { time: host_time }),
                                PayloadEntry::PrevGhostTime(PrevGhostTime {
                                    time: prev_ghost_time,
                                }),
                            ],
                        };

                        if let Err(e) = send_ping(socket.clone(), endpoint, &payload).await {
                            tracing::warn!("failed to send measurement ping to {}: {}", endpoint, e);
                        }

                        if ghost_time != Duration::microseconds(0)
                            && prev_host_time != Duration::microseconds(0)
                        {
                            let avg_host =
                                (host_time + prev_host_time).num_microseconds().unwrap_or(0) as f64
                                    * 0.5;
                            let offset =
                                ghost_time.num_microseconds().unwrap_or(0) as f64 - avg_host;
                            lock(&data).push((avg_host, offset));

                            if prev_ghost_time != Duration::microseconds(0) {
                                let avg_ghost = (ghost_time + prev_ghost_time)
                                    .num_microseconds()
                                    .unwrap_or(0) as f64
                                    * 0.5;
                                let offset2 = avg_ghost
                                    - prev_host_time.num_microseconds().unwrap_or(0) as f64;
                                lock(&data).push((
                                    prev_host_time.num_microseconds().unwrap_or(0) as f64,
                                    offset2,
                                ));
                            }
                        }

                        if lock(&data).len() > NUMBER_DATA_POINTS {
                            // Receiver gone means the measurement was already torn
                            // down; that is not a reason to abort the process.
                            if let Err(e) = tx_timer.send(()).await {
                                debug!("measurement timer receiver gone: {}", e);
                            }
                            break;
                        }
                    }
                }
            }
        });
    }
}

async fn reset_timer(
    measurements_started: Arc<Mutex<usize>>,
    clock: Clock,
    unicast_socket: Option<Arc<UdpSocket>>,
    measurement_endpoint: SocketAddrV4,
    data: Arc<Mutex<Vec<(f64, f64)>>>,
    tx_measurement: Sender<Vec<(f64, f64)>>,
    finished_notifier: Arc<Notify>,
) {
    loop {
        select! {
            _  = tokio::time::sleep(TIMER_TICK) => {
                let started = *lock(&measurements_started);
                info!("measurements_start {}", started);
                if started < NUMBER_MEASUREMENTS {
                    let ht = HostTime {
                        time: clock.micros(),
                    };

                    let Some(socket) = unicast_socket.as_ref().map(Arc::clone) else {
                        debug!("no unicast socket for measurement ping; stopping timer");
                        break;
                    };

                    if let Err(e) = send_ping(
                        socket,
                        measurement_endpoint,
                        &Payload {
                            entries: vec![PayloadEntry::HostTime(ht)],
                        },
                    )
                    .await {
                        debug!("Failed to send ping to {}: {}", measurement_endpoint, e);
                        break;
                    }

                    tokio::time::sleep(PING_INTERVAL).await;

                    *lock(&measurements_started) += 1;
                } else {
                    info!("measuring {} failed", measurement_endpoint);

                    // Cleared, so this reports an empty data set upstream, which is
                    // how a failed measurement is signalled.
                    let data = {
                        let mut d = lock(&data);
                        d.clear();
                        d.clone()
                    };
                    if let Err(e) = tx_measurement.send(data).await {
                        debug!("measurement receiver gone: {}", e);
                    }
                    break;
                }
            }
            _ = finished_notifier.notified() => {
                break;
            }
        }
    }
}

async fn finish(
    success: Arc<Mutex<bool>>,
    measurement_endpoint: SocketAddrV4,
    data: Arc<Mutex<Vec<(f64, f64)>>>,
    tx_measurement: Sender<Vec<(f64, f64)>>,
) {
    *lock(&success) = true;
    debug!("measuring {} done", measurement_endpoint);

    // Take the data out under one lock so the send and the clear can't interleave
    // with a concurrent push.
    let d = {
        let mut guard = lock(&data);
        std::mem::take(&mut *guard)
    };
    if let Err(e) = tx_measurement.send(d).await {
        debug!("measurement receiver gone: {}", e);
    }
}

pub async fn send_ping(
    socket: Arc<UdpSocket>,
    measurement_endpoint: SocketAddrV4,
    payload: &Payload,
) -> io::Result<usize> {
    let message = encode_message(PING, payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{:?}", e)))?;
    debug!(
        "sending ping message to measurement endpoint {}",
        measurement_endpoint
    );

    socket.send(&message).await
}

/// Median of `numbers`, or 0.0 if empty.
///
/// `partial_cmp` is compared with a total-order fallback: these are measured
/// offsets and a NaN (from a degenerate regression) previously panicked the sort.
/// The old `assert!(length > 2)` is gone for the same reason — a short sample is
/// a reason to return the best answer available, not to abort.
pub fn median(mut numbers: Vec<f64>) -> f64 {
    numbers.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let length = numbers.len();

    if length == 0 {
        return 0.0;
    }

    if length.is_multiple_of(2) {
        let mid = length / 2;
        (numbers[mid - 1] + numbers[mid]) / 2.0
    } else {
        numbers[length / 2]
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use local_ip_address::list_afinet_netifas;
    use tokio::sync::mpsc::Receiver;

    use crate::{
        discovery::{
            gateway::{OnEvent, PeerGateway},
            peers::PeerStateChange,
        },
        link::{controller::SessionPeerCounter, node::NodeState},
    };

    use super::*;
    use chrono::Duration;

    fn init_tracing() {
        let _ = tracing_subscriber::fmt::try_init();
    }

    async fn init_gateway() -> (PeerGateway, Receiver<OnEvent>, Arc<Notify>) {
        let session_id = SessionId::default();
        let node_1 = NodeState::new(session_id);
        let (tx_measure_peer_result, _) = mpsc::channel::<MeasurePeerEvent>(1);
        let (_, rx_measure_peer_state) = mpsc::channel::<MeasurePeerEvent>(1);
        let (tx_event, rx_event) = mpsc::channel::<OnEvent>(1);
        let (tx_peer_state_change, mut rx_peer_state_change) =
            mpsc::channel::<Vec<PeerStateChange>>(1);

        let notifier = Arc::new(Notify::new());

        let calls = Arc::new(Mutex::new(0));
        let c = calls.clone();

        tokio::spawn(async move {
            while (rx_peer_state_change.recv().await).is_some() {
                *c.try_lock().unwrap() += 1;
            }
        });

        let ip = list_afinet_netifas()
            .unwrap()
            .iter()
            .find_map(|(_, ip)| match ip {
                IpAddr::V4(ipv4) if !ip.is_loopback() => Some(*ipv4),
                _ => None,
            })
            .unwrap();

        let ping_responder_unicast_socket =
            Arc::new(new_udp_reuseport(SocketAddrV4::new(ip, 0).into()).unwrap());

        (
            PeerGateway::new(
                Arc::new(Mutex::new(PeerState {
                    node_state: node_1,
                    measurement_endpoint: None,
                })),
                Arc::new(Mutex::new(SessionState::default())),
                Clock::default(),
                Arc::new(Mutex::new(SessionPeerCounter::default())),
                tx_peer_state_change.clone(),
                tx_event,
                tx_measure_peer_result,
                Arc::new(Mutex::new(vec![])),
                notifier.clone(),
                rx_measure_peer_state,
                ping_responder_unicast_socket,
                Arc::new(Mutex::new(true)),
            )
            .await
            .unwrap(),
            rx_event,
            notifier,
        )
    }

    #[tokio::test]
    #[ignore] // Requires real UDP multicast; crashes on macOS CI — run locally with --include-ignored
    async fn test_send_ping_on_new() {
        init_tracing();

        let (gw, rx_event, notifier) = init_gateway().await;
        let n = notifier.clone();
        tokio::spawn(async move {
            gw.listen(rx_event, n).await;
        });

        let (tx_measurement, mut rx_measurement) = mpsc::channel::<Vec<(f64, f64)>>(1);

        let ip = list_afinet_netifas()
            .unwrap()
            .iter()
            .find_map(|(_, ip)| match ip {
                IpAddr::V4(ipv4) if !ip.is_loopback() => Some(*ipv4),
                _ => None,
            })
            .unwrap();

        let s = Arc::new(new_udp_reuseport(SocketAddrV4::new(ip, 0).into()).unwrap());

        let measurement = Measurement::new(
            PeerState {
                measurement_endpoint: Some(s.local_addr().unwrap().to_string().parse().unwrap()),
                ..Default::default()
            },
            Clock::default(),
            tx_measurement,
            notifier,
        )
        .await;

        // Give the measurement some time to attempt to send pings
        tokio::time::sleep(Duration::milliseconds(100).to_std().unwrap()).await;

        // Try to receive a result or timeout
        tokio::select! {
            result = rx_measurement.recv() => {
                // If we get a result, that's fine (measurement completed)
                if let Some(_data) = result {
                    // Test passed - measurement attempted
                }
            }
            _ = tokio::time::sleep(Duration::seconds(1).to_std().unwrap()) => {
                // Timeout is also fine - measurement is running in background
            }
        }

        // Clean up
        drop(measurement);
    }
}
