use std::{
    fmt::{self, Display},
    mem,
    sync::{Arc, Mutex},
};

use chrono::Duration;
use tokio::sync::Notify;
use tracing::{debug, info};

use crate::{
    discovery::peers::ControllerPeer,
    encoding::{self, Decode, Encode},
    sync::lock,
};

use super::{
    clock::Clock, encoding::PayloadEntryHeader, ghostxform::GhostXForm,
    measurement::MeasurePeerEvent, node::NodeId, timeline::Timeline, Result,
};

pub const SESSION_MEMBERSHIP_HEADER_KEY: u32 = u32::from_be_bytes(*b"sess");
pub const SESSION_MEMBERSHIP_SIZE: u32 = mem::size_of::<SessionId>() as u32;
pub const SESSION_MEMBERSHIP_HEADER: PayloadEntryHeader = PayloadEntryHeader {
    key: SESSION_MEMBERSHIP_HEADER_KEY,
    size: SESSION_MEMBERSHIP_SIZE,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd)]
pub struct SessionId(pub NodeId);

impl Encode for SessionId {
    fn encode_to(&self, out: &mut Vec<u8>) {
        self.0.encode_to(out);
    }
    fn encoded_size(&self) -> usize {
        self.0.encoded_size()
    }
}

impl Decode for SessionId {
    fn decode_from(bytes: &[u8]) -> std::result::Result<(Self, usize), encoding::DecodeError> {
        let (node_id, n) = NodeId::decode_from(bytes)?;
        Ok((SessionId(node_id), n))
    }
}

impl Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SessionMembership {
    pub session_id: SessionId,
}

impl Encode for SessionMembership {
    fn encode_to(&self, out: &mut Vec<u8>) {
        self.session_id.encode_to(out);
    }
    fn encoded_size(&self) -> usize {
        self.session_id.encoded_size()
    }
}

impl Decode for SessionMembership {
    fn decode_from(bytes: &[u8]) -> std::result::Result<(Self, usize), encoding::DecodeError> {
        let (session_id, n) = SessionId::decode_from(bytes)?;
        Ok((Self { session_id }, n))
    }
}

impl From<SessionId> for SessionMembership {
    fn from(session_id: SessionId) -> Self {
        SessionMembership { session_id }
    }
}

impl SessionMembership {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoded = SESSION_MEMBERSHIP_HEADER.encode()?;
        encoded.append(&mut encoding::encode_to_vec(&self.session_id)?);
        Ok(encoded)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SessionMeasurement {
    pub x_form: GhostXForm,
    pub timestamp: Duration,
}

impl Default for SessionMeasurement {
    fn default() -> Self {
        Self {
            x_form: GhostXForm::default(),
            timestamp: Duration::zero(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Session {
    pub session_id: SessionId,
    pub timeline: Timeline,
    pub measurement: SessionMeasurement,
}

#[derive(Clone)]
pub struct Sessions {
    pub other_sessions: Arc<Mutex<Vec<Session>>>,
    pub current: Arc<Mutex<Session>>,
    pub is_founding: Arc<Mutex<bool>>,
    pub tx_measure_peer_state: tokio::sync::mpsc::Sender<MeasurePeerEvent>,
    pub peers: Arc<Mutex<Vec<ControllerPeer>>>,
    pub clock: Clock,
    pub has_joined: Arc<Mutex<bool>>,
}

impl Sessions {
    pub fn new(
        init: Session,
        tx_measure_peer_state: tokio::sync::mpsc::Sender<MeasurePeerEvent>,
        peers: Arc<Mutex<Vec<ControllerPeer>>>,
        clock: Clock,
        tx_join_session: tokio::sync::mpsc::Sender<Session>,
        notifier: Arc<Notify>,
        mut rx_measure_peer_result: tokio::sync::mpsc::Receiver<MeasurePeerEvent>,
    ) -> Self {
        let other_sessions = Arc::new(Mutex::new(vec![init.clone()]));
        let current = Arc::new(Mutex::new(init));

        let other_sessions_loop = other_sessions.clone();
        let current_loop = current.clone();
        let tx_join_session_loop = tx_join_session.clone();
        let peers_loop = peers.clone();
        let tx_measure_peer_state_loop = tx_measure_peer_state.clone();

        let jh = tokio::spawn(async move {
            loop {
                if let Some(MeasurePeerEvent::XForm(session_id, x_form)) =
                    rx_measure_peer_result.recv().await
                {
                    if x_form == GhostXForm::default() {
                        handle_failed_measurement(
                            session_id,
                            other_sessions_loop.clone(),
                            current_loop.clone(),
                            peers_loop.clone(),
                            tx_measure_peer_state_loop.clone(),
                        )
                        .await;
                    } else {
                        handle_successful_measurement(
                            session_id,
                            x_form,
                            other_sessions_loop.clone(),
                            current_loop.clone(),
                            clock,
                            tx_join_session_loop.clone(),
                            peers_loop.clone(),
                            tx_measure_peer_state_loop.clone(),
                        )
                        .await;
                    }
                } else {
                    info!("measure peer event channel closed");
                }
            }
        });

        tokio::spawn(async move {
            notifier.notified().await;

            jh.abort();
        });

        Self {
            other_sessions,
            current,
            tx_measure_peer_state,
            peers,
            clock,
            is_founding: Arc::new(Mutex::new(false)),
            has_joined: Arc::new(Mutex::new(false)),
        }
    }

    pub fn reset_session(&mut self, session: Session) {
        *lock(&self.current) = session;
        lock(&self.other_sessions).clear()
    }

    pub fn reset_timeline(&self, timeline: Timeline) {
        // Read the current session id first and release that lock before taking
        // `other_sessions`. Previously this locked `other_sessions` and then took
        // `current` inside the `find` closure; with `try_lock().unwrap()` that
        // panicked (and aborted the host) whenever the two collided.
        let current_session_id = lock(&self.current).session_id;

        if let Some(session) = lock(&self.other_sessions)
            .iter_mut()
            .find(|s| s.session_id == current_session_id)
        {
            session.timeline = timeline;
        }
    }

    pub async fn saw_session_timeline(
        &self,
        session_id: SessionId,
        timeline: Timeline,
    ) -> Timeline {
        debug!(
            "saw session timeline {:?} for session {}",
            timeline, session_id,
        );

        let current_session = lock(&self.current).clone();

        if current_session.session_id == session_id {
            let session = self.update_timeline(current_session, timeline);
            lock(&self.current).timeline = session.timeline;

            let mut has_joined = lock(&self.has_joined);
            if !*has_joined {
                debug!(
                    "updating current session {} with timeline {:?}",
                    session_id, session.timeline
                );

                *has_joined = true;
            }
        } else {
            let session = Session {
                session_id,
                timeline,
                measurement: SessionMeasurement {
                    x_form: GhostXForm::default(),
                    timestamp: Duration::zero(),
                },
            };

            let s = lock(&self.other_sessions)
                .iter()
                .cloned()
                .enumerate()
                .find(|(_, s)| s.session_id == session_id);

            if let Some((idx, s)) = s {
                let session = self.update_timeline(s, timeline);
                info!(
                    "updating already seen session {} with timeline {:?}",
                    session_id, session.timeline
                );
                // The vector can shrink between the lookup above and here, so
                // update through `get_mut` rather than indexing.
                if let Some(existing) = lock(&self.other_sessions).get_mut(idx) {
                    existing.timeline = session.timeline;
                }
            } else {
                info!("adding session {} to other sessions", session_id);
                lock(&self.other_sessions).push(session.clone());

                launch_session_measurement(
                    self.peers.clone(),
                    self.tx_measure_peer_state.clone(),
                    session,
                )
                .await;
            }
        }

        lock(&self.current).timeline
    }

    pub fn update_timeline(&self, mut session: Session, timeline: Timeline) -> Session {
        if timeline.beat_origin > session.timeline.beat_origin {
            info!(
                "[adopting] updating peer timeline for session {} (bpm: {}, beat origin: {}, time: origin: {})",
                session.session_id,
                timeline.tempo.bpm().round(),
                timeline.beat_origin.floating(),
                timeline.time_origin,
            );
            session.timeline = timeline;
        } else {
            debug!(
                "[rejecting] updating peer timeline with beat origin: {}. current timeline beat origin: {}",
                timeline.beat_origin.floating(),
                session.timeline.beat_origin.floating()
            );
        }

        session
    }
}

pub async fn launch_session_measurement(
    peers: Arc<Mutex<Vec<ControllerPeer>>>,
    tx_measure_peer_state: tokio::sync::mpsc::Sender<MeasurePeerEvent>,
    mut session: Session,
) {
    info!(
        "launching session measurement for session {}",
        session.session_id
    );

    let peers = session_peers(peers.clone(), session.session_id);

    // A send failure just means the measurement task has already shut down, which
    // is normal during teardown — log it instead of taking the process down.
    let peer_state = peers
        .iter()
        .find(|p| p.peer_state.ident() == session.session_id.0)
        .or_else(|| peers.first())
        .map(|p| p.peer_state.clone());

    if let Some(peer_state) = peer_state {
        session.measurement.timestamp = Duration::zero();
        if let Err(e) = tx_measure_peer_state
            .send(MeasurePeerEvent::PeerState(session.session_id, peer_state))
            .await
        {
            debug!(
                "failed to launch measurement for session {}: {}",
                session.session_id, e
            );
        }
    }
}

pub async fn handle_successful_measurement(
    session_id: SessionId,
    x_form: GhostXForm,
    other_sessions: Arc<Mutex<Vec<Session>>>,
    current: Arc<Mutex<Session>>,
    clock: Clock,
    tx_join_session: tokio::sync::mpsc::Sender<Session>,
    peers: Arc<Mutex<Vec<ControllerPeer>>>,
    tx_measure_peer_state: tokio::sync::mpsc::Sender<MeasurePeerEvent>,
) {
    info!(
        "session {} measurement completed with result ({}, {})",
        session_id,
        x_form.slope,
        x_form.intercept.num_microseconds().unwrap_or(0),
    );

    let measurement = SessionMeasurement {
        x_form,
        timestamp: clock.micros(),
    };

    let current_session_id = lock(&current).session_id;
    debug!(
        "Current session: {}, measured session: {}",
        current_session_id, session_id
    );

    if current_session_id == session_id {
        // Take the lock once: record the measurement and clone the result out
        // before releasing it, so the value sent below is self-consistent.
        let session = {
            let mut cur = lock(&current);
            cur.measurement = measurement;
            cur.clone()
        };
        if let Err(e) = tx_join_session.send(session).await {
            debug!("Failed to send session join event: {}", e);
        }
    } else {
        let s = lock(&other_sessions)
            .iter()
            .cloned()
            .enumerate()
            .find(|(_, s)| s.session_id == session_id);

        if let Some((idx, mut s)) = s {
            const SESSION_EPS: Duration = Duration::microseconds(500000);

            let host_time = clock.micros();
            // Read both values under a single lock so they describe the same
            // snapshot of `current`.
            let (cur_ghost, current_session_id) = {
                let cur = lock(&current);
                (
                    cur.measurement.x_form.host_to_ghost(host_time),
                    cur.session_id,
                )
            };
            let new_ghost = measurement.x_form.host_to_ghost(host_time);

            s.measurement = measurement;
            // `other_sessions` may have been mutated since the lookup above, so
            // write through `get_mut` rather than indexing.
            if let Some(existing) = lock(&other_sessions).get_mut(idx) {
                *existing = s.clone();
            }

            let ghost_diff = new_ghost - cur_ghost;
            // `num_microseconds` only returns None on absurd (>292k year)
            // durations; fall back to 0 rather than panicking in a log line.
            let ghost_diff_us = ghost_diff.num_microseconds().unwrap_or(0);
            let session_eps_us = SESSION_EPS.num_microseconds().unwrap_or(500_000);
            debug!(
                "Ghost time comparison: current={} us, new={} us, diff={} us, eps={} us",
                cur_ghost.num_microseconds().unwrap_or(0),
                new_ghost.num_microseconds().unwrap_or(0),
                ghost_diff_us,
                session_eps_us
            );

            // Session switching logic: be selective about when to join other sessions
            // 1. Always join if we have significantly better timing (>500ms)
            // 2. Join if times are similar and we prefer older session IDs
            // 3. Join if we just started up and have no peers (prefer any established session)
            let current_session_has_no_peers =
                session_peers(peers.clone(), current_session_id).is_empty();
            let just_started =
                current_session_has_no_peers && measurement.timestamp < Duration::seconds(5);

            let should_switch =
                // Significant timing advantage
                ghost_diff > SESSION_EPS
                // Similar timing, prefer older session
                || (ghost_diff_us.abs() < session_eps_us
                    && session_id < current_session_id)
                // Just started, prefer any established session over isolation
                || just_started;

            if should_switch {
                info!("Session {} wins over current session (ghost_diff={} us, just_started={}, tempo={}), switching!",
                      session_id,
                      ghost_diff_us,
                      just_started,
                      s.timeline.tempo.bpm());
                // Swap the winning session into `current` and park the outgoing
                // one back in `other_sessions` at the same index. Done as a
                // single replace rather than remove+insert, which would panic if
                // the vector shrank in between.
                let c = {
                    let mut cur = lock(&current);
                    let previous = cur.clone();
                    *cur = s.clone();
                    previous
                };
                {
                    let mut others = lock(&other_sessions);
                    if let Some(slot) = others.get_mut(idx) {
                        *slot = c;
                    } else {
                        others.push(c);
                    }
                }

                if let Err(e) = tx_join_session.send(s.clone()).await {
                    debug!("Failed to send session join event: {}", e);
                }

                schedule_remeasurement(peers.clone(), tx_measure_peer_state.clone(), s).await;
            } else {
                debug!("Session {} does not win over current session (ghost_diff={} us, just_started={}), staying with current",
                       session_id,
                       ghost_diff_us,
                       just_started);
            }
        }
    }
}

pub async fn handle_failed_measurement(
    session_id: SessionId,
    other_sessions: Arc<Mutex<Vec<Session>>>,
    current: Arc<Mutex<Session>>,
    peers: Arc<Mutex<Vec<ControllerPeer>>>,
    tx_measure_peer: tokio::sync::mpsc::Sender<MeasurePeerEvent>,
) {
    info!("session {} measurement failed", session_id);

    let current_session = lock(&current).clone();

    if current_session.session_id == session_id {
        schedule_remeasurement(peers, tx_measure_peer, current_session).await;
    } else {
        // NOTE: this looks for a session whose id does *not* match the failed one
        // (`!=`), which is the pre-existing behaviour and is preserved here. Only
        // the panics are being fixed.
        let s = lock(&other_sessions)
            .iter()
            .cloned()
            .enumerate()
            .find(|(_, s)| s.session_id != session_id);

        if let Some((idx, _)) = s {
            {
                let mut others = lock(&other_sessions);
                if idx < others.len() {
                    others.remove(idx);
                }
            }

            // Drop every peer belonging to the failed session in one pass.
            // The previous code collected indices and removed them one by one,
            // which shifts the remaining indices and could remove the wrong peer
            // (or panic on an out-of-range index).
            lock(&peers).retain(|p| p.peer_state.session_id() != session_id);
        }
    }
}

pub async fn schedule_remeasurement(
    peers: Arc<Mutex<Vec<ControllerPeer>>>,
    tx_measure_peer: tokio::sync::mpsc::Sender<MeasurePeerEvent>,
    session: Session,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::microseconds(30000000).to_std().unwrap()).await;
            launch_session_measurement(peers.clone(), tx_measure_peer.clone(), session.clone())
                .await;
        }
    });
}

pub fn session_peers(
    peers: Arc<Mutex<Vec<ControllerPeer>>>,
    session_id: SessionId,
) -> Vec<ControllerPeer> {
    let mut peers = peers
        .try_lock()
        .unwrap()
        .iter()
        .filter(|p| p.peer_state.session_id() == session_id)
        .cloned()
        .collect::<Vec<_>>();
    peers.sort_by_key(|a| a.peer_state.ident());

    peers
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::link::node::NodeId;

    #[test]
    fn test_key() {
        assert_eq!(SESSION_MEMBERSHIP_HEADER_KEY, 0x73657373);
    }

    #[test]
    fn session_id_equality() {
        let id1 = SessionId(NodeId::from_array([1, 2, 3, 4, 5, 6, 7, 8]));
        let id2 = SessionId(NodeId::from_array([1, 2, 3, 4, 5, 6, 7, 8]));
        let id3 = SessionId(NodeId::from_array([8, 7, 6, 5, 4, 3, 2, 1]));
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn session_id_ordering() {
        let id_low = SessionId(NodeId::from_array([0, 0, 0, 0, 0, 0, 0, 1]));
        let id_high = SessionId(NodeId::from_array([0, 0, 0, 0, 0, 0, 0, 2]));
        assert!(id_low < id_high);
    }

    #[test]
    fn session_id_display() {
        let id = SessionId(NodeId::from_array([
            0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44,
        ]));
        let display = format!("{}", id);
        assert_eq!(display, "aabbccdd11223344");
    }

    #[test]
    fn session_membership_from_session_id() {
        let id = SessionId(NodeId::from_array([1, 2, 3, 4, 5, 6, 7, 8]));
        let membership = SessionMembership::from(id);
        assert_eq!(membership.session_id, id);
    }

    #[test]
    fn session_membership_roundtrip_encode() {
        let id = SessionId(NodeId::from_array([10, 20, 30, 40, 50, 60, 70, 80]));
        let membership = SessionMembership::from(id);
        let encoded = membership.encode().unwrap();
        // Should include the header + SessionId bytes
        assert!(!encoded.is_empty());
    }

    #[test]
    fn session_measurement_default() {
        let sm = SessionMeasurement::default();
        assert_eq!(sm.x_form, GhostXForm::default());
        assert_eq!(sm.timestamp, Duration::zero());
    }

    #[test]
    fn session_peers_empty_when_no_match() {
        let peers = Arc::new(Mutex::new(vec![]));
        let session_id = SessionId(NodeId::from_array([1, 2, 3, 4, 5, 6, 7, 8]));
        let result = session_peers(peers, session_id);
        assert!(result.is_empty());
    }
}
