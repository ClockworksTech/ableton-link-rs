use std::sync::{Arc, Mutex};

use crate::encoding::{self, Decode, Encode};

use tokio::{net::UdpSocket, sync::Notify};
use tracing::{debug, info};

use crate::{
    discovery::messages::parse_payload,
    link::{
        payload::{GhostTime, PayloadEntry},
        sessions::SessionMembership,
    },
};

use super::{
    clock::Clock, error::Error, ghostxform::GhostXForm, payload::Payload, sessions::SessionId,
    Result,
};
use crate::sync::lock;

pub const MAX_MESSAGE_SIZE: usize = 512;
pub const PROTOCOL_HEADER_SIZE: usize = 8;

pub type MessageType = u8;
pub type ProtocolHeader = [u8; PROTOCOL_HEADER_SIZE];

pub const PING: MessageType = 1;
pub const PONG: MessageType = 2;

pub const MESSAGE_TYPES: [&str; 2] = ["PING", "PONG"];

pub const PROTOCOL_HEADER: ProtocolHeader = [b'_', b'l', b'i', b'n', b'k', b'_', b'v', 1];

pub const MESSAGE_HEADER_SIZE: usize = std::mem::size_of::<MessageType>();

#[derive(Debug)]
pub struct MessageHeader {
    pub message_type: MessageType,
}

impl Encode for MessageHeader {
    fn encode_to(&self, out: &mut Vec<u8>) {
        self.message_type.encode_to(out);
    }
    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for MessageHeader {
    fn decode_from(bytes: &[u8]) -> std::result::Result<(Self, usize), encoding::DecodeError> {
        let (message_type, n) = u8::decode_from(bytes)?;
        Ok((Self { message_type }, n))
    }
}

#[derive(Debug, Clone)]
pub struct PingResponder {
    pub session_id: Arc<Mutex<SessionId>>,
    pub ghost_x_form: Arc<Mutex<GhostXForm>>,
    pub clock: Clock,
    pub unicast_socket: Option<Arc<UdpSocket>>,
}

impl PingResponder {
    pub fn new(
        unicast_socket: Arc<UdpSocket>,
        session_id: SessionId,
        ghost_x_form: GhostXForm,
        clock: Clock,
    ) -> Self {
        PingResponder {
            unicast_socket: Some(unicast_socket),
            session_id: Arc::new(Mutex::new(session_id)),
            ghost_x_form: Arc::new(Mutex::new(ghost_x_form)),
            clock,
        }
    }

    pub async fn listen(&self, _notifier: Arc<Notify>) {
        let Some(unicast_socket) = self.unicast_socket.as_ref().map(Arc::clone) else {
            debug!("ping responder has no unicast socket; not listening");
            return;
        };
        let session_id = self.session_id.clone();
        let ghost_x_form = self.ghost_x_form.clone();
        let clock = self.clock;

        match unicast_socket.local_addr() {
            Ok(addr) => info!("listening for ping messages on {}", addr),
            Err(e) => info!("listening for ping messages (local addr unavailable: {})", e),
        }

        let mut ping_message_received = false;

        tokio::spawn(async move {
            loop {
                let mut buf = [0; MAX_MESSAGE_SIZE];

                if let Ok((amt, src)) = unicast_socket.recv_from(&mut buf).await {
                    if !buf.starts_with(&PROTOCOL_HEADER) {
                        info!("protocol header mismatch");
                        continue;
                    }

                    // Anything can send to this port, so a malformed datagram must
                    // be dropped rather than aborting the process.
                    let (header, header_len) = match parse_message_header(&buf[..amt]) {
                        Ok(parsed) => parsed,
                        Err(e) => {
                            debug!("ignoring malformed ping header from {}: {}", src, e);
                            continue;
                        }
                    };
                    if header_len > amt {
                        debug!("ignoring truncated ping from {}", src);
                        continue;
                    }
                    let payload_size = buf[header_len..amt].len();
                    let max_payload_size = 40;

                    if header.message_type == PING && payload_size <= max_payload_size as usize {
                        if !ping_message_received {
                            info!("received ping message from {}", src);
                        }

                        let payload = match parse_payload(&buf[header_len..amt]) {
                            Ok(payload) => payload,
                            Err(e) => {
                                debug!("ignoring malformed ping payload from {}: {}", src, e);
                                continue;
                            }
                        };

                        let mut payload_entries = vec![];
                        for entry in payload.entries.into_iter() {
                            if matches!(
                                entry,
                                PayloadEntry::HostTime(_) | PayloadEntry::PrevGhostTime(_)
                            ) {
                                payload_entries.push(entry);
                            }
                        }

                        let id = SessionMembership {
                            session_id: *lock(&session_id),
                        };
                        let current_gt = GhostTime {
                            time: lock(&ghost_x_form).host_to_ghost(clock.micros()),
                        };

                        payload_entries.push(PayloadEntry::SessionMembership(id));
                        payload_entries.push(PayloadEntry::GhostTime(current_gt));

                        let pong_payload = Payload {
                            entries: payload_entries,
                        };

                        if !ping_message_received {
                            debug!("pong_payload {:?}", pong_payload);
                        }

                        let pong_message = match encode_message(PONG, &pong_payload) {
                            Ok(pong_message) => pong_message,
                            Err(e) => {
                                tracing::warn!("failed to encode pong message: {:?}", e);
                                continue;
                            }
                        };
                        // The peer may have gone away or be unreachable; a
                        // failed unicast pong must not panic the responder.
                        if let Err(e) = unicast_socket.send_to(&pong_message, src).await {
                            tracing::warn!("failed to send pong message to {}: {}", src, e);
                            continue;
                        }
                        if !ping_message_received {
                            debug!("sent pong message to {}", src);
                        }

                        ping_message_received = true;
                    } else {
                        debug!("received invalid message from {}", src);
                    }
                }
            }
        });
    }

    pub async fn update_node_state(&self, session_id: SessionId, x_form: GhostXForm) {
        *lock(&self.session_id) = session_id;
        *lock(&self.ghost_x_form) = x_form;
    }
}

pub fn encode_message(message_type: MessageType, payload: &Payload) -> Result<Vec<u8>> {
    let header = MessageHeader { message_type };

    let message_size = PROTOCOL_HEADER_SIZE + MESSAGE_HEADER_SIZE + payload.size() as usize;

    if message_size > MAX_MESSAGE_SIZE {
        return Err(Error::Protocol("exceeded maximum message size"));
    }

    let mut encoded = encoding::encode_to_vec(&PROTOCOL_HEADER)?;
    encoded.append(&mut encoding::encode_to_vec(&header)?);
    encoded.append(&mut payload.encode()?);

    Ok(encoded)
}

pub fn parse_message_header(data: &[u8]) -> Result<(MessageHeader, usize)> {
    let min_message_size = PROTOCOL_HEADER_SIZE + MESSAGE_HEADER_SIZE;

    if data.len() < min_message_size {
        return Err(Error::Protocol("invalid message size"));
    }

    if !data.starts_with(&PROTOCOL_HEADER) {
        return Err(Error::Protocol("invalid protocol header"));
    }

    let (header, consumed) = encoding::decode_from_slice::<MessageHeader>(
        &data[PROTOCOL_HEADER_SIZE..min_message_size],
    )?;
    Ok((header, PROTOCOL_HEADER_SIZE + consumed))
}

#[cfg(test)]
mod tests {
    use crate::link::payload::HostTime;

    use super::*;

    fn init_tracing() {
        let _ = tracing_subscriber::fmt::try_init();
    }

    #[test]
    fn roundtrip() {
        init_tracing();

        let payload = Payload {
            entries: vec![PayloadEntry::HostTime(HostTime::default())],
        };

        let message = encode_message(PING, &payload).unwrap();
        info!("message: {:?}", message);

        let header = parse_message_header(&message).unwrap();
        info!("header: {:?}", header);
    }
}
