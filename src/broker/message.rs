#[cfg(any(feature = "fuzzy", test))]
use arbitrary::{Arbitrary, Error as ArbitraryError, Unstructured};
use log::{error, warn};

use std::sync::{mpsc, Arc};
use std::{collections::BTreeMap, fmt, net, result};

#[allow(unused_imports)]
use crate::broker::Shard;

use crate::broker::SessionArgsReplica;
use crate::broker::{Config, InpSeqno, OutSeqno};
use crate::{ClientID, PacketID, Protocol, QPacket, QoS, QueueStatus};

/// Context for reading packets from a session and converting them to [Message].
#[derive(Default)]
pub struct RouteIO {
    pub disconnected: bool,
    // Message::ClientAck carrying PingResp, PubAck-QoS-1
    // Message::{Retain, Subscribe, UnSubscribe, ShardIndex, Routed}
    pub oug_msgs: Vec<Message>,
    pub cons_io: ConsensIO,
}

impl RouteIO {
    pub fn reset_session(mut self) -> Self {
        self.disconnected = false;
        self.oug_msgs.truncate(0);
        self
    }
}

/// Context for Consensus-loop.
#[derive(Default)]
pub struct ConsensIO {
    // Message::Subscribe
    pub oug_subs: BTreeMap<ClientID, Vec<Message>>,
    // Message::UnSubscribe
    pub oug_unsubs: BTreeMap<ClientID, Vec<Message>>,

    pub oug_seqno: BTreeMap<ClientID, OutSeqno>,
    // Message::Routed, won't go through consensus loop.
    pub oug_qos0: BTreeMap<ClientID, Vec<Message>>,
    // Message::{Routed, Retain}
    pub oug_qos12: BTreeMap<ClientID, Vec<Message>>,
    // list of packet_ids for qos12 msgs that has received ack from subscribed clients.
    pub ack_qos12: Vec<PacketID>,
    // Message::{AddSession, RemSession}
    pub ctrl_msgs: Vec<Message>,
}

/// Type implement the tx-handle for a message-queue.
#[derive(Clone)]
pub struct MsgTx {
    shard_id: u32,                 // message queue for shard
    tx: mpsc::SyncSender<Message>, // shard's incoming message queue
    waker: Arc<mio::Waker>,        // receiving shard's waker
    count: usize,
}

impl Drop for MsgTx {
    fn drop(&mut self) {
        if self.count > 0 {
            if let Err(err) = self.waker.wake() {
                error!("shard-{} waking the receiving shard err:{}", self.shard_id, err)
            }
        }
    }
}

impl MsgTx {
    pub fn try_sends(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
        let mut iter = msgs.into_iter();

        loop {
            match iter.next() {
                Some(msg) => match self.tx.try_send(msg) {
                    Ok(()) => self.count += 1,
                    Err(mpsc::TrySendError::Full(msg)) => {
                        let mut msgs: Vec<Message> = Vec::from_iter(iter);
                        msgs.insert(0, msg);
                        break QueueStatus::Block(msgs);
                    }
                    Err(mpsc::TrySendError::Disconnected(msg)) => {
                        warn!("shard-{} shard disconnected ...", self.shard_id);
                        let mut msgs: Vec<Message> = Vec::from_iter(iter);
                        msgs.insert(0, msg);
                        break QueueStatus::Disconnected(msgs);
                    }
                },
                None => break QueueStatus::Ok(Vec::new()),
            }
        }
    }

    pub fn count(&self) -> usize {
        self.count
    }
}

/// Type implement the rx-handle for a message-queue.
pub struct MsgRx {
    shard_id: u32, // message queue for shard.
    msg_batch_size: usize,
    rx: mpsc::Receiver<Message>,
}

impl MsgRx {
    pub fn try_recvs(&self) -> QueueStatus<Message> {
        let mut msgs = Vec::new(); // TODO: with_capacity ?
        loop {
            match self.rx.try_recv() {
                Ok(msg) if msgs.len() < self.msg_batch_size => msgs.push(msg),
                Ok(msg) => {
                    msgs.push(msg);
                    break QueueStatus::Ok(msgs);
                }
                Err(mpsc::TryRecvError::Empty) => break QueueStatus::Block(msgs),
                Err(mpsc::TryRecvError::Disconnected) => {
                    warn!("shard-{} shard disconnected ...", self.shard_id);
                    break QueueStatus::Disconnected(msgs);
                }
            }
        }
    }
}

/// Message is a unit of communication between shards hosted on the same node.
#[derive(Clone, Eq, PartialEq)]
pub enum Message {
    // shard boundary
    /// CONNACK  - happens during add_session.
    /// PINGRESP - happens for every PINGREQ is handled by this session.
    /// PUBACK   - happens for every PINGREQ is handled by this session.
    /// SUBACK   - happens after SUBSCRIBE is commited to [Shard].
    /// UNSUBACK - happens after UNSUBSCRIBE is commited to [Shard].
    ClientAck {
        packet: QPacket,
    },
    /// Retain publish messages.
    Retain {
        out_seqno: OutSeqno,
        publish: QPacket,
    },
    /// Consensus Loop, carrying SUBSCRIBE packet.
    Subscribe {
        sub: QPacket,
    },
    /// Consensus Loop, carrying UNSUBSCRIBE packet.
    UnSubscribe {
        unsub: QPacket,
    },
    /// Incoming PUBLISH packets, QoS > 0 are indexed in the shard instance.
    ShardIndex {
        src_client_id: ClientID,
        inp_seqno: InpSeqno,
        packet_id: PacketID,
        qos: QoS,
    },

    // round-trip
    /// Incoming PUBLISH Packets received from clients and routed to other local sessions.
    Routed {
        src_shard_id: u32,   // sending shard-id
        dst_shard_id: u32,   // receiving shard-id
        client_id: ClientID, // receiving client-id
        inp_seqno: InpSeqno, // shard's inp_seqno
        out_seqno: OutSeqno, // shall be set on the receiving side.
        publish: QPacket,    // publish packet, as received from publishing client
    },
    /// Message that is periodically published by a session to other local shards.
    LocalAck {
        shard_id: u32,        // shard sending the acknowledgement
        last_acked: InpSeqno, // from publishing-shard.
    },

    // session boundary
    /// PUBLISH Packets converted from Message::Routed and/or Message::Retain, before
    /// sending them downstream.
    Oug {
        out_seqno: OutSeqno,
        publish: QPacket,
    },

    // Consensus
    AddSession {
        shard_id: u32,
        client_id: ClientID,
        raddr: net::SocketAddr,
        config: Config,
        proto: Protocol,
        clean_start: bool,
    },
    RemSession {
        shard_id: u32,
        client_id: ClientID,
    },
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Message::ClientAck { .. } => write!(f, "Message::ClientAck"),
            Message::Retain { .. } => write!(f, "Message::Retain"),
            Message::Subscribe { .. } => write!(f, "Message::Subscribe"),
            Message::UnSubscribe { .. } => write!(f, "Message::UnSubscribe"),
            Message::ShardIndex { .. } => write!(f, "Message::ShardIndex"),

            Message::Routed { .. } => write!(f, "Message::Routed"),
            Message::LocalAck { .. } => write!(f, "Message::LocalAck"),

            Message::Oug { .. } => write!(f, "Message::Oug"),

            Message::AddSession { .. } => write!(f, "Message::AddSession"),
            Message::RemSession { .. } => write!(f, "Message::RemSession"),
        }
    }
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for Message {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        use crate::v5;
        use std::str::FromStr;

        let val = match uns.arbitrary::<u8>()? % 10 {
            0 => Message::ClientAck {
                packet: {
                    let packet = match uns.arbitrary::<u8>()? % 8 {
                        0 => v5::Packet::ConnAck(uns.arbitrary()?),
                        1 => v5::Packet::PubAck(uns.arbitrary()?),
                        2 => v5::Packet::PubRec(uns.arbitrary()?),
                        3 => v5::Packet::PubRel(uns.arbitrary()?),
                        4 => v5::Packet::PubComp(uns.arbitrary()?),
                        5 => v5::Packet::SubAck(uns.arbitrary()?),
                        6 => v5::Packet::UnsubAck(uns.arbitrary()?),
                        7 => v5::Packet::PingResp,
                        _ => unreachable!(),
                    };
                    QPacket::V5(packet)
                },
            },
            1 => Message::Retain {
                out_seqno: uns.arbitrary()?,
                publish: QPacket::V5(v5::Packet::Publish(uns.arbitrary()?)),
            },
            2 => Message::Subscribe {
                sub: QPacket::V5(v5::Packet::Subscribe(uns.arbitrary()?)),
            },
            3 => Message::UnSubscribe {
                unsub: QPacket::V5(v5::Packet::UnSubscribe(uns.arbitrary()?)),
            },
            4 => Message::ShardIndex {
                src_client_id: uns.arbitrary()?,
                inp_seqno: uns.arbitrary()?,
                packet_id: uns.arbitrary()?,
                qos: uns.arbitrary()?,
            },

            5 => Message::Routed {
                src_shard_id: uns.arbitrary()?,
                dst_shard_id: uns.arbitrary()?,
                client_id: uns.arbitrary()?,
                inp_seqno: uns.arbitrary()?,
                out_seqno: uns.arbitrary()?,
                publish: QPacket::V5(v5::Packet::Publish(uns.arbitrary()?)),
            },
            6 => Message::LocalAck {
                shard_id: uns.arbitrary()?,
                last_acked: uns.arbitrary()?,
            },

            7 => Message::Oug {
                out_seqno: uns.arbitrary()?,
                publish: QPacket::V5(v5::Packet::Publish(uns.arbitrary()?)),
            },

            8 => Message::AddSession {
                shard_id: uns.arbitrary()?,
                client_id: uns.arbitrary()?,
                raddr: net::SocketAddr::from_str("192.168.2.10:1883").unwrap(),
                config: Config::default(), // TODO: make config arbitrary
                proto: Protocol::default(),
                clean_start: uns.arbitrary()?,
            },
            9 => Message::RemSession {
                shard_id: uns.arbitrary()?,
                client_id: uns.arbitrary()?,
            },
            _ => unreachable!(),
        };

        Ok(val)
    }
}

impl Message {
    pub fn new_conn_ack(connack: QPacket) -> Message {
        Message::ClientAck { packet: connack }
    }

    pub fn new_ping_resp(ping: QPacket) -> Message {
        Message::ClientAck { packet: ping }
    }

    pub fn new_pub_ack(puback: QPacket) -> Message {
        Message::ClientAck { packet: puback }
    }

    pub fn new_sub_ack(suback: QPacket) -> Message {
        Message::ClientAck { packet: suback }
    }

    pub fn new_unsub_ack(unsuback: QPacket) -> Message {
        Message::ClientAck { packet: unsuback }
    }

    pub fn new_retain_publish(publish: QPacket) -> Message {
        Message::Retain { out_seqno: 0, publish }
    }

    pub fn new_sub(sub: QPacket) -> Message {
        Message::Subscribe { sub }
    }

    pub fn new_unsub(unsub: QPacket) -> Message {
        Message::UnSubscribe { unsub }
    }

    pub fn new_index(id: &ClientID, s: InpSeqno, publish: &QPacket) -> Message {
        let packet_id = publish.to_packet_id().unwrap();
        Message::ShardIndex {
            src_client_id: id.clone(),
            inp_seqno: s,
            packet_id,
            qos: publish.to_qos(),
        }
    }

    pub fn new_routed() -> Message {
        todo!() // directly constructed
    }

    pub fn new_local_ack(shard_id: u32, last_acked: InpSeqno) -> Message {
        Message::LocalAck { shard_id, last_acked }
    }

    pub fn new_add_session() -> Message {
        todo!() // directly constructed
    }

    pub fn new_rem_session(shard_id: u32, client_id: ClientID) -> Message {
        Message::RemSession { shard_id, client_id }
    }
}

impl Message {
    pub fn set_clean_start(&mut self, cstart: bool) {
        match self {
            Message::AddSession { clean_start, .. } => *clean_start = cstart,
            _ => unreachable!(),
        }
    }

    pub fn to_packet(&self) -> QPacket {
        match self {
            Message::ClientAck { packet, .. } => packet.clone(),
            Message::Oug { publish, .. } => publish.clone(),
            _ => unreachable!(),
        }
    }

    pub fn to_packet_id(&self) -> Option<PacketID> {
        match self {
            Message::Retain { publish, .. } => publish.to_packet_id(),
            Message::Routed { publish, .. } => publish.to_packet_id(),
            Message::Oug { publish, .. } => publish.to_packet_id(),
            _ => unreachable!(),
        }
    }
}

impl Message {
    pub fn into_oug(self, out_seqno: OutSeqno, packet_id: PacketID) -> Message {
        let mut publish = match self {
            Message::Routed { publish, out_seqno: 0, .. } => publish,
            Message::Retain { publish, out_seqno: 0, .. } => publish,
            _ => unreachable!(),
        };

        assert!(publish.to_packet_id().is_none());
        publish.set_packet_id(packet_id);
        Message::Oug { out_seqno, publish }
    }

    pub fn into_session_args_replica(self) -> SessionArgsReplica {
        match self {
            Message::AddSession {
                shard_id,
                client_id,
                raddr,
                config,
                proto,
                clean_start,
            } => SessionArgsReplica {
                shard_id,
                client_id,
                raddr,
                config,
                proto,
                clean_start,
            },
            _ => unreachable!(),
        }
    }
}

/// Create a message-queue for shard `shard_id` that can hold upto `size` messages.
///
/// `waker` is attached to the [Shard] thread receiving this messages from the queue.
/// When MsgTx is dropped, thread will be woken up using `waker`.
pub fn msg_channel(shard_id: u32, size: usize, waker: Arc<mio::Waker>) -> (MsgTx, MsgRx) {
    let (tx, rx) = mpsc::sync_channel(size);
    let msg_tx = MsgTx { shard_id, tx, waker, count: usize::default() };
    let msg_rx = MsgRx { shard_id, msg_batch_size: size, rx };

    (msg_tx, msg_rx)
}
