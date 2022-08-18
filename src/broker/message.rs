#[cfg(any(feature = "fuzzy", test))]
use arbitrary::{Arbitrary, Error as ArbitraryError, Unstructured};
use log::{error, warn};

use std::sync::{mpsc, Arc};
use std::{fmt, result};

#[allow(unused_imports)]
use crate::broker::Shard;

use crate::broker::{InpSeqno, OutSeqno, QueueStatus, Session};

use crate::{v5, ClientID, PacketID};

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
            match self.waker.wake() {
                Ok(()) => (),
                Err(err) => {
                    error!(
                        "shard-{} waking the receiving shard err:{}",
                        self.shard_id, err
                    )
                }
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
    // session boundary
    /// Acknowledgement packets to remote client, connected to this session.
    ///
    /// CONNACK - happens during add_session.
    /// PUBACK  - happens after QoS-1 and QoS-2 messaegs are replicated.
    /// SUBACK  - happens after SUBSCRIBE is commited to [Cluster].
    /// UNSUBACK- happens after UNSUBSCRIBE is committed to [Cluster].
    /// PINGRESP- happens for every PINGREQ is handled by this session.
    ClientAck { packet: v5::Packet },
    /// PUBLISH Packets Message::Routed converted to Message::Packet before sending
    /// it downstream.
    Packet {
        out_seqno: OutSeqno,
        packet_id: Option<PacketID>,
        publish: v5::Publish,
    },

    // shard boundary
    /// Incoming PUBLISH packets indexed by shards in Shard::RunLoop::index
    Index {
        src_client_id: ClientID,
        packet_id: PacketID,
    },

    // round-trip
    /// Incoming PUBLISH Packets received from clients and routed to other local sessions.
    Routed {
        src_shard_id: u32,    // sending shard-id
        client_id: ClientID,  // receiving client-id
        inp_seqno: InpSeqno,  // shard's inp_seqno
        out_seqno: OutSeqno,  // updated by the receiving session, at a later time.
        publish: v5::Publish, // publish packet, as received from publishing client
        ack_needed: bool,
    },
    /// Message that is periodically published by a session to other local shards.
    LocalAck {
        shard_id: u32,        // shard sending the acknowledgement
        last_acked: InpSeqno, // from publishing-shard.
    },
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Message::ClientAck { .. } => write!(f, "Message::ClientAck"),
            Message::Packet { .. } => write!(f, "Message::Packet"),
            Message::Index { .. } => write!(f, "Message::Index"),
            Message::Routed { .. } => write!(f, "Message::Routed"),
            Message::LocalAck { .. } => write!(f, "Message::LocalAck"),
        }
    }
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for Message {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let val = match uns.arbitrary::<u8>()? % 5 {
            0 => Message::ClientAck {
                packet: match uns.arbitrary::<u8>()? % 9 {
                    0 => v5::Packet::ConnAck(uns.arbitrary()?),
                    1 => v5::Packet::PubAck(uns.arbitrary()?),
                    2 => v5::Packet::PubRec(uns.arbitrary()?),
                    3 => v5::Packet::PubRel(uns.arbitrary()?),
                    4 => v5::Packet::PubComp(uns.arbitrary()?),
                    5 => v5::Packet::SubAck(uns.arbitrary()?),
                    6 => v5::Packet::UnsubAck(uns.arbitrary()?),
                    7 => v5::Packet::PingResp,
                    8 => v5::Packet::Auth(uns.arbitrary()?),
                    _ => unreachable!(),
                },
            },
            1 => Message::Packet {
                out_seqno: uns.arbitrary()?,
                packet_id: uns.arbitrary()?,
                publish: uns.arbitrary()?,
            },
            2 => Message::Index {
                src_client_id: uns.arbitrary()?,
                packet_id: uns.arbitrary()?,
            },
            3 => Message::Routed {
                src_shard_id: uns.arbitrary()?,
                client_id: uns.arbitrary()?,
                inp_seqno: uns.arbitrary()?,
                out_seqno: uns.arbitrary()?,
                publish: uns.arbitrary()?,
                ack_needed: uns.arbitrary()?,
            },
            4 => Message::LocalAck {
                shard_id: uns.arbitrary()?,
                last_acked: uns.arbitrary()?,
            },
            _ => unreachable!(),
        };

        Ok(val)
    }
}

impl Message {
    /// Create a new Message::ClientAck value.
    pub fn new_conn_ack(connack: v5::ConnAck) -> Message {
        Message::ClientAck { packet: v5::Packet::ConnAck(connack) }
    }

    /// Create a new Message::ClientAck value.
    pub fn new_pub_ack(puback: v5::Pub) -> Message {
        Message::ClientAck { packet: v5::Packet::PubAck(puback) }
    }

    /// Create a new Message::ClientAck value.
    pub fn new_ping_resp() -> Message {
        Message::ClientAck { packet: v5::Packet::PingResp }
    }

    /// Create a new Message::Routed value.
    pub fn new_routed(
        sess: &Session,
        seqno: InpSeqno,
        publish: v5::Publish,
        client_id: ClientID,
        ack_needed: bool,
    ) -> Message {
        Message::Routed {
            src_shard_id: sess.to_shard_id(),
            client_id,
            inp_seqno: seqno,
            out_seqno: 0,
            publish,
            ack_needed,
        }
    }

    pub fn new_index(src_client_id: &ClientID, packet_id: PacketID) -> Message {
        Message::Index { src_client_id: src_client_id.clone(), packet_id }
    }

    pub fn into_packet(self, pktid: Option<PacketID>) -> Message {
        match self {
            Message::Routed { out_seqno, mut publish, .. } => {
                if let Some(packet_id) = pktid {
                    publish.set_packet_id(packet_id);
                }
                Message::Packet { out_seqno, packet_id: pktid, publish }
            }
            _ => unreachable!(),
        }
    }

    pub fn to_v5_packet(&self) -> v5::Packet {
        match self {
            Message::ClientAck { packet, .. } => packet.clone(),
            Message::Packet { publish, .. } => v5::Packet::Publish(publish.clone()),
            _ => unreachable!(),
        }
    }

    pub fn to_out_seqno(&self) -> OutSeqno {
        match self {
            Message::Routed { out_seqno, .. } => *out_seqno,
            Message::Packet { out_seqno, .. } => *out_seqno,
            _ => unreachable!(),
        }
    }

    pub fn to_packet_id(&self) -> PacketID {
        match self {
            Message::Packet { packet_id: Some(packet_id), .. } => *packet_id,
            _ => unreachable!(),
        }
    }

    pub fn as_client_id(&self) -> &ClientID {
        match self {
            Message::Routed { client_id, .. } => client_id,
            _ => unreachable!(),
        }
    }

    pub fn to_qos(&self) -> v5::QoS {
        match self {
            Message::Routed { publish, .. } => publish.qos.clone(),
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
