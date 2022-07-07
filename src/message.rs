use log::{error, warn};

use std::collections::{BTreeMap, VecDeque};
use std::sync::{mpsc, Arc};
use std::time;

use crate::{v5, ClientID, PacketID, QueueStatus};

#[derive(Clone)]
pub struct MsgTx {
    shard_id: u32,                 // message queue for shard.
    tx: mpsc::SyncSender<Message>, // shard's incoming message queue.
    waker: Arc<mio::Waker>,        // receiving shard's waker
    count: usize,
}

impl Drop for MsgTx {
    fn drop(&mut self) {
        if self.count > 0 {
            match self.waker.wake() {
                Ok(()) => (),
                Err(err) => {
                    error!("shard-{} waking the receiving shard: {}", self.shard_id, err)
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
}

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

// This is per-session data structure.
// Note that Session::timestamp is related to ClientInp::timestamp.
pub struct ClientInp {
    // Monotonically increasing `seqno`, starting from 1, that is bumped up for every
    // incoming message. This seqno shall be attached to every Message::Packet.
    pub seqno: u64,
    // This index is a collection of un-acked collection of incoming packets.
    // All incoming SUBSCRIBE, UNSUBSCRIBE, PUBLISH (QoS-!,2) shall be indexed here
    // using the packet_id. It will be deleted only when corresponding ACK is queued
    // in the outbound channel. And this ACK shall be dispatched Only when:
    // * PUBLISH-ack is received from other local-sessions.
    // * SUBSCRIBE/UNSUBSCRIBE committed to SessionState.
    //
    // Periodically purge this index based on `min(timestamp:seqno)`. To effeciently
    // implement this index-purge cycle, we use the `timestamp` collection. When ever
    // PUBLISH packet is sent to other local-sessions, `timestamp` index will be updated
    // for ClientID with (0, Instant::now()), provided it does not already have an entry
    // for ClientID.
    //
    // This index is also used to detect duplicate PUBLISH, SUBSCRIBE, and UNSUBSCRIBE
    // packets.
    pub index: BTreeMap<PacketID, Message>,
    // For N active sessions in this node, there can be upto be N-1 entries in this index.
    //
    // Entry-value is (ClientInp::seqno, last-ack-instant), where seqno is this session's
    // ClientInp::seqno.
    //
    // Periodically, the minimum value of this list shall be computed and Messages older
    // than the computed-minium shall be purged from the `index`.
    //
    // Entries whose `seqno` is ZERO and `lask-ack-instant` is older that configured
    // limit shall be considered dead session and cluster shall be consulted for
    // cleanup.
    pub timestamp: BTreeMap<ClientID, (u64, time::Instant)>,
}

// This is per-session data structure.
pub struct ClientOut {
    // Monotonically increasing `seqno`, starting from 1, that is bumped up for every
    // out going message for this session. This will also be sent in PUBLISH UserProp.
    //
    // TODO: can we fold this into consensus seqno ?
    pub seqno: u64,
    // This index is essentially un-acked collection of inflight PUBLISH messages.
    //
    // All incoming messages will be indexed here using monotonically increasing
    // sequence number tracked by `ClientOut::seqno`.
    //
    // Note that before indexing message, its `seqno` shall be overwritten from
    // ClientOut::seqno, and its `packet_id` field will be overwritten with the one
    // procured from `next_packet_id` cache.
    //
    // Note that length of this collection is only as high as the allowed limit of
    // concurrent PUBLISH.
    pub index: BTreeMap<PacketID, Message>,
    // Rolling 16-bit packet-identifier, packet-id ZERO is not used and reserved.
    //
    // This value is incremented for every new PUBLISH(qos>0), SUBSCRIBE, UNSUBSCRIBE
    // messages that is going out to the client.
    //
    // We don't increment this value if index.len() exceeds the `receive_maximum`
    // set by the client.
    pub next_packet_id: PacketID,
    // Back log of messages that needs to be flushed out to the client. All messages
    // meant for client first lands here.
    //
    // CONNACK, PUBLISH-ack, SUBACK, UNSUBACK, PINGRESP, AUTH
    // PUBLISH
    pub back_log: VecDeque<Message>,
}

// This is per-session data structure
pub struct RouteOut {
    // This is the outgoing buffer for all messages that published to other local-sessions.
    // When a packet is going to be published to other local-session, it first lands here
    // Subsequently the hosting shard's MsgTx is used to send the message out. In this,
    // index there is one entry for each shard, the maximum size of Vec<Message> cannot
    // be more that `msg_batch_size`.
    pub index: BTreeMap<u32, Vec<Message>>,
}

pub enum Message {
    /// Message that is periodically, say every 30ms, published by a session to other
    /// local sessions.
    LocalAck {
        client_id: ClientID,
        seqno: u64, // sending-session -> receive-session -> sending-session
        instant: time::Instant, // instant the ack is sent from local session.
    },
    /// Packets that are received from clients and sent to other local sessions.
    /// Packets that are received from other local session and meant for this client.
    /// Only PUBLISH packets.
    Packet {
        client_id: ClientID,
        shard_id: u32,
        seqno: u64,          // from ClientInp::seqno or ClientOut::seqno,
        packet_id: PacketID, // from ClientInp or ClientOut
        packet: v5::Packet,
    },
    /// Packets that are generated by sessions locally and sent to clients.
    ///
    /// CONNACK, PUBLISH-ack, SUBACK, UNSUBACK, PINGRESP, DISCONNECT, AUTH packets.
    ClientAck { packet: v5::Packet },
}

impl Message {
    pub fn new_client_ack(packet: v5::Packet) -> Message {
        Message::ClientAck { packet }
    }

    pub fn set_seqno(&mut self, new_seqno: u64, new_packet_id: PacketID) {
        match self {
            Message::Packet { seqno, packet_id, .. } => {
                *seqno = new_seqno;
                *packet_id = new_packet_id;
            }
            _ => unreachable!(),
        }
    }

    pub fn as_client_id(&self) -> &ClientID {
        match self {
            Message::LocalAck { client_id, .. } => client_id,
            Message::Packet { client_id, .. } => client_id,
            _ => unreachable!(),
        }
    }
}

pub fn msg_channel(shard_id: u32, size: usize, waker: Arc<mio::Waker>) -> (MsgTx, MsgRx) {
    let (tx, rx) = mpsc::sync_channel(size);
    let msg_tx = MsgTx { shard_id, tx, waker, count: usize::default() };
    let msg_rx = MsgRx { shard_id, msg_batch_size: size, rx };

    (msg_tx, msg_rx)
}
