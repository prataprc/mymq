use std::collections::{BTreeMap, VecDeque};
use std::{net, sync::mpsc, time};

use crate::packet::{PacketRead, PacketWrite};
use crate::{v5, ClientID, PacketID};

pub type QueueTx = mpsc::SyncSender<v5::Packet>;
pub type QueueRx = mpsc::Receiver<v5::Packet>;

pub struct Socket {
    pub client_id: ClientID,
    pub conn: mio::net::TcpStream,
    pub addr: net::SocketAddr,
    pub token: mio::Token,
    pub rd: Source,
    pub wt: Sink,
}

pub struct Source {
    pub pr: PacketRead,
    pub timeout: Option<time::Instant>,
    pub tx: QueueTx,
    pub packets: Vec<v5::Packet>,
}

pub struct Sink {
    pub pw: PacketWrite,
    pub timeout: Option<time::Instant>,
    pub rx: QueueRx,
    pub packets: Vec<v5::Packet>,
}

impl Socket {
    pub fn read_elapsed(&self) -> bool {
        match &self.rd.timeout {
            Some(timeout) if timeout > &time::Instant::now() => true,
            Some(_) | None => false,
        }
    }

    pub fn write_elapsed(&self) -> bool {
        match &self.wt.timeout {
            Some(timeout) if timeout > &time::Instant::now() => true,
            Some(_) | None => false,
        }
    }

    pub fn set_read_timeout(&mut self, retry: bool, timeout: u32) {
        if retry && self.rd.timeout.is_none() {
            self.rd.timeout =
                Some(time::Instant::now() + time::Duration::from_secs(timeout as u64));
        } else if retry == false {
            self.rd.timeout = None;
        }
    }

    pub fn set_write_timeout(&mut self, retry: bool, timeout: u32) {
        if retry && self.wt.timeout.is_none() {
            self.wt.timeout =
                Some(time::Instant::now() + time::Duration::from_secs(timeout as u64));
        } else if retry == false {
            self.wt.timeout = None;
        }
    }
}

// Note that Session::timestamp is related to ClientInp::timestamp.
pub struct ClientInp {
    // Monotonically increasing `seqno`, starting from 1, that is bumped up for every
    // incoming message. This seqno shall be attached to every Message::Packet.
    pub seqno: u64,
    // This index is a collection of un-acked collection of incoming packets.
    // All incoming SUBSCRIBE, UNSUBSCRIBE, PUBLISH (QoS-!,2), PINGREQ
    // will be indexed here using the packet_id. It will be deleted only when
    // corresponding ACK is queued in the outbound channel. And this ACK shall be
    // dispatched Only when:
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
    // Index of seqno gathered from Message::LocalAck from other local-sessions.
    // For N active sessions in this node, there snall be N-1 entries in this index.
    //
    // Entry-value is (ClientOut::seqno, last-ack-instant)
    //
    // Periodically, the minimum value of this list shall be computed and Messages older
    // than the computed-minium shall be purged from the `index`.
    //
    // Entries whose `seqno` is ZERO and `lask-ack-instant` is older that configured
    // limit shall be considered dead session and cluster shall be consulted for
    // cleanup.
    pub timestamp: BTreeMap<ClientID, (u64, time::Instant)>,
}

pub struct ClientOut {
    // Monotonically increasing `seqno`, starting from 1, that is bumped up for every
    // out going message for this session. This will also be sent in PUBLISH UserProp.
    //
    // TODO: can we fold this into consensus seqno ?
    pub seqno: u64,
    // This index is essentially un-acked collection of inflight PUBLISH messages.
    // All incoming PUBLISH messages will be indexed here using monotonically increasing
    // sequence number tracked by `ClientOut::seqno`.
    //
    // Note that before indexing message, its `seqno` shall be overwritten from
    // ClientOut::seqno, and its `packet_id` field will be overwritten with the one
    // procured from `packet_ids` cache.
    //
    // Note that length of this collection is only as high as the allowed limit of
    // concurrent PUBLISH.
    pub index: BTreeMap<PacketID, Message>,
    // Cache of packet-ids that can be used concurrently for outgoing PUBLISH message.
    //
    // Note that the size of this collection shall not exceed the allowed limit of
    // concurrent PUBLISH
    pub packet_ids: VecDeque<PacketID>,
}

pub enum Message {
    /// Message that is periodically, say every second, published by a session to other
    /// local sessions.
    LocalAck {
        client_id: ClientID,
        seqno: u64,           // from ClientOut::seqno
        clock: time::Instant, // instant the ack is sent from local session.
    },
    /// Packets that are received from clients and sent to other local sessions.
    Packet {
        seqno: u64, // from ClientInp::seqno
        packet_id: PacketID,
        packet: v5::Packet,
    },
}

#[inline]
pub fn queue_channel(size: usize) -> (QueueTx, QueueRx) {
    mpsc::sync_channel(size)
}
