use log::{error, warn};

use std::collections::{BTreeMap, VecDeque};
use std::sync::{mpsc, Arc};

use crate::{v5, ClientID, PacketID, QueueStatus, Session};

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

// This is per-shard data structure.
pub struct ClientInp {
    // Monotonically increasing `seqno`, starting from 1, that is bumped up for every
    // incoming message. This seqno shall be attached to every Message::Packet.
    pub seqno: u64,
    // This index is a set of un-acked incoming PUBLISH (QoS-1,2) packets. They are
    // indexed here using the ClientInp::seqno. It will be deleted only when
    // corresponding ACK is recieved from other local-shards.
    //
    // Entries in this index are deleted based on the activities in `timestamp` index
    // and `ack_timestamp` index.
    //
    // When an entry is being deleted, ACK shall be sent back to the publishing-client
    pub unacks: BTreeMap<u64, (ClientID, Message)>,
    // For N shards in this node, there shall be N-1 entries in this index.
    //
    // Key = shard_id (of other shards)
    // value = (last-routed-seqno, last-received-ack),
    //
    // * When ever a new message is routed to the shard, shard_id's value shall be
    //   updated, its last-routed-seqno shall be updated to the newly routed message's
    //   seqno.
    // * When ever ClientOut::LocalAck is received from`ack_timestamp` shards's
    //   last-recieved-ack shall be updated to local-ack-seqno.
    // * `last-routed-seq` shall always be <= `last-received-ack`.
    // * Entries in  `unacks`, whose seqno are < `last-received-ack` can be deleted.
    // * If `last-routed-seq` == `last-recieved-ack`, then there are no outstanding ACKs.
    pub timestamp: BTreeMap<u32, (u64, u64)>,
    // Back log of messages that needs to be flushed to other local-shards/sessions.
    //
    // SUBSCRIBE, UNSUBSCRIBE, PINGREQ shall be synchronously handled, that is, the call
    // shall block until they are commited to cluster.
    //
    // DISCONNECT and AUTH shall also immediately execute, they may not block since
    // they are not expected to be part of the consensus loop.
    //
    // A routed PUBLISH shall first land here.
    pub shard_back_log: BTreeMap<u32, Vec<Message>>,
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
    // PUBLISH, Retain-PUBLISH
    pub back_log: VecDeque<Message>,
}

#[derive(Clone)]
pub enum Message {
    /// Message that is periodically, say every 30ms, published by a session to other
    /// local sessions.
    LocalAck {
        shard_id: u32,            // shard sending the acknowledgement
        last_received_seqno: u64, // from publishing-shard.
    },
    /// Packets that are received from clients and sent to other local sessions.
    /// Packets that are received from other local session and meant for this client.
    /// Only PUBLISH packets.
    Packet {
        client_id: ClientID, // sending client
        shard_id: u32,       // sending shard
        seqno: u64,          // sending ClientInp::seqno later becomes ClientOut::seqno
        packet_id: PacketID, // from ClientInp or ClientOut
        subscriptions: Vec<v5::Subscription>,
        packet: v5::Packet,
    },
    /// Packets that are generated by sessions locally and sent to clients.
    ///
    /// CONNACK, PUBLISH-ack, SUBACK, UNSUBACK, PINGRESP, AUTH packets.
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

    pub fn into_packet(self) -> v5::Packet {
        match self {
            Message::ClientAck { packet } => packet,
            Message::Packet { packet, .. } => packet,
            _ => unreachable!(),
        }
    }

    pub fn publish_out(self, session: &mut Session) -> Vec<Message> {
        use std::cmp;

        // TODO: should we carry forward the `message_expiry_interval` on routed
        //       publish messages.
        // TODO: if topic_alias is enabled, use that.

        let (client_id, shard_id, subscriptions, publish) = match self {
            Message::Packet { client_id, shard_id, subscriptions, packet, .. } => {
                match packet {
                    v5::Packet::Publish(publish) => {
                        (client_id, shard_id, subscriptions, publish)
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        };

        let server_qos =
            v5::QoS::try_from(session.as_config().mqtt_maximum_qos()).unwrap();

        let mut msgs: Vec<Message> = Vec::with_capacity(subscriptions.len());
        for subscr in subscriptions.into_iter() {
            let mut publish = publish.clone();
            let retain = subscr.retain_as_published && publish.retain;
            let qos = cmp::min(cmp::min(server_qos, subscr.qos), publish.qos);

            let (seqno, packet_id) = session.incr_cout_seqno();

            publish.set_fixed_header(retain, qos, false).set_packet_id(packet_id);
            publish.add_subscription_id(subscr.subscription_id);

            // TODO: set seqno as UserProp

            let msg = Message::Packet {
                client_id: client_id.clone(),
                shard_id,
                seqno,
                packet_id,
                subscriptions: Vec::new(),
                packet: v5::Packet::Publish(publish),
            };
            msgs.push(msg)
        }

        msgs
    }
}

pub fn msg_channel(shard_id: u32, size: usize, waker: Arc<mio::Waker>) -> (MsgTx, MsgRx) {
    let (tx, rx) = mpsc::sync_channel(size);
    let msg_tx = MsgTx { shard_id, tx, waker, count: usize::default() };
    let msg_rx = MsgRx { shard_id, msg_batch_size: size, rx };

    (msg_tx, msg_rx)
}
