use std::collections::{BTreeMap, VecDeque};
use std::{net, sync::mpsc};

use crate::{queue, v5};
use crate::{ClientID, Config, PacketID, TopicFilter, TopicName, TopicTrie};
use crate::{Error, ErrorKind, ReasonCode, Result};
use crate::{KeepAlive, Shard};

// TODO A PUBLISH packet MUST NOT contain a Packet Identifier if its QoS value is
//      set to 0.
// TODO Revisit 2.2.1 Packet Identifier
//
// *Will Message*
//
// TODO The Will Message MUST be published after the Network Connection is subsequently
//      closed and either the Will Delay Interval has elapsed or the Session ends,
//      unless the Will Message has been deleted by the Server on receipt of a
//      DISCONNECT packet with Reason Code 0x00 (Normal disconnection) or a new
//      Network Connection for the ClientID is opened before the Will Delay Interval
//      has elapsed.
//
//      Situations in which the Will Message is published include, but are not
//      limited to:
//      * An I/O error or network failure detected by the Server.
//      * The Client fails to communicate within the Keep Alive time.
//      * The Client closes the Network Connection without first sending a DISCONNECT
//        packet with a Reason Code 0x00 (Normal disconnection).
//      * The Server closes the Network Connection without first receiving a
//        DISCONNECT packet with a Reason Code 0x00 (Normal disconnection).
// TODO The Will Message MUST be removed from the stored Session State in the Server
//      once it has been published or the Server has received a DISCONNECT packet
//      with a Reason Code of 0x00 (Normal disconnection) from the Client.
// TODO In the case of a Server shutdown or failure, the Server MAY defer publication
//      of Will Messages until a subsequent restart. If this happens, there might be
//      a delay between the time the Server experienced failure and when the Will
//      Message is published.
// TODO If the Will Flag is set to 0, then the Will QoS MUST be set to 0 (0x00)
//      If the Will Flag is set to 1, the value of Will QoS can be 0 (0x00),
//      1 (0x01), or 2 (0x02) [MQTT-3.1.2-12]. A value of 3 (0x03) is a Malformed Packet.
//
// *Session Reconnect*
//
// TODO: `session_expiry_interval`.

pub struct SessionArgs {
    pub addr: net::SocketAddr,
    pub client_id: ClientID,
    pub miot_tx: queue::PktTx,
    pub session_rx: queue::PktRx,
}

pub struct Session {
    /// Remote socket address.
    addr: net::SocketAddr,
    prefix: String,
    client_receive_maximum: u16,
    config: Config,

    // Outbound channel to Miot thread.
    miot_tx: queue::PktTx,
    // Inbound channel from Miot thread.
    session_rx: queue::PktRx,
    // inbound queue from client.
    cinp: queue::ClientInp,
    // A Periodic Message::LocalAck shall be sent to other sessions. Every incoming
    // PUBLISH from a local-session will be indexed with its cliend_id, and
    // Message::Packet::seqno. Only the latest seqno shall be indexed here, and once
    // the ack is sent back, entry shall be removed from this index.
    timestamp: BTreeMap<ClientID, (u32, u64)>, // (shard_id, seqno)

    keep_alive: KeepAlive,

    // MQTT Will-Delay-Publish
    will_message: Option<WillMessage>,

    state: SessionState,
}

/// SessionState is a candidate for consensus with replicas.
struct SessionState {
    /// Client's ClientID that created this session.
    client_id: ClientID,
    /// List of topic-filters subscribed by this client, when ever SUBSCRIBE/UNSUBSCRIBE
    /// messages are committed here, [Cluster::topic_filter] will also be updated.
    subscriptions: Vec<Subscription>,
    /// Manages out-bound messages to client.
    cout: queue::ClientOut,
}

struct Subscription {
    subscription_id: u32,
    topic_filter: TopicFilter,
}

struct WillMessage {
    retain: bool,
    qos: v5::QoS,
    properties: v5::WillProperties,
    topic: TopicName,
    payload: Vec<u8>,
}

impl Session {
    pub fn start(args: SessionArgs, config: Config, pkt: &v5::Connect) -> Session {
        let cinp = queue::ClientInp {
            seqno: 1,
            index: BTreeMap::default(),
            timestamp: BTreeMap::default(),
        };
        let cout = queue::ClientOut {
            seqno: 1,
            index: BTreeMap::default(),
            next_packet_id: 1,
            back_log: VecDeque::default(),
        };
        let state = SessionState {
            client_id: args.client_id,
            subscriptions: Vec::default(),
            cout,
        };

        let (_clean_start, wflag, qos, retain) = pkt.flags.unwrap();
        let will_message = match wflag {
            true => Some(WillMessage {
                retain,
                qos,
                properties: pkt.payload.will_properties.clone().unwrap(),
                topic: pkt.payload.will_topic.clone().unwrap(),
                payload: pkt.payload.will_payload.clone().unwrap(),
            }),
            false => None,
        };

        let prefix = format!("session:{}", args.addr);
        Session {
            addr: args.addr,
            prefix: prefix.clone(),
            client_receive_maximum: pkt.receive_maximum(),
            config: config.clone(),

            miot_tx: args.miot_tx,
            session_rx: args.session_rx,
            cinp,
            timestamp: BTreeMap::default(),

            keep_alive: KeepAlive::new(&prefix, &pkt, &config),

            will_message,

            state,
        }
    }

    pub fn success_ack(&mut self, _pkt: &v5::Connect, _shard: &Shard) -> v5::ConnAck {
        let props = v5::ConnAckProperties {
            receive_maximum: Some(self.config.mqtt_receive_maximum()),
            ..v5::ConnAckProperties::default()
        };
        let connack = v5::ConnAck::new_success(Some(props));
        // TODO: `session_present`

        connack
    }

    pub fn close(mut self) -> Self {
        use std::mem;

        let (tx, rx) = queue::pkt_channel(1); // DUMMY channels
        mem::drop(mem::replace(&mut self.miot_tx, tx));
        mem::drop(mem::replace(&mut self.session_rx, rx));

        self
    }
}

impl Session {
    pub fn remove_topic_filters(&mut self, topic_filters: &TopicTrie) {
        for subsc in self.state.subscriptions.iter() {
            topic_filters.unsubscribe(&subsc.topic_filter).ok();
        }
    }
}

impl Session {
    pub fn route(&mut self, shard: &Shard) -> Result<()> {
        use crate::miot::rx_packets;

        match rx_packets(&self.session_rx, self.config.mqtt_msg_batch_size() as usize) {
            (pkts, _empty, true) => {
                self.route_packets(shard, pkts)?;
                err!(Disconnected, desc: "{} downstream disconnect", self.prefix)?
            }
            (pkts, _empty, _disconnected) => self.route_packets(shard, pkts)?,
        }

        self.keep_alive.expired()
    }

    fn route_packets(&mut self, shard: &Shard, pkts: Vec<v5::Packet>) -> Result<()> {
        if pkts.len() > 0 {
            self.keep_alive.live();
        }

        for pkt in pkts.into_iter() {
            self.route_packet(shard, pkt)?
        }

        Ok(())
    }

    fn route_packet(&self, _shard: &Shard, pkt: v5::Packet) -> Result<()> {
        self.check_protocol_error(&pkt)?;

        Ok(())
    }

    fn check_protocol_error(&self, pkt: &v5::Packet) -> Result<()> {
        match pkt {
            v5::Packet::Connect(_) => err!(
                ProtocolError,
                code: ProtocolError,
                "{} duplicate packet error",
                self.prefix
            ),
            v5::Packet::ConnAck(_)
            | v5::Packet::SubAck(_)
            | v5::Packet::UnsubAck(_)
            | v5::Packet::PingResp => err!(
                ProtocolError,
                code: ProtocolError,
                "{} packet type {:?} not expected from client",
                self.prefix,
                pkt.to_packet_type()
            ),
            _ => Ok(()),
        }
    }
}

// Handle incoming messages.
impl Session {
    pub fn in_messages(&mut self, msgs: Vec<queue::Message>) {
        for msg in msgs.into_iter() {
            match &msg {
                queue::Message::LocalAck { .. } | queue::Message::ClientAck { .. } => (),
                queue::Message::Packet { client_id, shard_id, seqno, .. } => {
                    self.timestamp.insert(client_id.clone(), (*shard_id, *seqno));
                }
            }
            self.state.cout.back_log.push_back(msg);
        }
    }

    // return (would_block,miot_wake)
    pub fn flush_messages(&mut self) -> (bool, bool) {
        let mut miot_wake = false;
        loop {
            let ok = self.state.cout.index.len() < self.client_receive_maximum.into();
            let msg = self.state.cout.back_log.pop_front();
            match msg {
                Some(queue::Message::LocalAck { client_id, seqno, instant }) => {
                    let old_seqno = match self.cinp.timestamp.get(&client_id) {
                        Some((seqno, _)) => *seqno,
                        None => 0,
                    };
                    assert!(old_seqno < seqno);
                    self.cinp.timestamp.insert(client_id, (seqno, instant));
                }
                Some(msg @ queue::Message::Packet { .. }) if !ok => {
                    self.state.cout.back_log.push_front(msg);
                    break (true, miot_wake);
                }
                Some(mut msg @ queue::Message::Packet { .. }) => {
                    let (would_block, wake) = self.flush_message(&msg);
                    miot_wake = miot_wake | wake;
                    if would_block {
                        self.state.cout.back_log.push_front(msg);
                        break (would_block, miot_wake);
                    } else {
                        let (curr_seqno, curr_packet_id) = self.incr_cout_seqno();
                        msg.set_seqno(curr_seqno, curr_packet_id);
                        self.state.cout.index.insert(curr_packet_id, msg);
                    }
                }
                Some(msg @ queue::Message::ClientAck { .. }) => {
                    let (would_block, wake) = self.flush_message(&msg);
                    miot_wake = miot_wake | wake;
                    if would_block {
                        self.state.cout.back_log.push_front(msg);
                        break (would_block, miot_wake);
                    }
                }
                None => break (false, miot_wake),
            }
        }
    }

    // return (would_block,miot_wake)
    // TODO: use ClientOut::seqno in outgoing PUBLISH messages.
    fn flush_message(&mut self, msg: &queue::Message) -> (bool, bool) {
        let packet = match &msg {
            queue::Message::ClientAck { packet } => packet.clone(),
            queue::Message::Packet { packet, .. } => packet.clone(),
            _ => unreachable!(),
        };
        match self.miot_tx.try_send(packet) {
            Ok(()) => (false, true),
            Err(mpsc::TrySendError::Full(_)) => (true, false),
            Err(mpsc::TrySendError::Disconnected(_)) => {
                // TODO: we ignore this disconnect because if miot-rx has closed, it will
                // lead to cycle back here via miot->shard.flush_connection
                (false, false)
            }
        }
    }

    fn incr_cout_seqno(&mut self) -> (u64, PacketID) {
        let (seqno, packet_id) = (self.state.cout.seqno, self.state.cout.next_packet_id);
        self.state.cout.seqno = self.state.cout.seqno + 1;
        self.state.cout.next_packet_id =
            match self.state.cout.next_packet_id.wrapping_add(1) {
                0 => 1,
                n => n,
            };
        (seqno, packet_id)
    }
}
