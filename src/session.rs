use std::collections::{BTreeMap, VecDeque};
use std::{net, sync::mpsc};

use crate::{message, socket, v5};
use crate::{ClientID, Config, PacketID, SubscribedTrie, TopicFilter, TopicName};
use crate::{Error, ErrorKind, ReasonCode, Result};
use crate::{KeepAlive, Message, PktRx, PktTx, Shard};

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
// *Session Reconnect/Restart*
//
// TODO: `session_expiry_interval`.
// TODO: For restart, try seqno handshake between broker/client during CONNECT/CONNACK.
//       seqno, can be exchanged via user-property.
//
// *Shared Subscription*
//
// TODO: In the case of a Shared Subscription where the message is too large to send to
//       one or more of the Clients but other Clients can receive it, the Server can
//       choose either discard the message without sending the message to any of the
//       Clients, or to send the message to one of the Clients that can receive it.
//
// *CONNECT Properties*
//
// TODO: `topic_alias_maximum`
// TODO: `request_response_info`
// TODO: `request_problem_info`
// TODO: `authentication_method`
// TODO: `authentication_data`
// TODO: `payload.username`
// TODO: `payload.password`
//
// *CONNACK Properties*
//
// TODO: `retain_available`
// TODO: `topic_alias_maximum`
// TODO: `shared_subscription_available`
// TODO: `response_information`
// TODO: `server_reference`
// TODO: `authentication_method`
// TODO: `authentication_data`

pub struct SessionArgs {
    pub addr: net::SocketAddr,
    pub client_id: ClientID,
    pub miot_tx: PktTx,
    pub session_rx: PktRx,
}

pub struct Session {
    /// Remote socket address.
    prefix: String,
    client_receive_maximum: u16,
    client_max_packet_size: u32,
    session_expiry_interval: Option<u32>,
    config: Config,

    // Outbound channel to Miot thread.
    miot_tx: PktTx,
    // Inbound channel from Miot thread.
    session_rx: PktRx,
    // inbound queue from client.
    cinp: message::ClientInp,
    // A Periodic Message::LocalAck shall be sent to other sessions. Every incoming
    // PUBLISH from a local-session will be indexed with its cliend_id, and
    // Message::Packet::seqno. Only the latest seqno shall be indexed here, and once
    // the ack is sent back, entry shall be removed from this index.
    timestamp: BTreeMap<ClientID, (u32, u64)>, // (shard_id, seqno)

    // MQTT Will-Delay-Publish
    #[allow(dead_code)]
    will_message: Option<WillMessage>,
    // MQTT Keep alive between client and broker.
    keep_alive: KeepAlive,

    state: SessionState,
}

/// SessionState is a candidate for consensus with replicas.
struct SessionState {
    /// Client's ClientID that created this session.
    client_id: ClientID,
    /// List of topic-filters subscribed by this client, when ever SUBSCRIBE/UNSUBSCRIBE
    /// messages are committed here, [Cluster::topic_filter] will also be updated.
    subscriptions: BTreeMap<TopicFilter, Subscription>,
    /// Manages out-bound messages to client.
    cout: message::ClientOut,
}

#[allow(dead_code)]
struct Subscription {
    subscription_id: u32,
    qos: v5::QoS,
    topic_filter: TopicFilter,
}

#[allow(dead_code)]
struct WillMessage {
    retain: bool,
    qos: v5::QoS,
    properties: v5::WillProperties,
    topic: TopicName,
    payload: Vec<u8>,
}

impl Session {
    pub fn start(args: SessionArgs, config: Config, pkt: &v5::Connect) -> Session {
        let cinp = message::ClientInp {
            seqno: 1,
            index: BTreeMap::default(),
            timestamp: BTreeMap::default(),
        };
        let cout = message::ClientOut {
            seqno: 1,
            index: BTreeMap::default(),
            next_packet_id: 1,
            back_log: VecDeque::default(),
        };
        let state = SessionState {
            client_id: args.client_id,
            subscriptions: BTreeMap::default(),
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
        let sei = config.mqtt_session_expiry_interval(pkt.session_expiry_interval());
        Session {
            prefix: prefix,
            client_receive_maximum: pkt.receive_maximum(),
            client_max_packet_size: pkt.max_packet_size(),
            session_expiry_interval: sei,
            config: config.clone(),

            miot_tx: args.miot_tx,
            session_rx: args.session_rx,
            cinp,
            timestamp: BTreeMap::default(),

            keep_alive: KeepAlive::new(args.addr, &pkt, &config),

            will_message,

            state,
        }
    }

    pub fn success_ack(&mut self, pkt: &v5::Connect, _shard: &Shard) -> v5::ConnAck {
        let mut props = v5::ConnAckProperties {
            session_expiry_interval: self.session_expiry_interval,
            receive_maximum: Some(self.config.mqtt_receive_maximum()),
            maximum_qos: Some(self.config.mqtt_maximum_qos().try_into().unwrap()),
            retain_available: None,
            max_packet_size: Some(self.config.mqtt_max_packet_size()),
            assigned_client_identifier: None,
            wildcard_subscription_available: Some(true),
            subscription_identifiers_available: Some(true),
            shared_subscription_available: None,
            ..v5::ConnAckProperties::default()
        };
        if pkt.payload.client_id.len() == 0 {
            props.assigned_client_identifier = Some((*self.state.client_id).clone());
        }
        if let Some(keep_alive) = self.keep_alive.keep_alive() {
            props.server_keep_alive = Some(keep_alive)
        }
        let connack = v5::ConnAck::new_success(Some(props));

        connack
    }

    pub fn close(mut self) -> Self {
        use std::mem;

        let (tx, rx) = socket::pkt_channel(1); // DUMMY channels
        mem::drop(mem::replace(&mut self.miot_tx, tx));
        mem::drop(mem::replace(&mut self.session_rx, rx));

        self
    }
}

impl Session {
    pub fn remove_topic_filters(&mut self, topic_filters: &SubscribedTrie) {
        for (topic_filter, _) in self.state.subscriptions.iter() {
            topic_filters.unsubscribe(topic_filter).ok();
        }
    }

    pub fn client_max_packet_size(&self) -> u32 {
        self.client_max_packet_size
    }
}

// handle incoming packets.
impl Session {
    pub fn route_packets(&mut self, shard: &Shard) -> Result<()> {
        use crate::miot::rx_packets;

        let batch_size = self.config.mqtt_msg_batch_size() as usize;
        let msgs = match rx_packets(&self.session_rx, batch_size) {
            (_pkts, _empty, true) => {
                // TODO: is it okay to leave pkts hanging ?
                err!(Disconnected, desc: "{} downstream disconnect", self.prefix)?
            }
            (pkts, _empty, _disconnected) => self.handle_packets(shard, pkts)?,
        };

        // msgs is locally generate Message::ClientAck
        self.in_messages(msgs);

        // flush any locally generate Message::ClientAck
        let (_would_block, wake) = self.flush_messages()?;
        if wake {
            shard.as_miot().wake()?;
        }

        self.keep_alive.check_expired()
    }

    // handle incoming packets
    fn handle_packets(
        &mut self,
        shard: &Shard,
        pkts: Vec<v5::Packet>,
    ) -> Result<Vec<Message>> {
        if pkts.len() > 0 {
            self.keep_alive.live();
        }

        let mut msgs = vec![];
        for pkt in pkts.into_iter() {
            msgs.push(self.handle_packet(shard, pkt)?);
        }

        Ok(msgs)
    }

    // handle incoming packet
    fn handle_packet(&self, _shard: &Shard, pkt: v5::Packet) -> Result<Message> {
        // CONNECT, CONNACK, SUBACK, UNSUBACK, PINGRESP all lead to errors.
        // SUBSCRIBE, UNSUBSCRIBE, PINGREQ, DISCONNECT all lead to Message::ClientAck
        // PUBLISH, PUBLISH-ack lead to message routing.

        let msg = match pkt {
            v5::Packet::Publish(_publish) => todo!(),
            v5::Packet::PubAck(_puback) => todo!(),
            v5::Packet::PubRec(_puback) => todo!(),
            v5::Packet::PubRel(_puback) => todo!(),
            v5::Packet::PubComp(_puback) => todo!(),
            v5::Packet::Subscribe(_sub) => todo!(),
            v5::Packet::UnSubscribe(_unsub) => todo!(),
            v5::Packet::PingReq => Message::new_client_ack(v5::Packet::PingReq),
            v5::Packet::Disconnect(_disconn) => {
                // TODO: handle disconnect packet, its header and properties.
                err!(Disconnected, code: Success, "{} client disconnect", self.prefix)?
            }
            v5::Packet::Auth(_auth) => todo!(),
            v5::Packet::Connect(_) => err!(
                ProtocolError,
                code: ProtocolError,
                "{} duplicate connect packet",
                self.prefix
            )?,
            v5::Packet::ConnAck(_) | v5::Packet::SubAck(_) => err!(
                ProtocolError,
                code: ProtocolError,
                "{} packet type {:?} not expected from client",
                self.prefix,
                pkt.to_packet_type()
            )?,
            v5::Packet::UnsubAck(_) | v5::Packet::PingResp => err!(
                ProtocolError,
                code: ProtocolError,
                "{} packet type {:?} not expected from client",
                self.prefix,
                pkt.to_packet_type()
            )?,
        };

        Ok(msg)
    }

    pub fn route_messages(&mut self) -> Result<()> {
        todo!()
    }
}

// Handle incoming messages, incoming messages could be from owning shard's message queue
// or locally generated (like Message::ClientAck)
impl Session {
    pub fn in_messages(&mut self, msgs: Vec<Message>) {
        for msg in msgs.into_iter() {
            match &msg {
                Message::LocalAck { .. } | Message::ClientAck { .. } => (),
                Message::Packet { client_id, shard_id, seqno, .. } => {
                    self.timestamp.insert(client_id.clone(), (*shard_id, *seqno));
                }
            }
            self.state.cout.back_log.push_back(msg);
        }
    }

    // return (would_block,miot_wake)
    pub fn flush_messages(&mut self) -> Result<(bool, bool)> {
        let mut miot_wake = false;
        loop {
            let ok = self.state.cout.index.len() < self.client_receive_maximum.into();
            let msg = self.state.cout.back_log.pop_front();
            match msg {
                Some(Message::LocalAck { client_id, seqno, instant }) => {
                    let old_seqno = match self.cinp.timestamp.get(&client_id) {
                        Some((seqno, _)) => *seqno,
                        None => 0,
                    };
                    assert!(old_seqno < seqno);
                    self.cinp.timestamp.insert(client_id, (seqno, instant));
                }
                Some(msg @ Message::Packet { .. }) if !ok => {
                    self.state.cout.back_log.push_front(msg);
                    break Ok((true, miot_wake));
                }
                Some(mut msg @ Message::Packet { .. }) => {
                    let (would_block, wake) = self.flush_message(&msg)?;
                    miot_wake = miot_wake | wake;
                    if would_block {
                        self.state.cout.back_log.push_front(msg);
                        break Ok((would_block, miot_wake));
                    } else {
                        let (curr_seqno, curr_packet_id) = self.incr_cout_seqno();
                        msg.set_seqno(curr_seqno, curr_packet_id);
                        self.state.cout.index.insert(curr_packet_id, msg);
                    }
                }
                Some(msg @ Message::ClientAck { .. }) => {
                    let (would_block, wake) = self.flush_message(&msg)?;
                    miot_wake = miot_wake | wake;
                    if would_block {
                        self.state.cout.back_log.push_front(msg);
                        break Ok((would_block, miot_wake));
                    }
                }
                None => break Ok((false, miot_wake)),
            }
        }
    }

    // return (would_block,miot_wake)
    // TODO: use ClientOut::seqno in outgoing PUBLISH messages as UserProp
    fn flush_message(&mut self, msg: &Message) -> Result<(bool, bool)> {
        let packet = match &msg {
            Message::ClientAck { packet } => packet.clone(),
            Message::Packet { packet, .. } => packet.clone(),
            _ => unreachable!(),
        };
        match self.miot_tx.try_send(packet) {
            Ok(()) => Ok((false, true)),
            Err(mpsc::TrySendError::Full(_)) => Ok((true, false)),
            Err(mpsc::TrySendError::Disconnected(_)) => {
                err!(IPCFail, desc: "{} miot_tx failed", self.prefix)
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
