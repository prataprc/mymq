use log::error;

use std::collections::{BTreeMap, VecDeque};
use std::{cmp, net};

use crate::{message, v5};
use crate::{ClientID, Config, PacketID, SubscribedTrie, TopicFilter, TopicName};
use crate::{Error, ErrorKind, ReasonCode, Result};
use crate::{KeepAlive, Message, PktRx, PktTx, QueueStatus, Shard};

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
    pub shard_id: u32,
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
    subscriptions: BTreeMap<TopicFilter, v5::Subscription>,
    /// Manages out-bound messages to client.
    cout: message::ClientOut,
}

pub struct SessionStats;

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

    pub fn close(self) -> SessionStats {
        std::mem::drop(self);
        SessionStats
    }
}

impl Session {
    pub fn remove_topic_filters(&mut self, topic_filters: &mut SubscribedTrie) {
        for (topic_filter, value) in self.state.subscriptions.iter() {
            topic_filters.unsubscribe(topic_filter, value);
        }
    }

    pub fn client_max_packet_size(&self) -> u32 {
        self.client_max_packet_size
    }
}

// handle incoming packets.
impl Session {
    pub fn route_packets(&mut self, shard: &Shard) -> Result<QueueStatus<v5::Packet>> {
        self.keep_alive.check_expired()?;

        let mut status = self.session_rx.try_recvs(&self.prefix);
        let pkts = status.take_values();

        if pkts.len() > 0 {
            self.keep_alive.live()
        }

        let mut msgs = Vec::with_capacity(pkts.len());
        for pkt in pkts.into_iter() {
            msgs.extend(self.handle_packet(shard, pkt)?.into_iter());
        }

        self.in_messages(msgs); // msgs is locally generate Message::ClientAck
        self.flush_messages(); // flush any locally generate Message::ClientAck

        if let QueueStatus::Disconnected(_) = status.clone() {
            error!("{} downstream disconnect", self.prefix);
        }

        Ok(status)
    }

    // handle incoming packet
    fn handle_packet(&mut self, shard: &Shard, pkt: v5::Packet) -> Result<Vec<Message>> {
        // CONNECT, CONNACK, SUBACK, UNSUBACK, PINGRESP all lead to errors.
        // SUBSCRIBE, UNSUBSCRIBE, PINGREQ, DISCONNECT all lead to Message::ClientAck
        // PUBLISH, PUBLISH-ack lead to message routing.

        let msgs = match pkt {
            v5::Packet::PingReq => vec![Message::new_client_ack(v5::Packet::PingReq)],
            v5::Packet::Subscribe(sub) => self.handle_subscribe(shard, sub),
            v5::Packet::UnSubscribe(_unsub) => todo!(),
            v5::Packet::Publish(_publish) => todo!(),
            v5::Packet::PubAck(_puback) => todo!(),
            v5::Packet::PubRec(_puback) => todo!(),
            v5::Packet::PubRel(_puback) => todo!(),
            v5::Packet::PubComp(_puback) => todo!(),
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

        Ok(msgs)
    }

    // return suback and retained-messages if any.
    fn handle_subscribe(&mut self, shard: &Shard, sub: v5::Subscribe) -> Vec<Message> {
        let subscription_id: Option<u32> = match &sub.properties {
            Some(props) => props.subscription_id.clone().map(|x| *x),
            None => None,
        };

        let mut return_codes = Vec::with_capacity(sub.filters.len());
        for filter in sub.filters.iter() {
            let (rfr, retain_as_published, no_local, qos) = filter.opt.unwrap();
            let subscription = v5::Subscription {
                shard_id: shard.shard_id,
                client_id: self.state.client_id.clone(),
                subscription_id: subscription_id,
                topic_filter: filter.topic_filter.clone(),
                qos,
                no_local,
                retain_as_published,
                retain_forward_rule: rfr,
            };

            shard.as_topic_filters().subscribe(
                &filter.topic_filter,
                subscription.clone(),
                false,
            );
            self.state.subscriptions.insert(filter.topic_filter.clone(), subscription);

            let server_qos = v5::QoS::try_from(self.config.mqtt_maximum_qos()).unwrap();
            let rc = match cmp::max(server_qos, qos) {
                v5::QoS::AtMostOnce => v5::SubAckReasonCode::QoS0,
                v5::QoS::AtLeastOnce => v5::SubAckReasonCode::QoS1,
                v5::QoS::ExactlyOnce => v5::SubAckReasonCode::QoS2,
            };
            return_codes.push(rc)
        }

        let sub_ack = v5::SubAck {
            packet_id: sub.packet_id,
            properties: None,
            return_codes,
        };
        vec![Message::ClientAck { packet: v5::Packet::SubAck(sub_ack) }]
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

    pub fn flush_messages(&mut self) -> QueueStatus<Message> {
        let mut miot_tx = self.miot_tx.clone(); // when dropped miot thread woken up.

        if self.state.cout.index.len() > self.client_receive_maximum.into() {
            return QueueStatus::Block(Vec::new());
        }

        let msgs: Vec<Message> = self.state.cout.back_log.drain(..).collect();
        let mut iter = msgs.into_iter();
        loop {
            let msg = iter.next();

            if msg.is_none() {
                break QueueStatus::Ok(Vec::new());
            }

            match msg.unwrap() {
                Message::LocalAck { client_id, seqno, instant } => {
                    match self.cinp.timestamp.insert(client_id, (seqno, instant)) {
                        Some((old_seqno, _)) => assert!(old_seqno < seqno),
                        None => (),
                    }
                }
                Message::Packet { client_id, shard_id, seqno, packet_id, packet } => {
                    match miot_tx.try_sends(&self.prefix, vec![packet.clone()]) {
                        QueueStatus::Ok(_) => {
                            let (seqno, packet_id) = self.incr_cout_seqno();
                            let msg = Message::Packet {
                                client_id,
                                shard_id,
                                seqno,
                                packet_id,
                                packet,
                            };
                            self.state.cout.index.insert(packet_id, msg);
                        }
                        QueueStatus::Block(_pkts) => {
                            let msg = Message::Packet {
                                client_id,
                                shard_id,
                                seqno,
                                packet_id,
                                packet,
                            };
                            self.state.cout.back_log.push_front(msg);
                            break QueueStatus::Block(Vec::new());
                        }
                        QueueStatus::Disconnected(_) => {
                            break QueueStatus::Disconnected(Vec::new());
                        }
                    }
                }
                Message::ClientAck { packet } => {
                    match miot_tx.try_sends(&self.prefix, vec![packet.clone()]) {
                        QueueStatus::Ok(_) => (),
                        QueueStatus::Block(_pkts) => {
                            let msg = Message::ClientAck { packet };
                            self.state.cout.back_log.push_front(msg);
                        }
                        QueueStatus::Disconnected(_pkts) => {
                            break QueueStatus::Disconnected(Vec::new());
                        }
                    }
                }
            }
        }
    }

    pub fn retry_publish(&mut self) {
        todo!()
    }
}

impl Session {
    fn incr_cout_seqno(&mut self) -> (u64, PacketID) {
        let (seqno, packet_id) = (self.state.cout.seqno, self.state.cout.next_packet_id);
        self.state.cout.seqno = self.state.cout.seqno.saturating_add(1);
        self.state.cout.next_packet_id =
            match self.state.cout.next_packet_id.wrapping_add(1) {
                0 => 1,
                n => n,
            };
        (seqno, packet_id)
    }
}
