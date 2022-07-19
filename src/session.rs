use log::{debug, error, trace};

use std::collections::{BTreeMap, VecDeque};
use std::{cmp, net};

use crate::{message, v5};
use crate::{ClientID, Config, PacketID, SubscribedTrie, TopicFilter, TopicName};
use crate::{Error, ErrorKind, ReasonCode, Result};
use crate::{KeepAlive, Message, PktRx, PktTx, QueueStatus, Shard};

type Messages = Vec<Message>;
type Packets = Vec<v5::Packet>;
type QueuePkt = QueueStatus<v5::Packet>;

// TODO Revisit 2.2.1 Packet Identifier
// TODO support topic_alias while broker publishing messages to client.
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
// TODO: `request_response_info`
// TODO: `request_problem_info`
// TODO: `authentication_method`
// TODO: `authentication_data`
// TODO: `payload.username`
// TODO: `payload.password`
//
// *CONNACK Properties*
//
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

/// Type implement the session for every connected client.
///
/// Sessions are hosted within the shards and shards are hosted within the nodes.
pub struct Session {
    /// Client's ClientID that created this session.
    client_id: ClientID,
    /// Remote socket address.
    prefix: String,
    client_receive_maximum: u16,
    client_max_packet_size: u32,
    #[allow(dead_code)]
    client_topic_alias_max: Option<u16>,
    session_expiry_interval: Option<u32>,
    config: Config,

    // Outbound channel to Miot thread.
    miot_tx: PktTx,
    // Inbound channel from Miot thread.
    session_rx: PktRx,

    // MQTT Will-Delay-Publish
    #[allow(dead_code)]
    will_message: Option<WillMessage>,
    // MQTT Keep alive between client and broker.
    keep_alive: KeepAlive,
    // MQTT topic-aliases if enabled. ZERO is not allowed.
    topic_aliases: BTreeMap<u16, TopicName>,
    // MQTT response-information sent via CONNACK, clients can use this to construct
    // ResponseTopic.
    response_info: Option<String>,
    // Sorted list of QoS-1 PacketID for which we havn't sent the acknowledgment.
    qos1: Vec<PacketID>,
    // Sorted list of QoS-2 PacketID for which we havn't sent the acknowledgment.
    qos2: Vec<PacketID>,
    // Message expiry timer for PUBLISH messages on this session.

    // List of topic-filters subscribed by this client, when ever SUBSCRIBE/UNSUBSCRIBE
    // messages are committed here, [Cluster::topic_filter] will also be updated.
    subscriptions: BTreeMap<TopicFilter, v5::Subscription>,
    // Manages out-bound messages to client.
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
        let cout = message::ClientOut {
            seqno: 1,
            index: BTreeMap::default(),
            next_packet_id: 1,
            back_log: VecDeque::default(),
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
            client_id: args.client_id,
            prefix: prefix,
            client_receive_maximum: pkt.receive_maximum(),
            client_max_packet_size: pkt.max_packet_size(),
            client_topic_alias_max: pkt.topic_alias_max(),
            session_expiry_interval: sei,
            config: config.clone(),

            miot_tx: args.miot_tx,
            session_rx: args.session_rx,

            will_message,
            keep_alive: KeepAlive::new(args.addr, &pkt, &config),
            topic_aliases: BTreeMap::default(),
            response_info: None, // TODO: get this from rr.rs

            qos1: Vec::default(),
            qos2: Vec::default(),
            subscriptions: BTreeMap::default(),
            cout,
        }
    }

    pub fn success_ack(&mut self, pkt: &v5::Connect, _shard: &Shard) -> v5::ConnAck {
        let mut props = v5::ConnAckProperties {
            session_expiry_interval: self.session_expiry_interval,
            receive_maximum: Some(self.config.mqtt_receive_maximum()),
            maximum_qos: Some(self.config.mqtt_maximum_qos().try_into().unwrap()),
            retain_available: Some(self.config.mqtt_retain_available()),
            max_packet_size: Some(self.config.mqtt_max_packet_size()),
            assigned_client_identifier: None,
            wildcard_subscription_available: Some(true),
            subscription_identifiers_available: Some(true),
            shared_subscription_available: None,
            topic_alias_max: self.config.mqtt_topic_alias_max(),
            ..v5::ConnAckProperties::default()
        };
        if pkt.payload.client_id.len() == 0 {
            props.assigned_client_identifier = Some((*self.client_id).clone());
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
        for (topic_filter, value) in self.subscriptions.iter() {
            topic_filters.unsubscribe(topic_filter, value);
        }
    }

    pub fn client_max_packet_size(&self) -> u32 {
        self.client_max_packet_size
    }
}

// handle incoming packets.
impl Session {
    pub fn route_packets(&mut self, shard: &mut Shard) -> Result<QueuePkt> {
        let rc_disconnected = QueueStatus::Disconnected(Vec::new());

        let mut down_status = self.session_rx.try_recvs(&self.prefix);
        match down_status.take_values() {
            pkts if pkts.len() == 0 => {
                self.keep_alive.check_expired()?;
                Ok(QueueStatus::Ok(Vec::new()))
            }
            pkts => {
                self.keep_alive.live();

                let status = self.handle_packets(shard, pkts)?;

                if let QueueStatus::Disconnected(_) = down_status {
                    error!("{} downstream-rx disconnect", self.prefix);
                    Ok(rc_disconnected)
                } else if let QueueStatus::Disconnected(_) = status {
                    error!("{} downstream-tx disconnect, or a slow client", self.prefix);
                    Ok(rc_disconnected)
                } else {
                    Ok(QueueStatus::Ok(Vec::new()))
                }
            }
        }
    }

    fn handle_packets(&mut self, shard: &mut Shard, pkts: Packets) -> Result<QueuePkt> {
        let mut msgs = Vec::with_capacity(pkts.len());

        for pkt in pkts.into_iter() {
            msgs.extend(self.handle_packet(shard, pkt)?.into_iter());
        }

        if let QueueStatus::Disconnected(_) = self.in_messages(msgs) {
            Ok(QueueStatus::Disconnected(Vec::new()))
        } else {
            Ok(self.flush_messages())
        }
    }

    // handle incoming packet, return Message::LocalAck, if any.
    // Disconnected
    // ProtocolError
    fn handle_packet(&mut self, shard: &mut Shard, pkt: v5::Packet) -> Result<Messages> {
        // SUBSCRIBE, UNSUBSCRIBE, PINGREQ, DISCONNECT all lead to Message::ClientAck
        // PUBLISH, PUBLISH-ack lead to message routing.

        let msgs = match pkt {
            v5::Packet::PingReq => vec![Message::new_client_ack(v5::Packet::PingReq)],
            v5::Packet::Publish(publish) => self.do_publish(shard, publish)?,
            v5::Packet::Subscribe(sub) => self.do_subscribe(shard, sub)?,
            v5::Packet::UnSubscribe(_unsub) => todo!(),
            v5::Packet::PubAck(_puback) => todo!(),
            v5::Packet::PubRec(_puback) => todo!(),
            v5::Packet::PubRel(_puback) => todo!(),
            v5::Packet::PubComp(_puback) => todo!(),
            v5::Packet::Disconnect(_disconn) => {
                // TODO: handle disconnect packet, its header and properties.
                err!(Disconnected, code: Success, "{} client disconnect", self.prefix)?
            }
            v5::Packet::Auth(_auth) => todo!(),

            // CONNECT, CONNACK, SUBACK, UNSUBACK, PINGRESP all lead to errors.
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

    fn do_publish(&mut self, shard: &mut Shard, publ: v5::Publish) -> Result<Messages> {
        // TODO: However, as the Server is permitted to map the Topic Name to another
        //       name, it might not be the same as the Topic Name in the original
        //       PUBLISH packet.

        if self.is_duplicate(&publ) {
            return Ok(Vec::new());
        }
        self.book_retain(shard, &publ)?;
        self.book_qos(&publ)?;
        let topic_name = self.publish_topic_name(&publ)?;
        let subscrs = shard.match_subscribers(self, &topic_name);

        for (_match_client_id, subscrs) in subscrs.into_iter() {
            if subscrs.len() == 0 {
                continue;
            }

            shard.route_to_client(self, subscrs, publ.clone());

            // TODO: handle retain_as_published.
            // TODO: handle retain_forward_rule
        }

        // TODO: handle `message_expiry_interval`

        Ok(Vec::new())
    }

    // return suback and retained-messages if any.
    fn do_subscribe(&mut self, shard: &Shard, sub: v5::Subscribe) -> Result<Messages> {
        let subscription_id: Option<u32> = match &sub.properties {
            Some(props) => props.subscription_id.clone().map(|x| *x),
            None => None,
        };

        let mut return_codes = Vec::with_capacity(sub.filters.len());
        for filter in sub.filters.iter() {
            let (rfr, retain_as_published, no_local, qos) = filter.opt.unwrap();
            let subscription = v5::Subscription {
                topic_filter: filter.topic_filter.clone(),

                client_id: self.client_id.clone(),
                shard_id: shard.shard_id,
                subscription_id: subscription_id,
                qos,
                no_local,
                retain_as_published,
                retain_forward_rule: rfr,
            };

            shard
                .as_topic_filters()
                .subscribe(&filter.topic_filter, subscription.clone());
            self.subscriptions.insert(filter.topic_filter.clone(), subscription);

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

        // When a new Non‑shared Subscription is made, the last retained message, if any,
        // on each matching topic name is sent to the Client as directed by the
        // Retain Handling Subscription Option. These messages are sent with the RETAIN
        // flag set to 1. Which retained messages are sent is controlled by the Retain
        // Handling Subscription Option. At the time of the Subscription:
        //
        // * If Retain Handling is set to 0 the Server MUST send the retained messages
        //   matching the Topic Filter of the subscription to the Client [MQTT-3.3.1-9].
        // * If Retain Handling is set to 1 then if the subscription did not already exist,
        //   the Server MUST send all retained message matching the Topic Filter of the
        //   subscription to the Client, and if the subscription did exist the Server
        //   MUST NOT send the retained messages. [MQTT-3.3.1-10].
        // * If Retain Handling is set to 2, the Server MUST NOT send the retained
        //   messages [MQTT-3.3.1-11].

        Ok(vec![Message::ClientAck { packet: v5::Packet::SubAck(sub_ack) }])
    }
}

impl Session {
    // Handle incoming messages, incoming messages could be from:
    // * add_session logic, sending CONNACK, part of handshake.
    // * shard's MsgRx, Message::{LocalAck, Packet}
    // * locally generated Message::ClientAck from Session::route_packets.
    // Note that Message::LocalAck shall not reach this far.
    pub fn in_messages(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
        for msg in msgs.into_iter() {
            match msg {
                msg @ Message::ClientAck { .. } => {
                    self.cout.back_log.push_back(msg);
                }
                msg @ Message::Packet { .. } => msg
                    .cout_publish(self)
                    .into_iter()
                    .for_each(|msg| self.cout.back_log.push_back(msg)),
                Message::LocalAck { .. } => unreachable!(),
            };
        }

        let m = self.cout.back_log.len();
        // TODO: separate back-log limit from mqtt_pkt_batch_size.
        let n = (self.config.mqtt_pkt_batch_size() as usize) * 2;
        if m > n {
            // TODO: if back-pressure is increasing due to a slow receiving client,
            // we will have to take drastic steps, like, closing this connection.
            error!("{} cout.back_log {} pressure exceeds limit {}", self.prefix, m, n);
            QueueStatus::Disconnected(Vec::new())
        } else {
            QueueStatus::Ok(Vec::new())
        }
    }

    pub fn flush_messages(&mut self) -> QueueStatus<v5::Packet> {
        if self.cout.index.len() > self.client_receive_maximum.into() {
            return QueueStatus::Block(Vec::new());
        }

        let mut miot_tx = self.miot_tx.clone(); // when dropped miot thread woken up.

        let mut msgs: Vec<Message> = self.cout.back_log.drain(..).collect();
        let pkts: Vec<v5::Packet> =
            msgs.clone().into_iter().map(|m| m.into_packet()).collect();

        let mut status = miot_tx.try_sends(&self.prefix, pkts);

        let ok_msgs = msgs.split_off(msgs.len() - status.take_values().len());
        // book `ok_msgs` as inflight messages
        for msg in ok_msgs.into_iter() {
            match &msg {
                Message::Packet { packet_id, .. } => {
                    self.cout.index.insert(*packet_id, msg);
                }
                Message::ClientAck { .. } => (),
                Message::LocalAck { .. } => (),
            }
        }
        // remaining messages, if any.
        for msg in msgs.into_iter() {
            self.cout.back_log.push_back(msg);
        }

        status
    }

    pub fn retry_publish(&mut self) {
        todo!()
    }
}

// Publish related book-keeping
impl Session {
    fn is_duplicate(&self, publ: &v5::Publish) -> bool {
        let ignore_dup = self.config.mqtt_ignore_duplicate();

        match publ.qos {
            v5::QoS::AtLeastOnce | v5::QoS::ExactlyOnce
                if publ.duplicate && ignore_dup =>
            {
                trace!("{} Duplicate publish received {}", self.prefix, publ);
                false
            }
            v5::QoS::AtLeastOnce => {
                matches!(self.qos1.binary_search(&publ.packet_id.unwrap()), Ok(_))
            }
            v5::QoS::ExactlyOnce => {
                matches!(self.qos2.binary_search(&publ.packet_id.unwrap()), Ok(_))
            }
            v5::QoS::AtMostOnce => false,
        }
    }

    fn book_qos(&mut self, publ: &v5::Publish) -> Result<bool> {
        let server_qos = v5::QoS::try_from(self.config.mqtt_maximum_qos()).unwrap();
        if publ.qos > server_qos {
            err!(
                ProtocolError,
                code: QoSNotSupported,
                "{} publish-qos exceeds server-qos {:?}",
                self.prefix,
                publ.qos
            )?;
        }

        let qos_vec = match publ.qos {
            v5::QoS::AtMostOnce => return Ok(true),
            v5::QoS::AtLeastOnce => &mut self.qos1,
            v5::QoS::ExactlyOnce => &mut self.qos2,
        };

        let packet_id = publ.packet_id.unwrap();
        match qos_vec.binary_search(&packet_id) {
            Ok(_off) => Ok(false),
            Err(off) => {
                qos_vec.insert(off, packet_id);
                Ok(true)
            }
        }
    }

    fn unbook_qos(&mut self, publ: &v5::Publish) -> bool {
        let qos_vec = match publ.qos {
            v5::QoS::AtMostOnce => return true,
            v5::QoS::AtLeastOnce => &mut self.qos1,
            v5::QoS::ExactlyOnce => &mut self.qos2,
        };

        let packet_id = publ.packet_id.unwrap();
        match qos_vec.binary_search(&packet_id) {
            Ok(off) => {
                qos_vec.remove(off);
                true
            }
            Err(_off) => false,
        }
    }

    fn publish_topic_name(&mut self, publ: &v5::Publish) -> Result<TopicName> {
        let (topic_name, topic_alias) = (publ.as_topic_name(), publ.topic_alias());
        let alias_max = self.config.mqtt_topic_alias_max();

        let topic_name = match topic_alias {
            Some(_alias) if alias_max.is_none() => err!(
                ProtocolError,
                code: TopicAliasInvalid,
                "{} topic-alias-is-not-supported by broker",
                self.prefix
            )?,
            Some(alias) if alias > alias_max.unwrap() => err!(
                ProtocolError,
                code: TopicAliasInvalid,
                "{} topic-alias-exceeds broker limit {} > {}",
                self.prefix,
                alias,
                alias_max.unwrap()
            )?,
            Some(alias) if topic_name.len() > 0 => {
                match self.topic_aliases.insert(alias, topic_name.clone()) {
                    Some(old) => debug!(
                        "{} for topic-alias {} replacing {:?} with {:?}",
                        self.prefix, alias, old, topic_name
                    ),
                    None => (),
                };
                topic_name.clone()
            }
            Some(alias) => match self.topic_aliases.get(&alias) {
                Some(topic_name) => topic_name.clone(),
                None => err!(
                    ProtocolError,
                    code: TopicAliasInvalid,
                    "{} alias {} is missing",
                    self.prefix,
                    alias
                )?,
            },
            None if topic_name.len() == 0 => err!(
                ProtocolError,
                code: TopicNameInvalid,
                "{} alias is ZERO and topic_name is empty",
                self.prefix
            )?,
            None => topic_name.clone(),
        };

        Ok(topic_name)
    }

    fn book_retain(&mut self, shard: &mut Shard, publ: &v5::Publish) -> Result<()> {
        if publ.retain && !self.config.mqtt_retain_available() {
            err!(
                ProtocolError,
                code: RetainNotSupported,
                "{} retain unavailable",
                self.prefix
            )?;
        } else if publ.retain {
            if publ.payload.as_ref().map(|x| x.len() == 0).unwrap_or(true) {
                shard.as_cluster().reset_retain_topic(publ.topic_name.clone())?;
            } else {
                shard.as_cluster().set_retain_topic(publ.clone())?;
            }
        }

        Ok(())
    }
}

impl Session {
    pub fn incr_cout_seqno(&mut self) -> (u64, PacketID) {
        let (seqno, packet_id) = (self.cout.seqno, self.cout.next_packet_id);
        self.cout.seqno = self.cout.seqno.saturating_add(1);
        self.cout.next_packet_id = match self.cout.next_packet_id.wrapping_add(1) {
            0 => 1,
            n => n,
        };
        (seqno, packet_id)
    }

    #[inline]
    pub fn as_config(&self) -> &Config {
        &self.config
    }

    #[inline]
    pub fn as_client_id(&self) -> &ClientID {
        &self.client_id
    }
}
