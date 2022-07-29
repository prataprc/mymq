use log::{debug, error, trace};

use std::collections::{BTreeMap, VecDeque};
use std::{cmp, mem, net};

use crate::broker::SubscribedTrie;
use crate::broker::{KeepAlive, Message, OutSeqno, PktRx, PktTx, QueueStatus, Shard};

use crate::{v5, ClientID, Config, PacketID, TopicFilter, TopicName};
use crate::{Error, ErrorKind, ReasonCode, Result};

type Messages = Vec<Message>;
type Packets = Vec<v5::Packet>;
type QueuePkt = QueueStatus<v5::Packet>;

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
    /// Broker Configuration.
    config: Config,

    // Immutable set of parameters for this session, after handshake.
    keep_alive: KeepAlive,                // Negotiated keep-alive.
    session_expiry_interval: Option<u32>, // Negotiated session expiry.
    connect: v5::Connect,                 // Connect msg that created this session.
    miot_tx: PktTx,                       // Outbound channel to Miot thread.
    session_rx: PktRx,                    // Inbound channel from Miot thread.

    // Sorted list of QoS-1 PacketID for which we havn't sent the acknowledgment.
    inp_qos1: Vec<PacketID>,
    // Sorted list of QoS-2 PacketID for which we havn't sent the acknowledgment.
    inp_qos2: Vec<PacketID>,

    // List of acknowledgements that needs to be sent to remote client. Pairs with
    // inp_qos1 and inp_qos2
    out_acks: VecDeque<Message>,

    // This index is a set of un-acked collection of inflight PUBLISH (QoS-1 & 2)
    // messages sent to subscribed clients. Entry is deleted from `qos1_unacks` and
    // `qos2_unacks` once ACK is received for PacketID,
    //
    // Note that length of this collection is only as high as the allowed limit of
    // concurrent PUBLISH.
    qos1_unacks: BTreeMap<PacketID, Message>,
    qos2_unacks: BTreeMap<PacketID, Message>,

    // This value is incremented for every new PUBLISH(qos>0), messages that is going
    // out to the client.
    //
    // We don't increment this value if index.len() exceeds the `receive_maximum`
    // set by the client.
    next_packet_id: PacketID,

    // Consensus state
    state: SessionState,
}

pub struct SessionState {
    /// MQTT topic-aliases if enabled. ZERO is not allowed.
    pub topic_aliases: BTreeMap<u16, TopicName>,
    /// List of topic-filters subscribed by this client, when ever SUBSCRIBE/UNSUBSCRIBE
    /// messages are committed here, [Cluster::topic_filter] will also be updated.
    pub subscriptions: BTreeMap<TopicFilter, v5::Subscription>,

    /// Monotonically increasing `seqno`, starting from 1, that is bumped up for every
    /// outgoing publish packet.
    pub out_seqno: OutSeqno,
    /// All the outgoing PUBLISH > QoS-0, after going through the consensus loop,
    /// shall first land here. While landing here Message::inp_seqno is updated with
    /// OutSeqno. Subsequently they are published to miot-thread and copied to
    /// `qos1_unacks` and `qos2_unacks`.
    ///
    /// Entries from this index are deleted after they are removed from `qos1_unacks`
    /// and `qos2_unacks`, and after they go through the consensus loop.
    pub back_log: BTreeMap<OutSeqno, Message>,
}

pub struct SessionStats;

impl Session {
    pub fn start(args: SessionArgs, config: Config, connect: &v5::Connect) -> Session {
        let state = SessionState {
            topic_aliases: BTreeMap::default(),
            subscriptions: BTreeMap::default(),
            out_seqno: 1,
            back_log: BTreeMap::default(),
        };

        let prefix = format!("session:{}", args.addr);
        let sei = config.mqtt_session_expiry_interval(connect.session_expiry_interval());
        Session {
            client_id: args.client_id,
            prefix: prefix,
            config: config.clone(),

            keep_alive: KeepAlive::new(args.addr, &connect, &config),
            session_expiry_interval: sei,
            connect: connect.clone(),
            miot_tx: args.miot_tx,
            session_rx: args.session_rx,

            inp_qos1: Vec::default(),
            inp_qos2: Vec::default(),
            out_acks: VecDeque::default(),
            qos1_unacks: BTreeMap::default(),
            qos2_unacks: BTreeMap::default(),

            next_packet_id: 1,
            state,
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
        for (topic_filter, value) in self.state.subscriptions.iter() {
            topic_filters.unsubscribe(topic_filter, value);
        }
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

        self.out_acks.extend(msgs.into_iter());
        Ok(self.flush_packets())
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
    // Handle replicated messages, incoming messages could be from:
    // * add_session logic, sending CONNACK, part of handshake.
    // * shard's MsgRx, Message::Packet
    // Note that Message::LocalAck shall not reach this far.
    pub fn in_messages(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
        for msg in msgs.into_iter() {
            match msg {
                msg @ Message::Packet { .. } => {
                    for (out_seqno, msg) in self.expand_subscriptions(msg) {
                        self.state.back_log.insert(out_seqno, msg);
                    }
                }
                Message::ClientAck { .. } | Message::LocalAck { .. } => unreachable!(),
            };
        }

        let m = self.state.back_log.len();
        // TODO: separate back-log limit from mqtt_pkt_batch_size.
        let n = (self.config.mqtt_pkt_batch_size() as usize) * 4;
        if m > n {
            // TODO: if back-pressure is increasing due to a slow receiving client,
            // we will have to take drastic steps, like, closing this connection.
            error!("{} session.back_log {} pressure exceeds limit {}", self.prefix, m, n);
            QueueStatus::Disconnected(Vec::new())
        } else {
            QueueStatus::Ok(Vec::new())
        }
    }

    pub fn flush_out_acks(&mut self) -> QueueStatus<v5::Packet> {
        let mut miot_tx = self.miot_tx.clone(); // when dropped miot thread woken up.

        let pkts: Vec<v5::Packet> =
            self.out_acks.clone().into_iter().map(|m| m.into_packet()).collect();

        let mut status = miot_tx.try_sends(&self.prefix, pkts);
        match status.take_values() {
            rems if rems.len() == 0 => status,
            rems => {
                let n = self.out_acks.len() - rems.len();
                self.out_acks.drain(..n);
                status
            }
        }
    }

    pub fn flush_packets(&mut self) -> QueueStatus<v5::Packet> {
        let receive_maximum = self.connect.receive_maximum() as usize;
        let mut miot_tx = self.miot_tx.clone(); // when dropped miot thread woken up.

        let n =
            cmp::min(receive_maximum - self.qos1_unacks.len(), self.state.back_log.len());

        let mut drained = vec![];
        let mut back_log = mem::replace(&mut self.state.back_log, BTreeMap::default());
        let mut iter = back_log.iter().take(n);
        let status = loop {
            match iter.next() {
                Some((out_seqno, msg)) => {
                    let (packet_id, pkts) = match msg.clone().into_packet() {
                        v5::Packet::Publish(mut publish) => {
                            let packet_id = self.incr_packet_id();
                            publish.set_packet_id(packet_id);
                            (packet_id, vec![v5::Packet::Publish(publish)])
                        }
                        _ => unreachable!(),
                    };
                    let mut status = miot_tx.try_sends(&self.prefix, pkts);
                    match status.take_values() {
                        rems if rems.len() == 0 => {
                            drained.push(*out_seqno);
                            self.qos1_unacks.insert(packet_id, msg.clone());
                        }
                        _rems => break status,
                    }
                }
                None => break QueueStatus::Ok(Vec::new()),
            }
        };
        for out_seqno in drained.into_iter() {
            back_log.remove(&out_seqno);
        }
        let _empty = mem::replace(&mut self.state.back_log, back_log);

        status
    }

    /// Expand outgoing message for matching subscription.
    ///
    /// a. If there are multiple subscriptions, generate a publish-message for each
    ///    subscription. TODO: there is scope for optimization.
    /// b. Use appropriate seqno/packet-id specific to this session.
    /// c. Adjust the qos based on server_qos and subscription_qos.
    /// d. Adjust the retain flag based on subscription's retain-as-published flag.
    /// e. TODO: Send seqno as UserProperty.
    /// f. TODO: Use topic-alias, if enabled.
    pub fn expand_subscriptions(&mut self, msg: Message) -> Vec<(OutSeqno, Message)> {
        let (src_client_id, src_shard_id, subscriptions, publish) = match msg {
            Message::Packet {
                src_client_id,
                src_shard_id,
                subscriptions,
                packet: v5::Packet::Publish(publish),
                ..
            } => (src_client_id, src_shard_id, subscriptions, publish),
            _ => unreachable!(),
        };

        let server_qos = v5::QoS::try_from(self.config.mqtt_maximum_qos()).unwrap();

        let mut msgs: Vec<(OutSeqno, Message)> = Vec::with_capacity(subscriptions.len());
        for subscr in subscriptions.into_iter() {
            let mut publish = publish.clone();
            let retain = subscr.retain_as_published && publish.retain;
            let qos = cmp::min(cmp::min(server_qos, subscr.qos), publish.qos);
            let seqno = self.incr_out_seqno();

            publish.set_fixed_header(retain, qos, false);
            publish.add_subscription_id(subscr.subscription_id);

            let msg = Message::Packet {
                src_client_id: src_client_id.clone(),
                src_shard_id,
                inp_seqno: seqno,
                packet_id: Default::default(),
                subscriptions: Vec::new(),
                packet: v5::Packet::Publish(publish),
            };
            msgs.push((seqno, msg))
        }

        msgs
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
                matches!(self.inp_qos1.binary_search(&publ.packet_id.unwrap()), Ok(_))
            }
            v5::QoS::ExactlyOnce => {
                matches!(self.inp_qos2.binary_search(&publ.packet_id.unwrap()), Ok(_))
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
            v5::QoS::AtLeastOnce => &mut self.inp_qos1,
            v5::QoS::ExactlyOnce => &mut self.inp_qos2,
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
            v5::QoS::AtLeastOnce => &mut self.inp_qos1,
            v5::QoS::ExactlyOnce => &mut self.inp_qos2,
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
                match self.state.topic_aliases.insert(alias, topic_name.clone()) {
                    Some(old) => debug!(
                        "{} for topic-alias {} replacing {:?} with {:?}",
                        self.prefix, alias, old, topic_name
                    ),
                    None => (),
                };
                topic_name.clone()
            }
            Some(alias) => match self.state.topic_aliases.get(&alias) {
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
    pub fn incr_out_seqno(&mut self) -> OutSeqno {
        let seqno = self.state.out_seqno;
        self.state.out_seqno = self.state.out_seqno.saturating_add(1);
        seqno
    }

    pub fn incr_packet_id(&mut self) -> PacketID {
        let packet_id = self.next_packet_id;
        self.next_packet_id = self.next_packet_id.wrapping_add(1);
        packet_id
    }

    #[inline]
    pub fn as_config(&self) -> &Config {
        &self.config
    }

    #[inline]
    pub fn as_client_id(&self) -> &ClientID {
        &self.client_id
    }

    #[inline]
    pub fn as_connect(&self) -> &v5::Connect {
        &self.connect
    }

    #[inline]
    pub fn as_mut_out_acks(&mut self) -> &mut VecDeque<Message> {
        &mut self.out_acks
    }
}
