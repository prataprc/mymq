use log::{debug, error, trace};

use std::collections::BTreeMap;
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
    /// Shard hosting this session.
    shard_id: u32,
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

    // MQTT topic-aliases if enabled. ZERO is not allowed.
    topic_aliases: BTreeMap<u16, TopicName>,
    // List of topic-filters subscribed by this client, when ever SUBSCRIBE/UNSUBSCRIBE
    // messages are committed here, [Cluster::topic_filters] will also be updated.
    subscriptions: BTreeMap<TopicFilter, v5::Subscription>,

    // Sorted list of QoS-1 & QoS-2 PacketID for managing incoming duplicate publish.
    inp_qos12: Vec<PacketID>,

    // This value is incremented for every out-going PUBLISH(qos>0).
    // If index.len() > `receive_maximum`, don't increment this value.
    next_packet_id: PacketID,
    // Message::ClientAck that needs to be sent to remote client.
    // CONNACK - happens during add_session.
    // PUBACK  - happens after QoS-1 and QoS-2 messaegs are replicated.
    // SUBACK  - happens after SUBSCRIBE is commited to [Cluster].
    // UNSUBACK- happens after UNSUBSCRIBE is committed to [Cluster].
    // PINGRESP- happens for every PINGREQ is handled by this session.
    out_acks: Vec<Message>,
    // This index is a set of un-acked collection of inflight PUBLISH (QoS-1 & 2)
    // messages sent to subscribed clients. Entry is deleted from `qos1e_unacks`.
    // ACK is received for PacketID.
    //
    // Note that length of this collection is only as high as the allowed limit of
    // concurrent PUBLISH.
    qos12_unacks: BTreeMap<PacketID, Message>,
    qos0_back_log: Vec<Message>,

    // Consensus state
    state: SessionState,
}

pub struct SessionState {
    /// Monotonically increasing `seqno`, starting from 1, that is bumped up for every
    /// outgoing publish packet.
    pub out_seqno: OutSeqno,
    /// Message::Packet outgoing PUBLISH > QoS-0, first land here.
    ///
    /// Entries from this index are deleted after they are removed from `qos12_unacks`
    /// and after they go through the consensus loop.
    pub back_log: BTreeMap<OutSeqno, Message>,
}

pub struct SessionStats;

impl Session {
    pub fn start(args: SessionArgs, config: Config, connect: &v5::Connect) -> Session {
        let state = SessionState { out_seqno: 1, back_log: BTreeMap::default() };

        let prefix = format!("session:{}", args.addr);
        let sei = config.mqtt_session_expiry_interval(connect.session_expiry_interval());
        Session {
            client_id: args.client_id,
            shard_id: args.shard_id,
            prefix: prefix,
            config: config.clone(),

            keep_alive: KeepAlive::new(args.addr, &connect, &config),
            session_expiry_interval: sei,
            connect: connect.clone(),
            miot_tx: args.miot_tx,
            session_rx: args.session_rx,

            topic_aliases: BTreeMap::default(),
            subscriptions: BTreeMap::default(),

            inp_qos12: Vec::default(),

            next_packet_id: 1,
            out_acks: Vec::default(),
            qos0_back_log: Vec::default(),
            qos12_unacks: BTreeMap::default(),
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
        for (topic_filter, value) in self.subscriptions.iter() {
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
        let mut out_acks = mem::replace(&mut self.out_acks, Vec::default());
        for pkt in pkts.into_iter() {
            out_acks.extend(self.handle_packet(shard, pkt)?.into_iter());
        }

        let _empty = mem::replace(&mut self.out_acks, out_acks);
        Ok(QueueStatus::Ok(Vec::new()))
    }

    // handle incoming packet, return Message::ClientAck, if any.
    // Disconnected
    // ProtocolError
    fn handle_packet(&mut self, shard: &mut Shard, pkt: v5::Packet) -> Result<Messages> {
        let msgs = match pkt {
            v5::Packet::PingReq => {
                let msg = Message::new_ping_resp();
                vec![msg]
            }
            v5::Packet::Publish(publ) => {
                let has_subscrs = self.rx_publish(shard, publ.clone())?;
                match (has_subscrs, publ.qos) {
                    (_, v5::QoS::AtMostOnce) => {
                        // QoS-0 do not have any acknowledgements.
                        vec![]
                    }
                    (false, _) => {
                        let puback = v5::Pub::new_pub_ack(publ.packet_id.unwrap());
                        let msg = Message::new_pub_ack(puback);
                        vec![msg]
                    }
                    (true, v5::QoS::AtLeastOnce) => {
                        // QoS-1 acks happen only after replication is done.
                        vec![]
                    }
                    (true, v5::QoS::ExactlyOnce) => todo!(),
                }
            }
            v5::Packet::Subscribe(sub) => self.rx_subscribe(shard, sub)?,
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

    // return `true` if there where subscribers.
    fn rx_publish(&mut self, shard: &mut Shard, publish: v5::Publish) -> Result<bool> {
        if publish.qos > v5::QoS::try_from(self.config.mqtt_maximum_qos()).unwrap() {
            err!(
                ProtocolError,
                code: QoSNotSupported,
                "{} publish-qos exceeds server-qos {:?}",
                self.prefix,
                publish.qos
            )?;
        }

        if self.is_duplicate(&publish) {
            return Ok(false);
        }

        self.book_retain(shard, &publish)?;
        self.book_qos(&publish)?;

        let inp_seqno = shard.incr_inp_seqno();
        let topic_name = self.publish_topic_name(&publish)?;
        let subscrs = shard.match_subscribers(&topic_name);
        let has_subscrs = subscrs.len() > 0;

        for (id, (subscr, ids)) in subscrs.into_iter() {
            if subscr.no_local && id == self.client_id {
                trace!("{} skipping {:?} in {:?} no_local", self.prefix, topic_name, id);
                continue;
            }

            let publish = {
                let mut publish = publish.clone();
                let retain = subscr.retain_as_published && publish.retain;
                let qos = subscr.route_qos(&publish, &self.config);
                publish.set_fixed_header(retain, qos, false);
                publish.set_subscription_ids(ids);
                publish
            };
            let qos = publish.qos;
            let msg = Message::new_routed(self, inp_seqno, publish, id);

            shard.route_to_client(&self.client_id, msg, subscr.shard_id, qos);
        }

        Ok(has_subscrs)
    }

    // return suback and retained-messages if any.
    fn rx_subscribe(&mut self, shard: &Shard, sub: v5::Subscribe) -> Result<Messages> {
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
    // Handle PUBLISH QoS-0
    pub fn out_qos0(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
        let m = self.qos0_back_log.len();
        // TODO: separate back-log limit from mqtt_pkt_batch_size.
        let n = (self.config.mqtt_pkt_batch_size() as usize) * 4;
        if m > n {
            // TODO: if back-pressure is increasing due to a slow receiving client,
            // we will have to take drastic steps, like, closing this connection.
            error!("{} session.qos0_back_log {} pressure > {}", self.prefix, m, n);
            return QueueStatus::Disconnected(Vec::new());
        }

        for msg in msgs.into_iter() {
            let msg = msg.into_packet(self.incr_out_seqno(), None);
            self.qos0_back_log.push(msg)
        }

        {
            let back_log = mem::replace(&mut self.qos0_back_log, vec![]);
            let mut status = self.flush_to_miot(back_log);
            let _empty = mem::replace(&mut self.qos0_back_log, status.take_values());
            status
        }
    }

    // Handle PUBLISH QoS-1 and QoS-2
    pub fn out_qos(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
        let m = self.state.back_log.len();
        // TODO: separate back-log limit from mqtt_pkt_batch_size.
        let n = (self.config.mqtt_pkt_batch_size() as usize) * 4;
        if m > n {
            // TODO: if back-pressure is increasing due to a slow receiving client,
            // we will have to take drastic steps, like, closing this connection.
            error!("{} session.state.back_log {} pressure > {}", self.prefix, m, n);
            return QueueStatus::Disconnected(Vec::new());
        }

        if self.qos12_unacks.len() >= usize::from(self.config.mqtt_receive_maximum()) {
            return QueueStatus::Block(Vec::new());
        }

        for msg in msgs.into_iter() {
            let out_seqno = self.incr_out_seqno();
            let packet_id = self.incr_packet_id();
            let msg = msg.into_packet(out_seqno, Some(packet_id));
            self.state.back_log.insert(out_seqno, msg);
        }

        let max = usize::try_from(self.config.mqtt_pkt_batch_size()).unwrap();
        let mut msgs = Vec::default();
        while let Some((_, msg)) = self.state.back_log.pop_first() {
            self.qos12_unacks.insert(msg.to_packet_id(), msg.clone());
            msgs.push(msg);
            if msgs.len() >= max {
                break;
            }
        }
        let mut status = self.flush_to_miot(msgs);
        // re-insert, cleanup for remaining messages.
        for msg in status.take_values().into_iter() {
            let packet_id = msg.to_packet_id();
            self.state.back_log.insert(msg.to_out_seqno(), msg);
            self.qos12_unacks.remove(&packet_id);
        }
        status
    }

    pub fn flush_out_acks(&mut self) -> QueueStatus<v5::Packet> {
        let out_acks = mem::replace(&mut self.out_acks, vec![]);
        let mut status = self.flush_to_miot(out_acks);
        let _empty = mem::replace(&mut self.out_acks, status.take_values());
        status.map(vec![])
    }

    fn flush_to_miot(&self, mut msgs: Vec<Message>) -> QueueStatus<Message> {
        let mut miot_tx = self.miot_tx.clone(); // when dropped miot thread woken up.

        let pkts: Vec<v5::Packet> = msgs.iter().map(|m| m.to_v5_packet()).collect();
        let mut status = miot_tx.try_sends(&self.prefix, pkts);
        let pkts = status.take_values();
        let m = msgs.len();
        let n = pkts.len();
        msgs.drain(..(m - n));

        status.map(msgs)
    }

    pub fn ack_publish(&mut self, packet_id: PacketID) {
        let msg = Message::new_pub_ack(v5::Pub::new_pub_ack(packet_id));
        self.out_acks.push(msg);
    }
}

// Publish related book-keeping
impl Session {
    fn is_duplicate(&self, publish: &v5::Publish) -> bool {
        let ignore_dup = self.config.mqtt_ignore_duplicate();

        match publish.qos {
            v5::QoS::AtMostOnce => false,
            v5::QoS::AtLeastOnce | v5::QoS::ExactlyOnce if publish.duplicate => {
                if ignore_dup {
                    trace!("{} Duplicate publish received {}", self.prefix, publish);
                    false
                } else {
                    true
                }
            }
            v5::QoS::AtLeastOnce | v5::QoS::ExactlyOnce => {
                let packet_id = publish.packet_id.unwrap();
                matches!(self.inp_qos12.binary_search(&packet_id), Ok(_))
            }
        }
    }

    fn book_qos(&mut self, publish: &v5::Publish) -> Result<()> {
        match publish.qos {
            v5::QoS::AtMostOnce => (),
            v5::QoS::AtLeastOnce | v5::QoS::ExactlyOnce => {
                let packet_id = publish.packet_id.unwrap();
                if let Err(off) = self.inp_qos12.binary_search(&packet_id) {
                    self.inp_qos12.insert(off, packet_id);
                } else {
                    error!("{} duplicated qos1-booking", self.prefix);
                }
            }
        };

        Ok(())
    }

    fn publish_topic_name(&mut self, publ: &v5::Publish) -> Result<TopicName> {
        let (topic_name, topic_alias) = (publ.as_topic_name(), publ.topic_alias());
        let server_alias_max = self.config.mqtt_topic_alias_max();

        let topic_name = match topic_alias {
            Some(_alias) if server_alias_max.is_none() => err!(
                ProtocolError,
                code: TopicAliasInvalid,
                "{} topic-alias-is-not-supported by broker",
                self.prefix
            )?,
            Some(alias) if alias > server_alias_max.unwrap() => err!(
                ProtocolError,
                code: TopicAliasInvalid,
                "{} topic-alias-exceeds broker limit {} > {}",
                self.prefix,
                alias,
                server_alias_max.unwrap()
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

    fn book_retain(&mut self, shard: &mut Shard, publish: &v5::Publish) -> Result<()> {
        if publish.retain && !self.config.mqtt_retain_available() {
            err!(
                ProtocolError,
                code: RetainNotSupported,
                "{} retain unavailable",
                self.prefix
            )?;
        } else if publish.retain {
            if publish.payload.as_ref().map(|x| x.len() == 0).unwrap_or(true) {
                shard.as_cluster().reset_retain_topic(publish.topic_name.clone())?;
            } else {
                shard.as_cluster().set_retain_topic(publish.clone())?;
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
    pub fn to_shard_id(&self) -> u32 {
        self.shard_id
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
    pub fn as_mut_out_acks(&mut self) -> &mut Vec<Message> {
        &mut self.out_acks
    }
}