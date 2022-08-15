use log::{debug, error, trace};

use std::{cmp, collections::BTreeMap, fmt, mem, net, result};

use crate::broker::{Config, SubscribedTrie};
use crate::broker::{KeepAlive, Message, OutSeqno, PktRx, PktTx, QueueStatus, Shard};

use crate::{v5, ClientID, PacketID, TopicFilter, TopicName};
use crate::{Error, ErrorKind, ReasonCode, Result};

type Messages = Vec<Message>;
type Packets = Vec<v5::Packet>;
type QueuePkt = QueueStatus<v5::Packet>;
type QueueMsg = QueueStatus<Message>;
type OutSeqnos = Vec<OutSeqno>;

/// Type implement the session for every connected client.
///
/// Sessions are hosted within the shards and shards are hosted within the nodes.
pub struct Session {
    /// Client's ClientID that created this session.
    pub client_id: ClientID,
    /// Remote address,
    pub raddr: net::SocketAddr,
    /// Shard hosting this session.
    pub shard_id: u32,
    /// Remote socket address.
    prefix: String,
    /// Broker Configuration.
    config: Config,

    state: SessionState,
}

enum SessionState {
    Active {
        prefix: String,
        config: Config,

        // Immutable set of parameters for this session, after handshake.
        keep_alive: KeepAlive, // Negotiated keep-alive.
        connect: v5::Connect,  // Connect msg that created this session.
        miot_tx: PktTx,        // Outbound channel to Miot thread.
        session_rx: PktRx,     // Inbound channel from Miot thread.

        // MQTT topic-aliases if enabled. ZERO is not allowed.
        topic_aliases: BTreeMap<u16, TopicName>,
        // List of topic-filters subscribed by this client, when ever
        // SUBSCRIBE/UNSUBSCRIBE messages are committed here, [Cluster::topic_filters]
        // will also be updated.
        subscriptions: BTreeMap<TopicFilter, v5::Subscription>,

        // Sorted list of QoS-1 & QoS-2 PacketID for managing incoming duplicate publish.
        inp_qos12: Vec<PacketID>,

        // outgoing QoS-0 PUBLISH messages.
        qos0_back_log: Vec<Message>,
        // Message::ClientAck that needs to be sent to remote client.
        // CONNACK - happens during add_session.
        // PUBACK  - happens after QoS-1 and QoS-2 messaegs are replicated.
        // SUBACK  - happens after SUBSCRIBE is commited to [Cluster].
        // UNSUBACK- happens after UNSUBSCRIBE is committed to [Cluster].
        // PINGRESP- happens for every PINGREQ is handled by this session.
        out_acks: Vec<Message>,

        // This index is a set of un-acked collection of inflight PUBLISH (QoS-1 & 2)
        // messages sent to subscribed clients. Entry is deleted from `qos12_unacks`
        // when ACK is received for PacketID.
        //
        // Note that length of this collection is only as high as the allowed limit of
        // concurrent PUBLISH specified by client.
        qos12_unacks: BTreeMap<PacketID, Message>,
        // This value is incremented for every out-going PUBLISH(qos>0).
        // If index.len() > `receive_maximum`, don't increment this value.
        next_packet_id: PacketID,
        /// Monotonically increasing `seqno`, starting from 1, that is bumped up for
        /// every outgoing publish packet.
        out_seqno: OutSeqno,
        /// Message::Packet outgoing PUBLISH > QoS-0, first land here.
        ///
        /// Entries from this index are deleted after they are removed from
        /// `qos12_unacks` and after they go through the consensus loop.
        back_log: BTreeMap<OutSeqno, Message>,
    },
    #[allow(dead_code)]
    Reconnect {
        // MQTT topic-aliases if enabled. ZERO is not allowed.
        topic_aliases: BTreeMap<u16, TopicName>,
        // List of topic-filters subscribed by this client, when ever
        // SUBSCRIBE/UNSUBSCRIBE messages are committed here, [Cluster::topic_filters]
        // will also be updated.
        subscriptions: BTreeMap<TopicFilter, v5::Subscription>,

        // Sorted list of QoS-1 & QoS-2 PacketID for managing incoming duplicate publish.
        inp_qos12: Vec<PacketID>,

        // This value is incremented for every out-going PUBLISH(qos>0).
        // If index.len() > `receive_maximum`, don't increment this value.
        next_packet_id: PacketID,
        /// Monotonically increasing `seqno`, starting from 1, that is bumped up for
        /// every outgoing publish packet.
        out_seqno: OutSeqno,
    },
    #[allow(dead_code)]
    Replica {
        prefix: String,
        config: Config,

        /// Monotonically increasing `seqno`, starting from 1, that is bumped up for
        /// every outgoing publish packet.
        out_seqno: OutSeqno,
        /// Message::Packet outgoing PUBLISH > QoS-0, first land here.
        ///
        /// Entries from this index are deleted after they are removed from
        /// `qos12_unacks` and after they go through the consensus loop.
        back_log: BTreeMap<OutSeqno, Message>,
    },
}

impl fmt::Debug for SessionState {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            SessionState::Active { .. } => write!(f, "SessionState::Active"),
            SessionState::Reconnect { .. } => write!(f, "SessionState::Reconnect"),
            SessionState::Replica { .. } => write!(f, "SessionState::Replica"),
        }
    }
}

impl SessionState {
    fn incr_out_seqno(&mut self, msg: &mut Message) {
        match self {
            SessionState::Active { out_seqno, .. } => {
                let seqno = *out_seqno;
                *out_seqno = out_seqno.saturating_add(1);
                match msg {
                    Message::Routed { out_seqno, .. } => *out_seqno = seqno,
                    _ => (),
                }
            }
            ss => unreachable!("{:?}", ss),
        }
    }

    fn out_qos0(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
        let (prefix, config, miot_tx, qos0_back_log) = match self {
            SessionState::Active { prefix, config, miot_tx, qos0_back_log, .. } => {
                (prefix, config, miot_tx, qos0_back_log)
            }
            ss => unreachable!("{:?}", ss),
        };

        let m = qos0_back_log.len();
        // TODO: separate back-log limit from mqtt_pkt_batch_size.
        let n = (config.mqtt_pkt_batch_size as usize) * 4;
        if m > n {
            // TODO: if back-pressure is increasing due to a slow receiving client,
            // we will have to take drastic steps, like, closing this connection.
            error!("{} session.qos0_back_log {} pressure > {}", prefix, m, n);
            return QueueStatus::Disconnected(Vec::new());
        }

        for msg in msgs.into_iter() {
            let msg = msg.into_packet(None);
            qos0_back_log.push(msg)
        }
        let back_log = mem::replace(qos0_back_log, vec![]);

        let mut status = flush_to_miot(prefix, miot_tx, back_log);
        let _empty = mem::replace(qos0_back_log, status.take_values());
        status
    }

    fn out_qos(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
        match self {
            SessionState::Active { .. } => self.out_qos_active(msgs),
            SessionState::Replica { .. } => self.out_qos_replica(msgs),
            ss => unreachable!("{:?}", ss),
        }
    }

    fn out_qos_active(&mut self, msgs: Vec<Message>) -> QueueMsg {
        let (prefix, config, miot_tx, qos12_unacks, next_packet_id, back_log) = match self
        {
            SessionState::Active {
                prefix,
                config,
                miot_tx,
                qos12_unacks,
                next_packet_id,
                back_log,
                ..
            } => (prefix, config, miot_tx, qos12_unacks, next_packet_id, back_log),
            ss => unreachable!("{:?}", ss),
        };

        let m = back_log.len();
        // TODO: separate back-log limit from mqtt_pkt_batch_size.
        let n = (config.mqtt_pkt_batch_size as usize) * 4;
        if m > n {
            // TODO: if back-pressure is increasing due to a slow receiving client,
            // we will have to take drastic steps, like, closing this connection.
            error!("{} session.back_log {} pressure > {}", prefix, m, n);
            return QueueStatus::Disconnected(Vec::new());
        }

        if qos12_unacks.len() >= usize::from(config.mqtt_receive_maximum) {
            return QueueStatus::Block(Vec::new());
        }

        for msg in msgs.into_iter() {
            let packet_id = *next_packet_id;
            *next_packet_id = next_packet_id.wrapping_add(1);

            let msg = msg.into_packet(Some(packet_id));
            back_log.insert(msg.to_out_seqno(), msg);
        }

        let max = usize::try_from(config.mqtt_pkt_batch_size).unwrap();
        let mut msgs = Vec::default();
        while msgs.len() < max {
            match back_log.pop_first() {
                Some((_, msg)) => msgs.push(msg),
                None => break,
            }
        }
        for msg in msgs.clone().into_iter() {
            qos12_unacks.insert(msg.to_packet_id(), msg);
        }

        let mut status = flush_to_miot(prefix, miot_tx, msgs);

        // re-insert, cleanup for remaining messages.
        for msg in status.take_values().into_iter() {
            let packet_id = msg.to_packet_id();
            back_log.insert(msg.to_out_seqno(), msg);
            qos12_unacks.remove(&packet_id);
        }

        status
    }

    fn out_qos_replica(&mut self, msgs: Vec<Message>) -> QueueMsg {
        let back_log = match self {
            SessionState::Active { back_log, .. } => back_log,
            ss => unreachable!("{:?}", ss),
        };

        for msg in msgs.into_iter() {
            // TODO: packet_id shall be inserted into the message when replica gets
            //       promoted to active _and_ remote is request for msgs in back_log.
            let msg = msg.into_packet(None);
            back_log.insert(msg.to_out_seqno(), msg);
        }

        QueueStatus::Ok(Vec::new())
    }

    fn out_acks_extend(&mut self, msgs: Vec<Message>) {
        let out_acks = match self {
            SessionState::Active { out_acks, .. } => out_acks,
            ss => unreachable!("{:?}", ss),
        };

        out_acks.extend(msgs.into_iter());
    }

    fn out_acks_publish(&mut self, packet_id: PacketID) {
        let out_acks = match self {
            SessionState::Active { out_acks, .. } => out_acks,
            ss => unreachable!("{:?}", ss),
        };

        out_acks.push(Message::new_pub_ack(v5::Pub::new_pub_ack(packet_id)));
    }

    fn out_acks_flush(&mut self) -> QueueStatus<Message> {
        let (prefix, miot_tx, inp_qos12, out_acks) = match self {
            SessionState::Active { prefix, miot_tx, inp_qos12, out_acks, .. } => {
                (prefix, miot_tx, inp_qos12, out_acks)
            }
            ss => unreachable!("{:?}", ss),
        };

        for ack in out_acks.iter() {
            match ack {
                Message::ClientAck { packet } => match packet {
                    v5::Packet::Publish(publish) => match publish.packet_id {
                        Some(packet_id) => match inp_qos12.binary_search(&packet_id) {
                            Ok(off) => {
                                inp_qos12.remove(off);
                            }
                            Err(_off) => (), // TODO: warning messages
                        },
                        None => (),
                    },
                    _ => (),
                },
                msg => unreachable!("{:?}", msg),
            }
        }

        let mut status = {
            let acks = mem::replace(out_acks, Vec::default());
            flush_to_miot(prefix, miot_tx, acks)
        };
        let _empty = mem::replace(out_acks, status.take_values());
        status
    }

    fn commit_acks(&mut self, out_seqnos: Vec<OutSeqno>) {
        match self {
            SessionState::Active { .. } => (),
            SessionState::Replica { back_log, .. } => {
                for out_seqno in out_seqnos.into_iter() {
                    back_log.remove(&out_seqno);
                }
            }
            ss => unreachable!("{:?}", ss),
        }
    }
}

impl SessionState {
    fn publish_topic_name(&mut self, publ: &v5::Publish) -> Result<TopicName> {
        let (prefix, config, topic_aliases) = match self {
            SessionState::Active { prefix, config, topic_aliases, .. } => {
                (prefix, config, topic_aliases)
            }
            ss => unreachable!("{:?}", ss),
        };

        let (topic_name, topic_alias) = (publ.as_topic_name(), publ.topic_alias());
        let server_alias_max = config.mqtt_topic_alias_max();

        let topic_name = match topic_alias {
            Some(_alias) if server_alias_max.is_none() => err!(
                ProtocolError,
                code: TopicAliasInvalid,
                "{} topic-alias-is-not-supported by broker",
                prefix
            )?,
            Some(alias) if alias > server_alias_max.unwrap() => err!(
                ProtocolError,
                code: TopicAliasInvalid,
                "{} topic-alias-exceeds broker limit {} > {}",
                prefix,
                alias,
                server_alias_max.unwrap()
            )?,
            Some(alias) if topic_name.len() > 0 => {
                match topic_aliases.insert(alias, topic_name.clone()) {
                    Some(old) => debug!(
                        concat!(
                            "{} topic_alias:{} old_topic:{:?} new_topic:{:?}",
                            "replacing ... "
                        ),
                        prefix, alias, old, topic_name
                    ),
                    None => (),
                };
                topic_name.clone()
            }
            Some(alias) => match topic_aliases.get(&alias) {
                Some(topic_name) => topic_name.clone(),
                None => err!(
                    ProtocolError,
                    code: TopicAliasInvalid,
                    "{} alias {} is missing",
                    prefix,
                    alias
                )?,
            },
            None if topic_name.len() == 0 => err!(
                ProtocolError,
                code: TopicNameInvalid,
                "{} alias is ZERO and topic_name is empty",
                prefix
            )?,
            None => topic_name.clone(),
        };

        Ok(topic_name)
    }

    fn is_duplicate(&self, publish: &v5::Publish) -> bool {
        let (config, prefix, inp_qos12) = match self {
            SessionState::Active { config, prefix, inp_qos12, .. } => {
                (config, prefix, inp_qos12)
            }
            ss => unreachable!("{:?}", ss),
        };

        let ignore_dup = config.mqtt_ignore_duplicate;
        match publish.qos {
            v5::QoS::AtMostOnce => false,
            v5::QoS::AtLeastOnce | v5::QoS::ExactlyOnce if publish.duplicate => {
                if ignore_dup {
                    trace!("{} publish:{} duplicate publish recvd", prefix, publish);
                    false
                } else {
                    true
                }
            }
            v5::QoS::AtLeastOnce | v5::QoS::ExactlyOnce => {
                let packet_id = publish.packet_id.unwrap();
                matches!(inp_qos12.binary_search(&packet_id), Ok(_))
            }
        }
    }

    fn book_qos(&mut self, publish: &v5::Publish) -> Result<()> {
        let (prefix, inp_qos12) = match self {
            SessionState::Active { prefix, inp_qos12, .. } => (prefix, inp_qos12),
            ss => unreachable!("{:?}", ss),
        };

        match publish.qos {
            v5::QoS::AtMostOnce => (),
            v5::QoS::AtLeastOnce | v5::QoS::ExactlyOnce => {
                let packet_id = publish.packet_id.unwrap();
                if let Err(off) = inp_qos12.binary_search(&packet_id) {
                    inp_qos12.insert(off, packet_id);
                } else {
                    error!("{} duplicated qos1-booking", prefix);
                }
            }
        };

        Ok(())
    }
}

impl SessionState {
    fn as_subscriptions(&self) -> &BTreeMap<TopicFilter, v5::Subscription> {
        match self {
            SessionState::Active { subscriptions, .. } => subscriptions,
            ss => unreachable!("{:?}", ss),
        }
    }

    fn as_mut_subscriptions(&mut self) -> &mut BTreeMap<TopicFilter, v5::Subscription> {
        match self {
            SessionState::Active { subscriptions, .. } => subscriptions,
            ss => unreachable!("{:?}", ss),
        }
    }

    fn as_mut_out_acks(&mut self) -> &mut Vec<Message> {
        match self {
            SessionState::Active { out_acks, .. } => out_acks,
            ss => unreachable!("{:?}", ss),
        }
    }
}

pub struct SessionStats;

pub struct SessionArgs {
    pub raddr: net::SocketAddr,
    pub client_id: ClientID,
    pub shard_id: u32,
    pub miot_tx: PktTx,
    pub session_rx: PktRx,
}

impl Session {
    pub fn start_active(args: SessionArgs, config: Config, pkt: &v5::Connect) -> Session {
        let prefix = format!("session:{}", args.raddr);
        Session {
            client_id: args.client_id,
            raddr: args.raddr,
            shard_id: args.shard_id,
            prefix: prefix.clone(),
            config: config.clone(),

            state: SessionState::Active {
                prefix: prefix.clone(),
                config: config.clone(),
                keep_alive: KeepAlive::new(args.raddr, &pkt, &config),
                connect: pkt.clone(),
                miot_tx: args.miot_tx,
                session_rx: args.session_rx,
                topic_aliases: BTreeMap::default(),
                subscriptions: BTreeMap::default(),

                inp_qos12: Vec::default(),

                out_acks: Vec::default(),
                qos0_back_log: Vec::default(),

                qos12_unacks: BTreeMap::default(),
                next_packet_id: 1,
                out_seqno: 1,
                back_log: BTreeMap::default(),
            },
        }
    }

    pub fn success_ack(&mut self, pkt: &v5::Connect, _shard: &Shard) -> v5::ConnAck {
        let val = pkt.session_expiry_interval();
        let sei = match (self.config.mqtt_session_expiry_interval, val) {
            (Some(_one), Some(two)) => Some(two),
            (Some(one), None) => Some(one),
            (None, Some(two)) => Some(two),
            (None, None) => None,
        };
        let mut props = v5::ConnAckProperties {
            session_expiry_interval: sei,
            receive_maximum: Some(self.config.mqtt_receive_maximum),
            maximum_qos: Some(self.config.mqtt_maximum_qos.try_into().unwrap()),
            retain_available: Some(self.config.mqtt_retain_available),
            max_packet_size: Some(self.config.mqtt_max_packet_size),
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
        if let Some(keep_alive) = self.to_keep_alive() {
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
        for (topic_filter, value) in self.state.as_subscriptions().iter() {
            topic_filters.unsubscribe(topic_filter, value);
        }
    }
}

// handle incoming packets.
impl Session {
    pub fn route_packets(&mut self, shard: &mut Shard) -> Result<(QueuePkt, OutSeqnos)> {
        let (session_rx, keep_alive) = match &mut self.state {
            SessionState::Active { session_rx, keep_alive, .. } => {
                (session_rx, keep_alive)
            }
            ss => unreachable!("{} {:?}", self.prefix, ss),
        };
        let mut down_status = session_rx.try_recvs(&self.prefix);

        mem::drop(session_rx);

        let rc_disconnected = QueueStatus::Disconnected(Vec::new());
        match down_status.take_values() {
            pkts if pkts.len() == 0 => {
                keep_alive.check_expired()?;
                Ok((down_status, OutSeqnos::default()))
            }
            pkts => {
                keep_alive.live();

                let (status, out_seqnos) = self.handle_packets(shard, pkts)?;

                if let QueueStatus::Disconnected(_) = down_status {
                    error!("{} downstream-rx disconnect", self.prefix);
                    Ok((rc_disconnected, out_seqnos))
                } else if let QueueStatus::Disconnected(_) = status {
                    error!("{} downstream-tx disconnect, or a slow client", self.prefix);
                    Ok((rc_disconnected, out_seqnos))
                } else {
                    Ok((QueueStatus::Ok(Vec::new()), out_seqnos))
                }
            }
        }
    }

    // handle incoming packet, return Message::ClientAck, if any.
    // Disconnected
    // ProtocolError
    fn handle_packets(
        &mut self,
        shard: &mut Shard,
        pkts: Packets,
    ) -> Result<(QueuePkt, OutSeqnos)> {
        let mut out_acks = Vec::default();
        let mut out_seqnos = Vec::default();
        for pkt in pkts.into_iter() {
            match pkt {
                v5::Packet::PingReq => {
                    trace!("{} received PingReq", self.prefix);
                    out_acks.push(Message::new_ping_resp());
                }
                v5::Packet::Publish(publ) => {
                    let has_subscrs = self.rx_publish(shard, publ.clone())?;
                    match (has_subscrs, publ.qos) {
                        (_, v5::QoS::AtMostOnce) => (),
                        (false, _) => {
                            let puback = v5::Pub::new_pub_ack(publ.packet_id.unwrap());
                            out_acks.push(Message::new_pub_ack(puback))
                        }
                        (true, v5::QoS::AtLeastOnce) => (),
                        (true, v5::QoS::ExactlyOnce) => (),
                    }
                }
                v5::Packet::Subscribe(sub) => {
                    out_acks.extend(self.rx_subscribe(shard, sub)?.into_iter());
                }
                v5::Packet::UnSubscribe(_unsub) => todo!(),
                v5::Packet::PubAck(_puback) => {
                    // TODO: cleanup qos12_unacks
                    out_seqnos.push(0);
                    todo!();
                }
                v5::Packet::PubRec(_puback) => todo!(),
                v5::Packet::PubRel(_puback) => todo!(),
                v5::Packet::PubComp(_puback) => todo!(),
                v5::Packet::Disconnect(_disconn) => {
                    // TODO: handle disconnect packet, its header and properties.
                    err!(
                        Disconnected,
                        code: Success,
                        "{} client disconnect",
                        self.prefix
                    )?
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
        }

        self.state.out_acks_extend(out_acks);

        Ok((QueueStatus::Ok(Vec::new()), out_seqnos))
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
            self.state
                .as_mut_subscriptions()
                .insert(filter.topic_filter.clone(), subscription);

            let server_qos = v5::QoS::try_from(self.config.mqtt_maximum_qos).unwrap();
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
    // return `true` if there where subscribers.
    fn rx_publish(&mut self, shard: &mut Shard, publish: v5::Publish) -> Result<bool> {
        if publish.qos > v5::QoS::try_from(self.config.mqtt_maximum_qos).unwrap() {
            err!(
                ProtocolError,
                code: QoSNotSupported,
                "{} publish-qos exceeds server-qos {:?}",
                self.prefix,
                publish.qos
            )?;
        }

        if self.state.is_duplicate(&publish) {
            return Ok(false);
        }

        self.book_retain(shard, &publish)?;
        self.state.book_qos(&publish)?;

        let inp_seqno = shard.incr_inp_seqno();
        let topic_name = self.state.publish_topic_name(&publish)?;
        let subscrs = shard.match_subscribers(&topic_name);
        let has_subscrs = subscrs.len() > 0;

        let ack_needed = match publish.packet_id {
            Some(packet_id) => {
                let msg = Message::new_index(&self.client_id, packet_id);
                shard.book_index(publish.qos, msg);
                true
            }
            None => false,
        };

        for (id, (subscr, ids)) in subscrs.into_iter() {
            if subscr.no_local && id == self.client_id {
                trace!(
                    "{} topic:{:?} client_id:{:?} skipping as no_local",
                    self.prefix,
                    topic_name,
                    id
                );
                continue;
            }

            let publish = {
                let mut publish = publish.clone();
                let retain = subscr.retain_as_published && publish.retain;
                let qos = subscr.route_qos(&publish, self.config.mqtt_maximum_qos);
                publish.set_fixed_header(retain, qos, false);
                publish.set_subscription_ids(ids);
                publish
            };
            let msg = Message::new_routed(self, inp_seqno, publish, id, ack_needed);
            shard.route_to_client(subscr.shard_id, msg);
        }

        Ok(has_subscrs)
    }

    fn book_retain(&mut self, shard: &mut Shard, publish: &v5::Publish) -> Result<()> {
        if publish.retain && !self.config.mqtt_retain_available {
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
    pub fn incr_out_seqno(&mut self, msg: &mut Message) {
        self.state.incr_out_seqno(msg)
    }

    // Handle PUBLISH QoS-0
    pub fn out_qos0(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
        self.state.out_qos0(msgs)
    }

    // Handle PUBLISH QoS-1 and QoS-2
    pub fn out_qos(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
        self.state.out_qos(msgs)
    }

    pub fn out_acks_flush(&mut self) -> QueueStatus<v5::Packet> {
        let status = self.state.out_acks_flush();
        status.map(vec![])
    }

    pub fn out_acks_publish(&mut self, packet_id: PacketID) {
        self.state.out_acks_publish(packet_id)
    }

    pub fn commit_acks(&mut self, out_seqnos: Vec<OutSeqno>) {
        self.state.commit_acks(out_seqnos)
    }
}

impl Session {
    #[inline]
    pub fn to_shard_id(&self) -> u32 {
        self.shard_id
    }

    #[inline]
    pub fn as_config(&self) -> &Config {
        &self.config
    }

    #[inline]
    pub fn as_connect(&self) -> &v5::Connect {
        match &self.state {
            SessionState::Active { connect, .. } => connect,
            ss => unreachable!("{} {:?}", self.prefix, ss),
        }
    }

    #[inline]
    pub fn as_mut_out_acks(&mut self) -> &mut Vec<Message> {
        self.state.as_mut_out_acks()
    }

    #[inline]
    fn to_keep_alive(&self) -> Option<u16> {
        match &self.state {
            SessionState::Active { keep_alive, .. } => keep_alive.keep_alive(),
            ss => unreachable!("{} {:?}", self.prefix, ss),
        }
    }
}

fn flush_to_miot(prefix: &str, miot_tx: &mut PktTx, mut msgs: Vec<Message>) -> QueueMsg {
    let pkts: Vec<v5::Packet> = msgs.iter().map(|m| m.to_v5_packet()).collect();
    let mut status = miot_tx.try_sends(&prefix, pkts);
    let pkts = status.take_values();

    let m = msgs.len();
    let n = pkts.len();
    msgs.drain(..(m - n));

    status.map(msgs)
}
