use log::{debug, error, trace};

use std::{cmp, collections::BTreeMap, fmt, mem, net, result};

use crate::broker::{Config, RouteIO, SubscribedTrie};
use crate::broker::{KeepAlive, Message, OutSeqno, PktRx, PktTx, QueueStatus, Shard};

use crate::{v5, ClientID, PacketID, TopicFilter, TopicName};
use crate::{Error, ErrorKind, ReasonCode, Result};

type Messages = Vec<Message>;
type QueuePkt = QueueStatus<v5::Packet>;
type QueueMsg = QueueStatus<Message>;
type OutSeqnos = Vec<OutSeqno>;
type HandleArgs = (&mut Shard, &mut RouteIO, v5::Packet);

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
    pub prefix: String,
    /// Broker Configuration.
    pub config: Config,

    pub state: SessionState,
    pub stats: Stats,
}

impl Default for Session {
    fn default() -> Session {
        Session {
            client_id: ClientID::default(),
            raddr: "0.0.0.0:0".parse().unwrap(),
            shard_id: u32::default(),
            prefix: String::default(),
            config: Config::default(),
            state: SessionState::None,
            stats: Stats::default(),
        }
    }
}

pub struct SessionArgsActive {
    pub raddr: net::SocketAddr,
    pub config: Config,
    pub client_id: ClientID,
    pub shard_id: u32,
    pub miot_tx: PktTx,
    pub session_rx: PktRx,
    pub connect: v5::Connect,
}

pub struct SessionArgsReplica {
    pub raddr: net::SocketAddr,
    pub config: Config,
    pub client_id: ClientID,
    pub shard_id: u32,
}

#[derive(Clone, Default)]
pub struct Stats {
    n_pings: usize,
}

// initial = None
// None -> Active / Replica
// Active -> Active / Reconnect
// Replica -> Active
// Reconnect -> Active / Replica
impl Session {
    pub fn into_active(self, args: SessionArgsActive) -> Session {
        use SessionState::{Active, Reconnect, Replica};

        let prefix = format!("session:active:{}", args.raddr);
        let keep_alive = KeepAlive::new(&args);

        let state = match self.state {
            SessionState::None => SessionState::Active {
                prefix: prefix.clone(),
                config: args.config.clone(),
                keep_alive,
                connect: args.connect,
                miot_tx: args.miot_tx,
                session_rx: args.session_rx,

                topic_aliases: BTreeMap::default(),
                subscriptions: BTreeMap::default(),

                inc_qos12: Vec::default(),
                oug_qos12: BTreeMap::default(),

                oug_acks: Vec::default(),
                oug_back_log: Vec::default(),

                next_packet_id: 1,
                cs_out_seqno: 1,
                cs_back_log: BTreeMap::default(),
            },
            Replica { cs_out_seqno, cs_back_log, .. } => SessionState::Active {
                prefix: prefix.clone(),
                config: args.config.clone(),
                keep_alive,
                connect: args.connect,
                miot_tx: args.miot_tx,
                session_rx: args.session_rx,

                topic_aliases: BTreeMap::default(),
                subscriptions: BTreeMap::default(),

                inc_qos12: Vec::default(),
                oug_qos12: BTreeMap::default(),

                oug_acks: Vec::default(),
                oug_back_log: Vec::default(),

                next_packet_id: 1,
                cs_out_seqno,
                cs_back_log,
            },
            Reconnect {
                topic_aliases,
                subscriptions,
                cs_out_seqno,
                cs_back_log,
                ..
            } => SessionState::Active {
                prefix: prefix.clone(),
                config: args.config.clone(),
                keep_alive,
                connect: args.connect,
                miot_tx: args.miot_tx,
                session_rx: args.session_rx,

                topic_aliases,
                subscriptions,

                inc_qos12: Vec::default(),

                oug_acks: Vec::default(),
                oug_back_log: Vec::default(),

                oug_qos12: BTreeMap::default(),
                next_packet_id: 1,
                cs_out_seqno,
                cs_back_log,
            },
            Active { .. } => SessionState::Active {
                prefix: prefix.clone(),
                config: args.config.clone(),
                keep_alive,
                connect: args.connect,
                miot_tx: args.miot_tx,
                session_rx: args.session_rx,

                topic_aliases: BTreeMap::default(),
                subscriptions: BTreeMap::default(),

                inc_qos12: Vec::default(),

                oug_acks: Vec::default(),
                oug_back_log: Vec::default(),

                oug_qos12: BTreeMap::default(),
                next_packet_id: 1,
                cs_out_seqno: 1,
                cs_back_log: BTreeMap::default(),
            },
        };

        Session {
            client_id: args.client_id,
            raddr: args.raddr,
            shard_id: args.shard_id,
            prefix: prefix.clone(),
            config: args.config.clone(),
            state,
            stats: Stats::default(),
        }
    }

    pub fn into_replica(self, args: SessionArgsReplica) -> Session {
        use SessionState::Reconnect;

        let prefix = format!("session:replica:{}", args.raddr);

        let state = match self.state {
            SessionState::None => SessionState::Replica {
                prefix: prefix.clone(),
                config: args.config.clone(),
                cs_out_seqno: 1,
                cs_back_log: BTreeMap::default(),
            },
            Reconnect { cs_out_seqno, cs_back_log, .. } => SessionState::Replica {
                prefix: prefix.clone(),
                config: args.config.clone(),
                cs_out_seqno,
                cs_back_log,
            },
            ss => unreachable!("{:?}", ss),
        };

        Session {
            client_id: args.client_id,
            raddr: args.raddr,
            shard_id: args.shard_id,
            prefix: prefix.clone(),
            config: args.config.clone(),

            state,
            stats: Stats::default(),
        }
    }

    pub fn into_reconnect(self) -> Session {
        let prefix = format!("session:reconnect:{}", self.raddr);

        let state = match self.state {
            SessionState::Active {
                config,
                topic_aliases,
                subscriptions,
                cs_out_seqno,
                cs_back_log,
                ..
            } => SessionState::Reconnect {
                prefix: prefix.clone(),
                config,
                topic_aliases,
                subscriptions,
                cs_out_seqno,
                cs_back_log,
            },
            ss => unreachable!("{:?}", ss),
        };

        Session {
            client_id: self.client_id,
            raddr: self.raddr,
            shard_id: self.shard_id,
            prefix,
            config: self.config.clone(),
            state,
            stats: self.stats,
        }
    }

    pub fn success_ack(&mut self, pkt: &v5::Connect) -> v5::ConnAck {
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

    pub fn close(self) -> Stats {
        self.stats
    }
}

impl Session {
    pub fn remove_topic_filters(&mut self, filters: &mut SubscribedTrie) {
        for (topic_filter, value) in self.state.as_subscriptions().iter() {
            filters.unsubscribe(topic_filter, value);
        }
    }
}

// handle incoming packets.
impl Session {
    // Returned RouteIO has following message types:
    // * Message::ClientAck carrying PingResp.
    // * Message::ClientAck carrying SubAck.
    // * Message::ClientAck carrying UnsubAck.
    // * Message::ClientAck carrying PubAck for QoS1.
    // * Message::Retain carrying retained publish to be sent back to this session.
    // * Message::ShardIndex to be indexed by shard for QoS > 0, managing ack.
    // * Message::Routed publish msg one of each subscribing client.
    pub fn route_packets(&mut self, shard: &mut Shard) -> Result<RouteIO> {
        let (session_rx, keep_alive) = match &mut self.state {
            SessionState::Active { session_rx, keep_alive, .. } => {
                (session_rx, keep_alive)
            }
            ss => unreachable!("{} {:?}", self.prefix, ss),
        };

        let mut down_status = session_rx.try_recvs(&self.prefix);
        mem::drop(session_rx);

        let mut route_io = RouteIO::default();

        route_io.disconnected = match down_status.take_values() {
            pkts if pkts.len() == 0 => {
                keep_alive.check_expired()?;
                down_status.is_disconnected();
            }
            pkts => {
                keep_alive.live();

                let status = self.handle_packets(shard, route_io, pkts)?;

                if let QueueStatus::Disconnected(_) = down_status {
                    error!("{} downstream-rx disconnect", self.prefix);
                    true
                } else if let QueueStatus::Disconnected(_) = status {
                    error!("{} downstream-tx disconnect, or a slow client", self.prefix);
                    true
                } else {
                    false
                }
            }
        }

        Ok(route_io)
    }

    // handle incoming packet, return Message::ClientAck, if any.
    // Disconnected
    // ProtocolError
    fn handle_packets(
        &mut self,
        shard: &mut Shard,
        route_io: &mut RouteIO,
        pkts: Vec<v5::Packet>,
    ) -> Result<()> {
        // NOTE: packets after DISCONNECT shall be ignored.
        for pkt in pkts.into_iter() {
            let args = (shard, route_io, pkt);
            match &pkt {
                v5::Packet::PingReq => self.rx_pingreq(args)?,
                v5::Packet::Subscribe(_) => self.rx_subscribe(args)?,
                v5::Packet::UnSubscribe(_) => self.rx_unsubscribe(args)?,
                v5::Packet::Publish(_) => self.rx_publish(args)?,
                v5::Packet::PubAck(_) => self.rx_puback(args)?,
                v5::Packet::PubRec(_) => self.rx_pubrec(args)?,
                v5::Packet::PubRel(_) => self.rx_pubrel(args)?,
                v5::Packet::PubComp(_) => self.rx_pubcomp(args)?,
                v5::Packet::Disconnect(_) => self.rx_disconnect(args)?,
                v5::Packet::Auth(_) => self.rx_auth(args)?,
                // Errors
                v5::Packet::Connect(_) => err_dup_connect(&self.prefix)?,
                v5::Packet::ConnAck(_) => err_invalid_pkt(&self.prefix, &args.2)?,
                v5::Packet::SubAck(_) => err_invalid_pkt(&self.prefix, &args.2)?,
                v5::Packet::UnsubAck(_) => err_invalid_pkt(&self.prefix, &args.2)?,
                v5::Packet::PingResp => err_invalid_pkt(&self.prefix, &args.2)?,
            }
        }

        Ok(())
    }

    fn rx_pingreq(&mut self, _: &mut Shard, route_io: &mut RouteIO) -> Result<()> {
        self.stats.n_pings += 1;
        trace!("{} received PingReq", self.prefix);
        route_io.oug_msgs.push(Message::new_ping_resp());
    }

    fn rx_subscribe(&mut self, (shard, route_io, pkt): HandleArgs) -> Result<()> {
        let server_qos = v5::QoS::try_from(self.config.mqtt_maximum_qos).unwrap();

        let sub = match pkt {
            v5::Packet::Subscribe(sub) => sub,
            _ => unreachable!(),
        };

        let subscription_id: Option<u32> = match &sub.properties {
            Some(props) => props.subscription_id.clone().map(|x| *x),
            None => None,
        };

        let mut return_codes = Vec::with_capacity(sub.filters.len());
        let mut retain_msgs = Vec::default();

        for filter in sub.filters.iter() {
            let (rfr, retain_as_published, no_local, qos) = filter.opt.unwrap();
            let subscription = v5::Subscription {
                topic_filter: filter.topic_filter.clone(),

                client_id: self.client_id.clone(),
                shard_id: shard.shard_id,
                subscription_id: subscription_id,
                qos: cmp::min(server_qos, qos),
                no_local,
                retain_as_published,
                retain_forward_rule: rfr,
            };

            shard
                .as_topic_filters()
                .subscribe(&filter.topic_filter, subscription.clone());

            let is_new_subscribe = self
                .state
                .as_mut_subscriptions()
                .insert(filter.topic_filter.clone(), subscription.clone())
                .is_none();

            let publs = match rfr {
                v5::RetainForwardRule::OnEverySubscribe => {
                    shard.as_retained_topics().match_topic_filter(&filter.topic_filter)
                }
                v5::RetainForwardRule::OnNewSubscribe if is_new_subscribe => {
                    shard.as_retained_topics().match_topic_filter(&filter.topic_filter)
                }
                v5::RetainForwardRule::OnNewSubscribe => Vec::default(),
                v5::RetainForwardRule::Never => Vec::default(),
            };
            retain_msgs.expand(publs.into_iter().map(|mut p| {
                p.retain = true;
                Message::new_retain_publish(p)
            }));

            let rc = match subscription.qos {
                v5::QoS::AtMostOnce => v5::SubAckReasonCode::QoS0,
                v5::QoS::AtLeastOnce => v5::SubAckReasonCode::QoS1,
                v5::QoS::ExactlyOnce => v5::SubAckReasonCode::QoS2,
            };
            return_codes.push(rc)
        }


        let sub_ack = v5::SubAck::from_sub(&sub, return_codes);
        retain_msgs.insert(0, Message::new_suback(sub_ack));

        route_io.oug_msgs.extend(retain_msgs.into_iter());

        Ok(())
    }

    fn rx_unsubscribe(&mut self, (shard, route_io, pkt): HandleArgs) -> Result<()> {
        let unsub = match pkt {
            v5::Packet::UnSubscribe(unsub) => unsub,
            _ => unreachable!(),
        };

        let mut return_codes = Vec::with_capacity(sub.filters.len());

        for topic_filter in unsub.filters.iter() {
            let subscription = v5::Subscription::from_filter(topic_filter);

            let res1 = shard
                .as_topic_filters()
                .unsubscribe(&topic_filter, &subscription);

            let res2 = self.state.as_mut_subscriptions().remove(&topic_filter);
            let code = match (&res1, &res2) {
                (Some(_), Some(_)) => UnsubAckReasonCode::Success,
                (None, None) => UnsubAckReasonCode::NoSubscriptionExisted,
                (Some(_), None) => {
                    let msg = format!("{} filter in trie, not in session", topic_filter);
                    err_inconsistent_subscription(&self.prefix, &msg)?;
                    UnsubAckReasonCode::UnspecifiedError
                }
                (None, Some(_)) => {
                    let msg = format!("{} filter in session, not in trie", topic_filter);
                    err_inconsistent_subscription(&self.prefix, &msg)?;
                    UnsubAckReasonCode::UnspecifiedError
                }
            };

            return_codes.push(code);
        }

        let unsub_ack = v5::UnsubAck::from_sub(&unsub, return_codes);
        route_io.oug_msgs.push(Message::new_unsuback(unsub_ack));

        Ok(())
    }

    fn rx_publish(&mut self, (shard, route_io, pkt): HandleArgs) -> Result<()> {
        let server_qos = v5::QoS:try_from(self.config.mqtt_maximum_qos).unwrap();

        let publish = match pkt {
            v5::Packet::Publish(publish) => publish,
            _ => unreachable!(),
        };

        if publish.qos > server_qos {
            err_unsup_qos(&self.prefix, publish.qos)?
        }

        if self.is_duplicate(&publish) {
            return Ok(());
        }
        self.book_qos(&publish)?;

        self.book_retain(shard, &publish)?;

        let inp_seqno = shard.incr_inp_seqno();
        let topic_name = self.publish_topic_name(&publish)?;

        let msg = Message::new_index(&self.client_id, inp_seqno, &publish);
        let ack_needed = msg.is_some();
        msg.map(|msg| route_io.oug_msgs.push(msg));

        for (target_id, (subscr, ids)) in shard.match_subscribers(&topic_name) {
            if subscr.no_local && target_id == self.client_id {
                trace!(
                    "{} topic:{:?} client_id:{:?} skipping as no_local",
                    self.prefix,
                    topic_name,
                    target_id
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

            route_io.oug_msgs.push(
                Message::new_routed(self, inp_seqno, publish, &subscr, ack_needed)
            );
        }

        match (publish.qos, subscrs.len()) {
            (v5::QoS::AtMostOnce, _) => (),
            (_, 0) => {
                let puback = v5::Pub::new_pub_ack(publish.packet_id.unwrap());
                route_io.oug_msgs.push(Message::new_pub_ack(puback));
            }
            (v5::QoS::AtLeastOnce, _) | (v5::QoS::ExactlyOnce, _) => (),
        }

        Ok(())
    }

    fn rx_puback(&mut self, (shard, route_io, pkt): HandleArgs) -> Result<()> {
        let _puback = match pkt {
            v5::Packet::PubAck(puback) => puback,
            _ => unreachable!(),
        };

        todo!()
    }

    fn rx_pubrec(&mut self, (shard, route_io, pkt): HandleArgs) -> Result<()> {
        let _pubrec = match pkt {
            v5::Packet::PubRec(pubrec) => pubrec,
            _ => unreachable!(),
        };

        todo!();
    }

    fn rx_pubrel(&mut self, (shard, route_io, pkt): HandleArgs) -> Result<()> {
        let _pubrel = match pkt {
            v5::Packet::PubRel(pubrel) => pubrel,
            _ => unreachable!(),
        };

        todo!();
    }

    fn rx_pubcomp(&mut self, (shard, route_io, pkt): HandleArgs) -> Result<()> {
        let _pubcomp = match pkt {
            v5::Packet::PubComp(pubcomp) => pubcomp,
            _ => unreachable!(),
        };

        todo!();
    }

    fn rx_disconnect(&mut self, (_shard, route_io, pkt): HandleArgs) -> Result<()> {
        use v5::client::DisconnReasonCode::*;

        let disconn = match pkt {
            v5::Packet::Disconnect(disconn) => disconn,
            _ => unreachable!(),
        };

        let code = match v5::client::DisconnReasonCode::try_from(disconn.code as u8)? {
            NormalDisconnect => v5::DisconnReasonCode::NormalDisconnect,
            DisconnectWillMessage => v5::DisconnReasonCode::NormalDisconnect,
            UnspecifiedError | MalformedPacket | ProtocolError | ImplementationError => {
                debug!("{} client disconnected code:{}", self.prefix, disconn.code);
                v5::DisconnReasonCode::NormalDisconnect
            }
            TopicNameInvalid | ExceededReceiveMaximum | TopicAliasInvalid => {
                debug!("{} client disconnected code:{}", self.prefix, disconn.code);
                v5::DisconnReasonCode::NormalDisconnect
            }
            PacketTooLarge | ExceedMessageRate | QuotaExceeded | AdminAction => {
                debug!("{} client disconnected code:{}", self.prefix, disconn.code);
                v5::DisconnReasonCode::NormalDisconnect
            }
            PayloadFormatInvalid => {
                debug!("{} client disconnected code:{}", self.prefix, disconn.code);
                v5::DisconnReasonCode::NormalDisconnect
            }
        };

        if let Some(props) = &disconn.properties {
            if let Some(txt) = &props.reason_string {
                debug!("{} disconnect with reason string {}", self.prefix, txt);
            }
        }

        let code = Some(ReasonCode::try_from(code as u8).unwrap())

        // TODO handle disconnect with will message
        err_disconnect(code)?;
    }

    fn rx_auth(&mut self, (_shard, route_io, pkt): HandleArgs) -> Result<()> {
        let auth = match pkt {
            v5::Packet::Auth(auth) => auth,
            _ => unreachable!(),
        };

        todo!()
    }

    fn book_retain(&mut self, _shard: &mut Shard, publish: &v5::Publish) -> Result<()> {
        if publish.retain && !self.config.mqtt_retain_available {
            err_unsup_retain(&self.prefix)?;
        } else if publish.retain {
            todo!()
        }

        Ok(())
    }

    fn publish_topic_name(&mut self, publ: &v5::Publish) -> Result<TopicName> {
        let (prefix, config, topic_aliases) = match &mut self.state {
            SessionState::Active { prefix, config, topic_aliases, .. } => {
                (prefix, config, topic_aliases)
            }
            ss => unreachable!("{:?}", ss),
        };

        let (topic_name, topic_alias) = (publ.as_topic_name(), publ.topic_alias());
        let server_alias_max = config.mqtt_topic_alias_max();

        let topic_name = match topic_alias {
            Some(_alias) if server_alias_max.is_none() => {
                err_unsup_topal(prefix)?;
                TopicName::default()
            }
            Some(alias) if alias > server_alias_max.unwrap() => {
                err_exceed_topal(prefix, alias, server_alias_max)?;
                TopicName::default()
            }
            Some(alias) if topic_name.len() > 0 => {
                if let Some(old) = topic_aliases.insert(alias, topic_name.clone()) {
                    dbg_replace_topal(prefix, alias, &old, topic_name);
                }
                topic_name.clone()
            }
            Some(alias) => match topic_aliases.get(&alias) {
                Some(topic_name) => topic_name.clone(),
                None => {
                    err_missing_alias(prefix, alias)?;
                    TopicName::default()
                }
            },
            None if topic_name.len() == 0 => {
                err_missing_topal(prefix)?;
                TopicName::default()
            }
            None => topic_name.clone(),
        };

        Ok(topic_name)
    }

    fn is_duplicate(&self, publish: &v5::Publish) -> bool {
        let (config, prefix, inc_qos12) = match &self.state {
            SessionState::Active { config, prefix, inc_qos12, .. } => {
                (config, prefix, inc_qos12)
            }
            ss => unreachable!("{:?}", ss),
        };

        match publish.qos {
            v5::QoS::AtMostOnce => false,
            v5::QoS::AtLeastOnce | v5::QoS::ExactlyOnce => {
                let packet_id = publish.packet_id.unwrap();
                matches!(inc_qos12.binary_search(&packet_id), Ok(_))
            }
        }
    }

    fn book_qos(&mut self, publish: &v5::Publish) -> Result<()> {
        let (prefix, inc_qos12) = match &mut self.state {
            SessionState::Active { prefix, inc_qos12, .. } => (prefix, inc_qos12),
            ss => unreachable!("{:?}", ss),
        };

        match publish.qos {
            v5::QoS::AtMostOnce => (),
            v5::QoS::AtLeastOnce | v5::QoS::ExactlyOnce => {
                let packet_id = publish.packet_id.unwrap();
                if let Err(off) = inc_qos12.binary_search(&packet_id) {
                    inc_qos12.insert(off, packet_id);
                } else {
                    // is_duplicate() would have filtered this path.
                    unreachable!()
                }
            }
        };

        Ok(())
    }
}

impl Session {
    pub fn incr_out_seqno(&mut self, msg: &mut Message) {
        self.state.incr_out_seqno(msg)
    }

    pub fn incr_packet_id(&mut self) {
        self.state.incr_packet_id();
    }

    // Handle PUBLISH QoS-0
    pub fn out_qos0(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
        self.state.out_qos0(msgs)
    }

    // Handle PUBLISH QoS-1 and QoS-2
    pub fn out_qos(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
        self.state.out_qos(msgs)
    }

    pub fn oug_acks_flush(&mut self) -> QueueStatus<v5::Packet> {
        let status = self.state.oug_acks_flush();
        status.map(vec![])
    }

    pub fn oug_acks_publish(&mut self, packet_id: PacketID) {
        self.state.oug_acks_publish(packet_id)
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
    pub fn as_connect(&self) -> &v5::Connect {
        match &self.state {
            SessionState::Active { connect, .. } => connect,
            ss => unreachable!("{} {:?}", self.prefix, ss),
        }
    }

    #[inline]
    pub fn as_mut_oug_acks(&mut self) -> &mut Vec<Message> {
        self.state.as_mut_oug_acks()
    }

    #[inline]
    pub fn as_mut_oug_back_log(&mut self) -> &mut Vec<Message> {
        self.state.as_mut_oug_back_log()
    }

    #[inline]
    pub fn is_active(&self) -> bool {
        matches!(&self.state, SessionState::Active { .. })
    }

    #[inline]
    pub fn is_replica(&self) -> bool {
        matches!(&self.state, SessionState::Replica { .. })
    }

    #[inline]
    fn to_keep_alive(&self) -> Option<u16> {
        match &self.state {
            SessionState::Active { keep_alive, .. } => keep_alive.keep_alive,
            ss => unreachable!("{} {:?}", self.prefix, ss),
        }
    }
}

pub enum SessionState {
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
        inc_qos12: Vec<PacketID>,
        // Message::ClientAck that needs to be sent to remote client.
        oug_acks: Vec<Message>,
        // Message::Retain outgoing QoS-0/1/2 Retain-PUBLISH messages.
        // Message::Packet outgoing QoS-0 PUBLISH messages.
        oug_back_log: Vec<Message>,
        // This index is a set of un-acked collection of inflight PUBLISH (QoS-1 & 2)
        // messages sent to subscribed clients. Entry is deleted from `oug_qos12`
        // when ACK is received for PacketID.
        //
        // Note that length of this collection is only as high as the allowed limit of
        // concurrent PUBLISH specified by client.
        oug_qos12: BTreeMap<PacketID, Message>,

        // This value is incremented for every out-going PUBLISH(qos>0).
        // If index.len() > `receive_maximum`, don't increment this value.
        next_packet_id: PacketID,
        /// Monotonically increasing `seqno`, starting from 1, that is bumped up for
        /// every outgoing publish packet.
        cs_out_seqno: OutSeqno,
        /// Message::Packet outgoing PUBLISH > QoS-0, first land here.
        ///
        /// Entries from this index are deleted after they are removed from
        /// `oug_qos12` and after they go through the consensus loop.
        cs_back_log: BTreeMap<OutSeqno, Message>,
    },
    #[allow(dead_code)]
    Replica {
        prefix: String,
        config: Config,

        /// Monotonically increasing `seqno`, starting from 1, that is bumped up for
        /// every outgoing publish packet.
        cs_out_seqno: OutSeqno,
        /// Message::Packet outgoing PUBLISH > QoS-0, first land here.
        ///
        /// Entries from this index are deleted after they are removed from
        /// `oug_qos12` and after they go through the consensus loop.
        cs_back_log: BTreeMap<OutSeqno, Message>,
    },
    #[allow(dead_code)]
    Reconnect {
        prefix: String,
        config: Config,

        // MQTT topic-aliases if enabled. ZERO is not allowed.
        topic_aliases: BTreeMap<u16, TopicName>,
        // List of topic-filters subscribed by this client, when ever
        // SUBSCRIBE/UNSUBSCRIBE messages are committed here, [Cluster::topic_filters]
        // will also be updated.
        subscriptions: BTreeMap<TopicFilter, v5::Subscription>,

        /// Monotonically increasing `seqno`, starting from 1, that is bumped up for
        /// every outgoing publish packet.
        cs_out_seqno: OutSeqno,
        /// Message::Packet outgoing PUBLISH > QoS-0, first land here.
        ///
        /// Entries from this index are deleted after they are removed from
        /// `oug_qos12` and after they go through the consensus loop.
        cs_back_log: BTreeMap<OutSeqno, Message>,
    },
    None,
}

impl fmt::Debug for SessionState {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            SessionState::Active { .. } => write!(f, "SessionState::Active"),
            SessionState::Reconnect { .. } => write!(f, "SessionState::Reconnect"),
            SessionState::Replica { .. } => write!(f, "SessionState::Replica"),
            SessionState::None { .. } => write!(f, "SessionState::None"),
        }
    }
}

impl SessionState {
    fn incr_out_seqno(&mut self, msg: &mut Message) {
        match self {
            SessionState::Active { cs_out_seqno, .. } => {
                let seqno = *cs_out_seqno;
                *cs_out_seqno = cs_out_seqno.saturating_add(1);
                match msg {
                    Message::Routed { out_seqno, .. } => *out_seqno = seqno,
                    _ => (),
                }
            }
            ss => unreachable!("{:?}", ss),
        }
    }

    fn incr_packet_id(&mut self) -> Option<PacketID> {
        let next_packet_id = match self {
            SessionState::Active { next_packet_id, .. } => next_packet_id,
            _ => unreachable!(),
        };
        let packet_id = *next_packet_id;
        *next_packet_id = next_packet_id.wrapping_add(1);
        Some(packet_id)
    }

    fn out_qos0(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
        let (prefix, config, miot_tx, oug_back_log) = match self {
            SessionState::Active { prefix, config, miot_tx, oug_back_log, .. } => {
                (prefix, config, miot_tx, oug_back_log)
            }
            ss => unreachable!("{:?}", ss),
        };

        let m = oug_back_log.len();
        // TODO: separate back-log limit from mqtt_pkt_batch_size.
        let n = (config.mqtt_pkt_batch_size as usize) * 4;
        if m > n {
            // TODO: if back-pressure is increasing due to a slow receiving client,
            // we will have to take drastic steps, like, closing this connection.
            error!("{} session.oug_back_log {} pressure > {}", prefix, m, n);
            return QueueStatus::Disconnected(Vec::new());
        }

        for msg in msgs.into_iter() {
            let msg = msg.into_packet(None);
            oug_back_log.push(msg)
        }

        let back_log = mem::replace(oug_back_log, vec![]);
        let mut status = flush_to_miot(prefix, miot_tx, back_log);
        let _empty = mem::replace(oug_back_log, status.take_values());

        status
    }

    fn out_qos(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
        match self {
            SessionState::Active { .. } => self.out_qos_active(msgs),
            SessionState::Replica { .. } => self.out_qos_replica(msgs),
            SessionState::Reconnect { .. } => QueueStatus::Ok(Vec::new()),
            ss => unreachable!("{:?}", ss),
        }
    }

    fn out_qos_active(&mut self, msgs: Vec<Message>) -> QueueMsg {
        let (prefix, config, oug_qos12, back_log) = match self {
            SessionState::Active {
                prefix, config, oug_qos12, cs_back_log, ..
            } => (prefix, config, oug_qos12, cs_back_log),
            ss => unreachable!("{:?}", ss),
        };
        let mqtt_pkt_batch_size = config.mqtt_pkt_batch_size;
        let mqtt_receive_maximum = config.mqtt_receive_maximum;

        let m = back_log.len();
        // TODO: separate back-log limit from mqtt_pkt_batch_size.
        let n = (mqtt_pkt_batch_size as usize) * 4;
        if m > n {
            // TODO: if back-pressure is increasing due to a slow receiving client,
            // we will have to take drastic steps, like, closing this connection.
            error!("{} session.back_log {} pressure > {}", prefix, m, n);
            return QueueStatus::Disconnected(Vec::new());
        }

        if oug_qos12.len() >= usize::from(mqtt_receive_maximum) {
            return QueueStatus::Block(Vec::new());
        }

        mem::drop(prefix);
        mem::drop(config);
        mem::drop(oug_qos12);
        mem::drop(back_log);

        for msg in msgs.into_iter() {
            let packet_id = self.incr_packet_id();
            let msg = msg.into_packet(packet_id);
            self.as_back_log().insert(msg.to_out_seqno(), msg);
        }

        let (prefix, miot_tx, oug_qos12, back_log) = match self {
            SessionState::Active {
                prefix, miot_tx, oug_qos12, cs_back_log, ..
            } => (prefix, miot_tx, oug_qos12, cs_back_log),
            ss => unreachable!("{:?}", ss),
        };

        let max = usize::try_from(mqtt_pkt_batch_size).unwrap();
        let mut msgs = Vec::default();
        while msgs.len() < max {
            match back_log.pop_first() {
                Some((_, msg)) => msgs.push(msg),
                None => break,
            }
        }
        for msg in msgs.clone().into_iter() {
            oug_qos12.insert(msg.to_packet_id(), msg);
        }

        let mut status = flush_to_miot(prefix, miot_tx, msgs);

        // re-insert, cleanup for remaining messages.
        for msg in status.take_values().into_iter() {
            let packet_id = msg.to_packet_id();
            back_log.insert(msg.to_out_seqno(), msg);
            oug_qos12.remove(&packet_id);
        }

        status
    }

    fn out_qos_replica(&mut self, msgs: Vec<Message>) -> QueueMsg {
        let back_log = match self {
            SessionState::Active { cs_back_log, .. } => cs_back_log,
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

    fn oug_acks_publish(&mut self, packet_id: PacketID) {
        match self {
            SessionState::Active { oug_acks, .. } => {
                oug_acks.push(Message::new_pub_ack(v5::Pub::new_pub_ack(packet_id)));
            }
            SessionState::Reconnect { .. } => (),
            ss => unreachable!("{:?}", ss),
        }
    }

    fn oug_acks_flush(&mut self) -> QueueStatus<Message> {
        let (prefix, miot_tx, inc_qos12, oug_acks) = match self {
            SessionState::Active { prefix, miot_tx, inc_qos12, oug_acks, .. } => {
                (prefix, miot_tx, inc_qos12, oug_acks)
            }
            ss => unreachable!("{:?}", ss),
        };

        for ack in oug_acks.iter() {
            match ack {
                Message::ClientAck { packet } => match packet {
                    v5::Packet::Publish(publish) => match publish.packet_id {
                        Some(packet_id) => match inc_qos12.binary_search(&packet_id) {
                            Ok(off) => {
                                inc_qos12.remove(off);
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
            let acks = mem::replace(oug_acks, Vec::default());
            flush_to_miot(prefix, miot_tx, acks)
        };
        let _empty = mem::replace(oug_acks, status.take_values());
        status
    }

    fn commit_acks(&mut self, out_seqnos: Vec<OutSeqno>) {
        let back_log = match self {
            SessionState::Active { cs_back_log, .. } => cs_back_log,
            SessionState::Replica { cs_back_log, .. } => cs_back_log,
            SessionState::Reconnect { cs_back_log, .. } => cs_back_log,
            ss => unreachable!("{:?}", ss),
        };
        for out_seqno in out_seqnos.into_iter() {
            back_log.remove(&out_seqno);
        }
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

    fn as_mut_oug_acks(&mut self) -> &mut Vec<Message> {
        match self {
            SessionState::Active { oug_acks, .. } => oug_acks,
            ss => unreachable!("{:?}", ss),
        }
    }

    fn as_mut_oug_back_log(&mut self) -> &mut Vec<Message> {
        match self {
            SessionState::Active { oug_back_log, .. } => oug_back_log,
            ss => unreachable!("{:?}", ss),
        }
    }

    fn as_back_log(&mut self) -> &mut BTreeMap<OutSeqno, Message> {
        match self {
            SessionState::Active { cs_back_log, .. } => cs_back_log,
            ss => unreachable!("{:?}", ss),
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

type PubMatches = BTreeMap<ClientID, (v5::Subscription, Vec<u32>)>;

// a. Only one message is sent to a client, even with multiple matches.
// b. subscr_qos is maximum of all matching-subscribption.
// c. final qos is min(server_qos, publish_qos, subscr_qos)
// d. retain if _any_ of the matching-subscription is calling for retain_as_published.
// e. no_local if _all_ of the matching-subscription is calling for no_local.
pub fn match_subscribers(shard: &Shard, topic_name: &TopicName) -> PubMatches {
    // group subscriptions based on client-id.
    let mut subscrs: PubMatches = BTreeMap::default();

    for subscr in shard.as_topic_filters().match_topic_name(topic_name).into_iter() {
        match subscrs.get_mut(&subscr.client_id) {
            Some((oldval, ids)) => {
                oldval.no_local &= subscr.no_local;
                oldval.retain_as_published |= subscr.retain_as_published;
                oldval.qos = cmp::max(oldval.qos, subscr.qos);
                if let Some(id) = subscr.subscription_id {
                    ids.push(id)
                }
            }
            None => {
                let ids = match subscr.subscription_id {
                    Some(id) => vec![id],
                    None => vec![],
                };
                subscrs.insert(subscr.client_id.clone(), (subscr, ids));
            }
        }
    }

    subscrs
}


fn err_disconnect(code: Option<ReasonCode>) -> Result<()> {
    Err(Error {
        kind: ErrorKind::Disconnected,
        code,
        loc: format!("{}:{}", file!(), line!()),
        ..Error::default()
    })
}

fn err_unsup_qos(prefix: &str, qos: v5::QoS) -> Result<()> {
    err!(
        ProtocolError,
        code: QoSNotSupported,
        "{} publish-qos exceeds server-qos {:?}",
        prefix,
        qos
    )
}

fn err_unsup_retain(prefix: &str) -> Result<()> {
    err!(ProtocolError, code: RetainNotSupported, "{} retain unavailable", prefix)
}

fn err_dup_connect(prefix: &str) -> Result<()> {
    err!(ProtocolError, code: ProtocolError, "{} duplicate connect packet", prefix)
}

fn err_invalid_pkt(prefix: &str, pkt: &v5::Packet) -> Result<()> {
    let pt = pkt.to_packet_type();
    err!(ProtocolError, code: ProtocolError, "{} unexpected packet type {:?}", prefix, pt)
}

fn err_unsup_topal(prefix: &str) -> Result<()> {
    err!(ProtocolError, code: TopicAliasInvalid, "{} not-supported by broker", prefix)
}

fn err_exceed_topal(prefix: &str, alias: u16, sam: Option<u16>) -> Result<()> {
    err!(
        ProtocolError,
        code: TopicAliasInvalid,
        "{} exceeds broker limit {} > {}",
        prefix,
        alias,
        sam.unwrap()
    )
}

fn err_missing_alias(prefix: &str, alias: u16) -> Result<()> {
    err!(ProtocolError, code: TopicAliasInvalid, "{} alias {} is missing", prefix, alias)
}

fn err_missing_topal(prefix: &str) -> Result<()> {
    err!(
        ProtocolError,
        code: TopicNameInvalid,
        "{} alias is ZERO and topic_name is empty",
        prefix
    )
}

fn err_inconsistent_subscription(prefix: &str, msg: &str) -> Result<()> {
    err!(
        Fatal,
        code: UnspecifiedError,
        "{} {}",
        prefix, msg
    )
}

fn dbg_replace_topal(prefix: &str, alias: u16, old: &TopicName, topic_name: &TopicName) {
    debug!(
        concat!("{} topic_alias:{} old_topic:{:?} new_topic:{:?}", "replacing ... "),
        prefix, alias, old, topic_name
    )
}
