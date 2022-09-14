use log::{debug, error, trace};

use std::ops::{Deref, DerefMut};
use std::{cmp, collections::BTreeMap, fmt, mem, net, result};

use crate::broker::{Config, InpSeqno, RetainedTrie, RouteIO, SubscribedTrie};
use crate::broker::{KeepAlive, Message, OutSeqno, PktRx, PktTx, QueueStatus};
use crate::broker::{SessionArgsActive, SessionArgsReplica};

use crate::{v5, ClientID, PacketID, TopicFilter, TopicName};
use crate::{Error, ErrorKind, ReasonCode, Result};

type QueueMsg = QueueStatus<Message>;
type HandleArgs<'a, S> = (&'a mut S, &'a mut RouteIO, v5::Packet);

pub trait Shard {
    fn to_shard_id(self) -> u32;

    fn as_topic_filters(&self) -> &SubscribedTrie;

    fn as_retained_topics(&self) -> &RetainedTrie;

    fn incr_inp_seqno(&mut self) -> InpSeqno;
}

////   Initial/None  --------> Active ----+---------> Reconnect
////          |                   ^       |              |
////          |                   |       |              |
////          |                   +-------+--------------+
////          |                   |                      |
////          |                   |                      |
////          +--------------> Replica <-----------------+
////
pub enum SessionState {
    Active(Active),
    Replica(Replica),
    Reconnect(Reconnect),
    None,
}

impl Default for SessionState {
    fn default() -> SessionState {
        SessionState::None
    }
}

impl fmt::Debug for SessionState {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            SessionState::Active(_) => write!(f, "SessionState::Active"),
            SessionState::Reconnect(_) => write!(f, "SessionState::Reconnect"),
            SessionState::Replica(_) => write!(f, "SessionState::Replica"),
            SessionState::None => write!(f, "SessionState::None"),
        }
    }
}

pub struct Common {
    prefix: String,
    config: Config,

    // MQTT topic-aliases if enabled. ZERO is not allowed.
    topic_aliases: BTreeMap<u16, TopicName>,

    /// List of topic-filters subscribed by this client, when ever
    /// SUBSCRIBE/UNSUBSCRIBE messages are committed here, [Cluster::topic_filters]
    /// will also be updated.
    cs_subscriptions: BTreeMap<TopicFilter, v5::Subscription>,
    /// Monotonically increasing `seqno`, starting from 1, that is bumped up for
    /// every outgoing publish packet.
    cs_oug_seqno: OutSeqno,
    /// Message::Packet outgoing PUBLISH > QoS-1/2, first land here.
    ///
    /// Entries from this index are deleted after they are removed from
    /// `oug_retry_qos12` and after they go through the consensus loop.
    cs_oug_back_log: Vec<Message>,
}

impl Default for Common {
    fn default() -> Common {
        Common {
            prefix: String::default(),
            config: Config::default(),
            topic_aliases: BTreeMap::default(),
            cs_subscriptions: BTreeMap::default(),
            cs_oug_seqno: 1,
            cs_oug_back_log: Vec::default(),
        }
    }
}

pub struct Active {
    common: Common,

    // Immutable set of parameters for this session, after handshake.
    keep_alive: KeepAlive, // Negotiated keep-alive.
    connect: v5::Connect,  // Connect msg that created this session.
    miot_tx: PktTx,        // Outbound channel to Miot thread.
    session_rx: PktRx,     // Inbound channel from Miot thread.

    // Sorted list of QoS-1 & QoS-2, SUB, UNSUB packet_id for managing incoming
    // duplicate publish and/or  Subscribe and UnSubscribe packets
    inc_packet_ids: Vec<PacketID>,
    // Message::ClientAck that needs to be sent to remote client.
    oug_acks: Vec<Message>,
    // Message::Packet outgoing QoS-0 PUBLISH messages.
    // Message::Packet outgoing QoS-0 Retain messages.
    oug_back_log: Vec<Message>,
    // This value is incremented for every out-going PUBLISH(qos>0).
    // If index.len() > `receive_maximum`, don't increment this value.
    next_packet_id: PacketID,
    // Message::Packet outgoing QoS-1/2 PUBLISH messages.
    // Message::Packet outgoing QoS-1/2 Retain messages.
    //
    // This index is a set of un-acked collection of inflight PUBLISH (QoS-1 & 2)
    // messages sent to subscribed clients. Entry is deleted from `oug_retry_qos12`
    // when ACK is received for PacketID.
    //
    // Note that length of this collection is only as high as the allowed limit of
    // concurrent PUBLISH specified by client.
    oug_retry_qos12: BTreeMap<PacketID, Message>,
}

impl From<SessionArgsActive> for Active {
    fn from(args: SessionArgsActive) -> Active {
        let keep_alive = KeepAlive::new(&args);
        Active {
            common: Common { config: args.config, ..Common::default() },

            keep_alive,
            connect: args.connect,
            miot_tx: args.miot_tx,
            session_rx: args.session_rx,

            inc_packet_ids: Vec::default(),
            oug_acks: Vec::default(),
            oug_back_log: Vec::default(),
            next_packet_id: 1,
            oug_retry_qos12: BTreeMap::default(),
        }
    }
}

impl Deref for Active {
    type Target = Common;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

impl DerefMut for Active {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.common
    }
}

pub struct Replica {
    common: Common,
}

impl From<SessionArgsReplica> for Replica {
    fn from(args: SessionArgsReplica) -> Replica {
        Replica {
            common: Common { config: args.config, ..Common::default() },
        }
    }
}
impl Deref for Replica {
    type Target = Common;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

impl DerefMut for Replica {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.common
    }
}

pub struct Reconnect {
    common: Common,
}

impl Deref for Reconnect {
    type Target = Common;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

impl DerefMut for Reconnect {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.common
    }
}

/// Type implement the session for every connected client.
///
/// Sessions are hosted within the shards and shards are hosted within the nodes.
pub struct Session {
    /// Remote socket address.
    pub prefix: String,
    /// Broker Configuration.
    pub config: Config,
    /// Remote address,
    pub raddr: net::SocketAddr,
    /// Client's ClientID that created this session.
    pub client_id: ClientID,
    /// Shard hosting this session.
    pub shard_id: u32,

    pub state: SessionState,
    pub stats: Stats,
}

impl Default for Session {
    fn default() -> Session {
        Session {
            prefix: String::default(),
            config: Config::default(),
            raddr: "0.0.0.0:0".parse().unwrap(),
            client_id: ClientID::default(),
            shard_id: u32::default(),

            state: SessionState::default(),
            stats: Stats::default(),
        }
    }
}

#[derive(Clone, Default)]
pub struct Stats {
    n_pings: usize,
}

impl Session {
    pub fn into_active(self, args: SessionArgsActive) -> Session {
        let prefix = format!("session:active:{}", args.raddr);
        let config = args.config.clone();
        let client_id = args.client_id.clone();
        let raddr = args.raddr;
        let shard_id = args.shard_id;

        let mut active: Active = args.into();
        active.prefix = prefix.clone();

        let state = match self.state {
            SessionState::None => SessionState::Active(active),
            SessionState::Active(active) => SessionState::Active(active),
            SessionState::Replica(mut replica) => {
                active.cs_subscriptions =
                    mem::replace(&mut replica.cs_subscriptions, BTreeMap::default());
                active.cs_oug_seqno = replica.cs_oug_seqno;
                active.cs_oug_back_log =
                    mem::replace(&mut replica.cs_oug_back_log, Vec::default());
                SessionState::Active(active)
            }
            SessionState::Reconnect(mut reconnect) => {
                active.topic_aliases =
                    mem::replace(&mut reconnect.topic_aliases, BTreeMap::default());
                active.cs_subscriptions =
                    mem::replace(&mut reconnect.cs_subscriptions, BTreeMap::default());
                active.cs_oug_seqno = reconnect.cs_oug_seqno;
                active.cs_oug_back_log =
                    mem::replace(&mut reconnect.cs_oug_back_log, Vec::default());
                SessionState::Active(active)
            }
        };

        Session {
            prefix,
            config,
            client_id,
            raddr,
            shard_id,

            state,
            stats: Stats::default(),
        }
    }

    pub fn into_replica(self, args: SessionArgsReplica) -> Session {
        let prefix = format!("session:replica:{}", args.raddr);
        let config = args.config.clone();
        let client_id = args.client_id.clone();
        let raddr = args.raddr;
        let shard_id = args.shard_id;

        let mut replica: Replica = args.into();
        replica.prefix = prefix.clone();

        let state = match self.state {
            SessionState::None => SessionState::Replica(replica),
            SessionState::Reconnect(mut reconnect) => {
                replica.topic_aliases =
                    mem::replace(&mut reconnect.topic_aliases, BTreeMap::default());
                replica.cs_subscriptions =
                    mem::replace(&mut reconnect.cs_subscriptions, BTreeMap::default());
                replica.cs_oug_seqno = reconnect.cs_oug_seqno;
                replica.cs_oug_back_log =
                    mem::replace(&mut reconnect.cs_oug_back_log, Vec::default());
                SessionState::Replica(replica)
            }
            ss => unreachable!("{:?}", ss),
        };

        Session {
            prefix,
            config,
            client_id,
            raddr,
            shard_id,

            state,
            stats: Stats::default(),
        }
    }

    pub fn into_reconnect(self) -> Session {
        let prefix = format!("session:reconnect:{}", self.raddr);
        let state = match self.state {
            SessionState::Active(mut active) => {
                let reconnect = Reconnect {
                    common: Common {
                        prefix: prefix.clone(),
                        config: active.config.clone(),
                        topic_aliases: mem::replace(
                            &mut active.topic_aliases,
                            BTreeMap::default(),
                        ),
                        cs_subscriptions: mem::replace(
                            &mut active.cs_subscriptions,
                            BTreeMap::default(),
                        ),
                        cs_oug_seqno: active.cs_oug_seqno,
                        cs_oug_back_log: mem::replace(
                            &mut active.cs_oug_back_log,
                            Vec::default(),
                        ),
                    },
                };
                SessionState::Reconnect(reconnect)
            }
            ss => unreachable!("{:?}", ss),
        };

        Session {
            prefix,
            config: self.config.clone(),
            client_id: self.client_id,
            raddr: self.raddr,
            shard_id: self.shard_id,

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
    #[inline]
    pub fn as_connect(&self) -> &v5::Connect {
        match &self.state {
            SessionState::Active(active) => &active.connect,
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
        matches!(&self.state, SessionState::Active(_))
    }

    #[inline]
    pub fn is_replica(&self) -> bool {
        matches!(&self.state, SessionState::Replica(_))
    }

    #[inline]
    fn to_keep_alive(&self) -> Option<u16> {
        match &self.state {
            SessionState::Active(active) => active.keep_alive.keep_alive,
            ss => unreachable!("{} {:?}", self.prefix, ss),
        }
    }
}

impl Session {
    pub fn remove_topic_filters(&mut self, filters: &mut SubscribedTrie) {
        match &self.state {
            SessionState::Active(active) => {
                for (topic_filter, value) in active.cs_subscriptions.iter() {
                    filters.unsubscribe(topic_filter, value);
                }
            }
            ss => unreachable!("{:?}", ss),
        }
    }
}

// handle incoming packets.
impl Session {
    // Returned RouteIO has following message types:
    // * Message::ClientAck carrying PingResp.
    // * Message::Subscribe
    // * Message::Retain
    // * Message::UnSubscribe
    // * Message::ShardIndex to be indexed by shard for QoS > 0, managing ack.
    // * Message::Routed publish msg one of each subscribing client.
    // * Message::ClientAck carrying PubAck for QoS1, with 0 subscripers for topic.
    pub fn route_packets<S>(&mut self, shard: &mut S, rio: &mut RouteIO) -> Result<()>
    where
        S: Shard,
    {
        let active = match &mut self.state {
            SessionState::Active(active) => active,
            ss => unreachable!("{} {:?}", self.prefix, ss),
        };

        let mut status = active.session_rx.try_recvs(&self.prefix);

        rio.disconnected = match status.take_values() {
            pkts if pkts.len() == 0 => {
                active.keep_alive.check_expired()?;
                status.is_disconnected()
            }
            pkts => {
                active.keep_alive.live();

                for pkt in pkts.into_iter() {
                    self.handle_packet(shard, rio, pkt)?;
                }

                if let QueueStatus::Disconnected(_) = status {
                    error!("{} downstream-rx disconnect", self.prefix);
                    true
                } else {
                    false
                }
            }
        };

        Ok(())
    }

    // Disconnected
    // ProtocolError
    fn handle_packet<S>(
        &mut self,
        s: &mut S,
        r: &mut RouteIO,
        p: v5::Packet,
    ) -> Result<()>
    where
        S: Shard,
    {
        match &p {
            v5::Packet::PingReq => self.rx_pingreq(r)?,
            v5::Packet::Subscribe(_) => self.rx_subscribe((s, r, p))?,
            v5::Packet::UnSubscribe(_) => self.rx_unsubscribe((s, r, p))?,
            v5::Packet::Publish(_) => self.rx_publish((s, r, p))?,
            v5::Packet::PubAck(_) => self.rx_puback((s, r, p))?,
            v5::Packet::PubRec(_) => self.rx_pubrec((s, r, p))?,
            v5::Packet::PubRel(_) => self.rx_pubrel((s, r, p))?,
            v5::Packet::PubComp(_) => self.rx_pubcomp((s, r, p))?,
            v5::Packet::Disconnect(_) => self.rx_disconnect((s, r, p))?,
            v5::Packet::Auth(_) => self.rx_auth((s, r, p))?,
            // Errors
            v5::Packet::Connect(_) => err_dup_connect(&self.prefix)?,
            v5::Packet::ConnAck(_) => err_invalid_pkt(&self.prefix, &p)?,
            v5::Packet::SubAck(_) => err_invalid_pkt(&self.prefix, &p)?,
            v5::Packet::UnsubAck(_) => err_invalid_pkt(&self.prefix, &p)?,
            v5::Packet::PingResp => err_invalid_pkt(&self.prefix, &p)?,
        }

        Ok(())
    }

    fn rx_pingreq(&mut self, route_io: &mut RouteIO) -> Result<()> {
        self.stats.n_pings += 1;
        trace!("{} received PingReq", self.prefix);
        route_io.oug_msgs.push(Message::new_ping_resp());

        Ok(())
    }

    fn rx_subscribe<S>(&mut self, (shard, route_io, pkt): HandleArgs<S>) -> Result<()>
    where
        S: Shard,
    {
        if self.is_duplicate(&pkt) {
            return Ok(());
        }
        self.book_packet_id(&pkt)?;

        let sub = match pkt {
            v5::Packet::Subscribe(sub) => sub,
            _ => unreachable!(),
        };

        let mut retain_msgs = vec![Message::new_sub(sub.clone())];

        for filter in sub.filters.iter() {
            let (rfr, _retain_as_published, _no_local, _qos) = filter.opt.unwrap();

            let is_new_subscribe =
                self.state.as_mut_subscriptions().get(&filter.topic_filter).is_none();

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

            retain_msgs.extend(publs.into_iter().map(|mut p| {
                p.retain = true;
                Message::new_retain_publish(p)
            }));
        }

        route_io.oug_msgs.extend(retain_msgs.into_iter());

        Ok(())
    }

    fn rx_unsubscribe<S>(&mut self, (_s, route_io, pkt): HandleArgs<S>) -> Result<()>
    where
        S: Shard,
    {
        if self.is_duplicate(&pkt) {
            return Ok(());
        }
        self.book_packet_id(&pkt)?;

        let unsub = match pkt {
            v5::Packet::UnSubscribe(unsub) => unsub,
            _ => unreachable!(),
        };
        route_io.oug_msgs.push(Message::new_unsub(unsub));

        Ok(())
    }

    fn rx_publish<S>(&mut self, (shard, route_io, pkt): HandleArgs<S>) -> Result<()>
    where
        S: Shard,
    {
        if self.is_duplicate(&pkt) {
            return Ok(());
        }
        self.book_packet_id(&pkt)?;

        let server_qos = v5::QoS::try_from(self.config.mqtt_maximum_qos).unwrap();

        let publish = match pkt {
            v5::Packet::Publish(publish) => publish,
            _ => unreachable!(),
        };

        if publish.qos > server_qos {
            err_unsup_qos(&self.prefix, publish.qos)?
        }

        self.book_retain(&publish)?;

        let server_qos = v5::QoS::try_from(self.config.mqtt_maximum_qos).unwrap();
        let inp_seqno = shard.incr_inp_seqno();
        let topic_name = self.publish_topic_name(&publish)?;

        let ack_needed = match publish.is_qos12() {
            true => {
                route_io.oug_msgs.push(
                    Message::new_index(&self.client_id, inp_seqno, &publish).unwrap(),
                );
                true
            }
            false => false,
        };

        let mut n_subscrs = 0;
        let publ_qos = cmp::min(server_qos, publish.qos);
        for (target_id, (subscr, ids)) in match_subscribers(shard, &topic_name) {
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
                publish.set_fixed_header(retain, cmp::min(publ_qos, subscr.qos), false);
                publish.set_subscription_ids(ids);
                publish
            };

            route_io.oug_msgs.push(Message::Routed {
                // new_routed
                src_shard_id: self.shard_id,
                dst_shard_id: subscr.shard_id,
                client_id: subscr.client_id.clone(),
                inp_seqno,
                publish,
                ack_needed,
            });

            n_subscrs += 1;
        }

        match (publish.qos, n_subscrs) {
            (v5::QoS::AtMostOnce, _) => (),
            (v5::QoS::AtLeastOnce, 0) => {
                let puback = v5::Pub::new_pub_ack(publish.packet_id.unwrap());
                route_io.oug_msgs.push(Message::new_pub_ack(puback));
            }
            (v5::QoS::AtLeastOnce, _) => (),
            (v5::QoS::ExactlyOnce, _) => todo!(),
        }

        Ok(())
    }

    fn rx_puback<S>(&mut self, (_s, _route_io, pkt): HandleArgs<S>) -> Result<()>
    where
        S: Shard,
    {
        let _puback = match pkt {
            v5::Packet::PubAck(puback) => puback,
            _ => unreachable!(),
        };

        todo!()
    }

    fn rx_pubrec<S>(&mut self, (_s, _route_io, pkt): HandleArgs<S>) -> Result<()>
    where
        S: Shard,
    {
        let _pubrec = match pkt {
            v5::Packet::PubRec(pubrec) => pubrec,
            _ => unreachable!(),
        };

        todo!();
    }

    fn rx_pubrel<S>(&mut self, (_s, _route_io, pkt): HandleArgs<S>) -> Result<()>
    where
        S: Shard,
    {
        let _pubrel = match pkt {
            v5::Packet::PubRel(pubrel) => pubrel,
            _ => unreachable!(),
        };

        todo!();
    }

    fn rx_pubcomp<S>(&mut self, (_s, _route_io, pkt): HandleArgs<S>) -> Result<()>
    where
        S: Shard,
    {
        let _pubcomp = match pkt {
            v5::Packet::PubComp(pubcomp) => pubcomp,
            _ => unreachable!(),
        };

        todo!();
    }

    fn rx_disconnect<S>(&mut self, (_shard, _route_io, pkt): HandleArgs<S>) -> Result<()>
    where
        S: Shard,
    {
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

        let code = Some(ReasonCode::try_from(code as u8).unwrap());

        err_disconnect(code)
    }

    fn rx_auth<S>(&mut self, (_shard, _route_io, pkt): HandleArgs<S>) -> Result<()>
    where
        S: Shard,
    {
        let _auth = match pkt {
            v5::Packet::Auth(auth) => auth,
            _ => unreachable!(),
        };

        todo!()
    }

    fn is_duplicate(&self, pkt: &v5::Packet) -> bool {
        let active = match &self.state {
            SessionState::Active(active) => active,
            ss => unreachable!("{:?}", ss),
        };

        match pkt {
            v5::Packet::Publish(publish) => match publish.qos {
                v5::QoS::AtMostOnce => false,
                v5::QoS::AtLeastOnce | v5::QoS::ExactlyOnce => {
                    let packet_id = publish.packet_id.unwrap();
                    matches!(active.inc_packet_ids.binary_search(&packet_id), Ok(_))
                }
            },
            v5::Packet::Subscribe(sub) => {
                let packet_id = sub.packet_id;
                matches!(active.inc_packet_ids.binary_search(&packet_id), Ok(_))
            }
            v5::Packet::UnSubscribe(unsub) => {
                let packet_id = unsub.packet_id;
                matches!(active.inc_packet_ids.binary_search(&packet_id), Ok(_))
            }
            pkt => unreachable!("{:?} packet type", pkt.to_packet_type()),
        }
    }

    fn book_packet_id(&mut self, pkt: &v5::Packet) -> Result<()> {
        let active = match &mut self.state {
            SessionState::Active(active) => active,
            ss => unreachable!("{:?}", ss),
        };

        match pkt {
            v5::Packet::Publish(publish) => match publish.qos {
                v5::QoS::AtMostOnce => (),
                v5::QoS::AtLeastOnce | v5::QoS::ExactlyOnce => {
                    let packet_id = publish.packet_id.unwrap();
                    if let Err(off) = active.inc_packet_ids.binary_search(&packet_id) {
                        active.inc_packet_ids.insert(off, packet_id);
                    } else {
                        unreachable!() // is_duplicate() would have filtered this path.
                    }
                }
            },
            v5::Packet::Subscribe(sub) => {
                if let Err(off) = active.inc_packet_ids.binary_search(&sub.packet_id) {
                    active.inc_packet_ids.insert(off, sub.packet_id);
                } else {
                    unreachable!() // is_duplicate() would have filtered this path.
                }
            }
            v5::Packet::UnSubscribe(unsub) => {
                if let Err(off) = active.inc_packet_ids.binary_search(&unsub.packet_id) {
                    active.inc_packet_ids.insert(off, unsub.packet_id);
                } else {
                    unreachable!() // is_duplicate() would have filtered this path.
                }
            }
            pkt => unreachable!("{:?} packet type", pkt.to_packet_type()),
        }

        Ok(())
    }

    fn book_retain(&mut self, publish: &v5::Publish) -> Result<()> {
        if publish.retain && !self.config.mqtt_retain_available {
            err_unsup_retain(&self.prefix)?;
        } else if publish.retain {
            todo!()
        }

        Ok(())
    }

    fn publish_topic_name(&mut self, publish: &v5::Publish) -> Result<TopicName> {
        let active = match &mut self.state {
            SessionState::Active(active) => active,
            ss => unreachable!("{:?}", ss),
        };

        let topic_name = publish.as_topic_name();
        let topic_alias = publish.topic_alias();
        let server_alias_max = active.config.mqtt_topic_alias_max();

        let topic_name = match topic_alias {
            Some(_alias) if server_alias_max.is_none() => {
                err_unsup_topal(&active.prefix)?;
                TopicName::default()
            }
            Some(alias) if alias > server_alias_max.unwrap() => {
                err_exceed_topal(&active.prefix, alias, server_alias_max)?;
                TopicName::default()
            }
            Some(alias) if topic_name.len() > 0 => {
                let name = topic_name.clone();
                if let Some(old) = active.topic_aliases.insert(alias, name) {
                    dbg_replace_topal(&active.prefix, alias, &old, topic_name);
                }
                topic_name.clone()
            }
            Some(alias) => match active.topic_aliases.get(&alias) {
                Some(topic_name) => topic_name.clone(),
                None => {
                    err_missing_alias(&active.prefix, alias)?;
                    TopicName::default()
                }
            },
            None if topic_name.len() == 0 => {
                err_missing_topal(&active.prefix)?;
                TopicName::default()
            }
            None => topic_name.clone(),
        };

        Ok(topic_name)
    }
}

impl Session {
    #[inline]
    pub fn incr_oug_qos0(&mut self) -> OutSeqno {
        self.state.incr_oug_qos0()
    }

    #[inline]
    pub fn incr_oug_qos12(&mut self) -> (OutSeqno, PacketID) {
        self.state.incr_oug_qos12()
    }

    pub fn tx_oug_acks(&mut self, msgs: Vec<Message>) -> QueueMsg {
        self.state.tx_oug_acks(msgs)
    }

    pub fn tx_oug_back_log(&mut self, msgs: Vec<Message>) -> QueueMsg {
        self.state.tx_oug_back_log(msgs)
    }

    pub fn commit_subs<S>(&mut self, s: &mut S, msgs: Vec<Message>) -> Result<QueueMsg>
    where
        S: Shard,
    {
        let mut ack_msgs = Vec::default();
        for msg in msgs.into_iter() {
            ack_msgs.push(self.commit_sub(s, msg)?);
        }
        // ack_msgs is Message::ClientAck carrying sub-ack
        Ok(self.state.tx_oug_acks(ack_msgs))
    }

    fn commit_sub<S>(&mut self, shard: &mut S, msg: Message) -> Result<Message>
    where
        S: Shard,
    {
        let server_qos = v5::QoS::try_from(self.config.mqtt_maximum_qos).unwrap();

        let sub = match msg {
            Message::Subscribe { sub } => sub,
            _ => unreachable!(),
        };

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
                shard_id: self.shard_id,
                subscription_id: subscription_id,
                qos: cmp::min(server_qos, qos),
                no_local,
                retain_as_published,
                retain_forward_rule: rfr,
            };

            let args = (filter.topic_filter.clone(), &subscription);
            self.state.commit_sub(shard, args)?;

            let rc = match subscription.qos {
                v5::QoS::AtMostOnce => v5::SubAckReasonCode::QoS0,
                v5::QoS::AtLeastOnce => v5::SubAckReasonCode::QoS1,
                v5::QoS::ExactlyOnce => v5::SubAckReasonCode::QoS2,
            };
            return_codes.push(rc)
        }

        match &self.state {
            SessionState::Active(_) => {
                Ok(Message::new_suback(v5::SubAck::from_sub(&sub, return_codes)))
            }
            ss => unreachable!("{:?}", ss),
        }
    }

    pub fn commit_unsubs<S>(&mut self, s: &mut S, msgs: Vec<Message>) -> Result<QueueMsg>
    where
        S: Shard,
    {
        let mut ack_msgs = Vec::default();
        for msg in msgs.into_iter() {
            ack_msgs.push(self.commit_unsub(s, msg)?)
        }
        // ack_msgs is Message::ClientAck carrying unsub-ack
        Ok(self.state.tx_oug_acks(ack_msgs))
    }

    fn commit_unsub<S>(&mut self, shard: &mut S, msg: Message) -> Result<Message>
    where
        S: Shard,
    {
        let unsub = match msg {
            Message::UnSubscribe { unsub } => unsub,
            _ => unreachable!(),
        };

        let mut rcodes = Vec::with_capacity(unsub.filters.len());

        for filter in unsub.filters.iter() {
            let mut subscription = v5::Subscription::default();
            subscription.topic_filter = filter.clone();
            subscription.client_id = self.client_id.clone();

            let code = self.state.commit_unsub(shard, subscription)?;
            rcodes.push(code);
        }

        match &self.state {
            SessionState::Active(_) => {
                Ok(Message::new_unsuback(v5::UnsubAck::from_unsub(&unsub, rcodes)))
            }
            ss => unreachable!("{:?}", ss),
        }
    }

    pub fn commit_cs_oug_back_log(&mut self, msgs: Vec<Message>) -> Result<QueueMsg> {
        self.state.commit_cs_oug_back_log(msgs)
    }

    pub fn book_oug_qos12(&mut self, msgs: Vec<Message>) -> Result<()> {
        self.state.book_oug_qos12(msgs)
    }

    //    pub fn oug_qos12(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
    //        self.state.oug_qos12(msgs)
    //    }
    //
    //    pub fn commit_acks(&mut self, out_seqnos: Vec<OutSeqno>) {
    //        self.state.commit_acks(out_seqnos)
    //    }
    //
    //    pub fn oug_acks_flush(&mut self) -> QueueStatus<v5::Packet> {
    //        let status = self.state.oug_acks_flush();
    //        status.map(vec![])
    //    }
    //
    //    pub fn oug_acks_publish(&mut self, packet_id: PacketID) {
    //        self.state.oug_acks_publish(packet_id)
    //    }
}

type HandleSubArgs<'a> = (TopicFilter, &'a v5::Subscription);

impl SessionState {
    pub fn incr_oug_qos0(&mut self) -> OutSeqno {
        match self {
            SessionState::Active(active) => {
                let seqno = active.cs_oug_seqno;
                active.cs_oug_seqno = seqno.saturating_add(1);
                seqno
            }
            ss => unreachable!("{:?}", ss),
        }
    }

    pub fn incr_oug_qos12(&mut self) -> (OutSeqno, PacketID) {
        match self {
            SessionState::Active(active) => {
                let seqno = active.cs_oug_seqno;
                active.cs_oug_seqno = seqno.saturating_add(1);
                let packet_id = active.next_packet_id;
                active.next_packet_id = packet_id.wrapping_add(1);
                (seqno, packet_id)
            }
            _ => unreachable!(),
        }
    }

    fn tx_oug_acks(&mut self, msgs: Vec<Message>) -> QueueMsg {
        let active = match self {
            SessionState::Active(active) => active,
            ss => unreachable!("{:?}", ss),
        };

        // TODO: separate back-log limit from mqtt_pkt_batch_size.
        let m = active.oug_acks.len();
        let n = (active.config.mqtt_pkt_batch_size as usize) * 4;
        if m > n {
            // TODO: if back-pressure is increasing due to a slow receiving client,
            // we will have to take drastic steps, like, closing this connection.
            error!("{} oug_acks {} pressure > {}", active.prefix, m, n);
            return QueueStatus::Disconnected(Vec::new());
        }

        active.oug_acks.extend(msgs.into_iter());

        let oug_acks = mem::replace(&mut active.oug_acks, vec![]);
        let (ids, mut status) = flush_to_miot(&active.prefix, &active.miot_tx, oug_acks);
        let _empty = mem::replace(&mut active.oug_acks, status.take_values());

        for packet_id in ids.into_iter() {
            match active.inc_packet_ids.binary_search(&packet_id) {
                Ok(off) => {
                    active.inc_packet_ids.remove(off);
                }
                Err(_off) => {
                    error!("{} packet_id:{} not booked", active.prefix, packet_id)
                }
            }
        }

        status
    }

    fn tx_oug_back_log(&mut self, msgs: Vec<Message>) -> QueueMsg {
        let active = match self {
            SessionState::Active(active) => active,
            ss => unreachable!("{:?}", ss),
        };

        // TODO: separate back-log limit from mqtt_pkt_batch_size.
        let m = active.oug_back_log.len();
        let n = (active.config.mqtt_pkt_batch_size as usize) * 4;
        if m > n {
            // TODO: if back-pressure is increasing due to a slow receiving client,
            // we will have to take drastic steps, like, closing this connection.
            error!("{} oug_back_log {} pressure > {}", active.prefix, m, n);
            return QueueStatus::Disconnected(Vec::new());
        }

        active.oug_back_log.extend(msgs.into_iter());

        let oug_back_log = mem::replace(&mut active.oug_back_log, vec![]);
        let (ids, mut status) =
            flush_to_miot(&active.prefix, &active.miot_tx, oug_back_log);
        let _empty = mem::replace(&mut active.oug_back_log, status.take_values());

        debug_assert!(ids.len() == 0);

        status
    }

    fn commit_sub<'a, S>(&mut self, shard: &mut S, args: HandleSubArgs<'a>) -> Result<()>
    where
        S: Shard,
    {
        let (topic_filter, subscr) = args;

        match self {
            SessionState::Active(active) => {
                active.cs_subscriptions.insert(topic_filter.clone(), subscr.clone());
                match shard.as_topic_filters().subscribe(&topic_filter, subscr.clone()) {
                    Some(old_subscr) => trace!(
                        "{} topic_filter:{:?} client_id:{:?}",
                        active.prefix,
                        topic_filter,
                        old_subscr.client_id
                    ),
                    None => (),
                }
            }
            SessionState::Replica(replica) => {
                replica.cs_subscriptions.insert(topic_filter.clone(), subscr.clone());
            }
            ss => unreachable!("{:?}", ss),
        }

        Ok(())
    }

    fn commit_unsub<'a, S>(
        &mut self,
        shard: &mut S,
        subr: v5::Subscription,
    ) -> Result<v5::UnsubAckReasonCode>
    where
        S: Shard,
    {
        use v5::UnsubAckReasonCode;

        let filter = &subr.topic_filter;
        let code = match self {
            SessionState::Active(active) => {
                let res1 = shard.as_topic_filters().unsubscribe(filter, &subr);
                let res2 = active.cs_subscriptions.remove(filter);
                match (&res1, &res2) {
                    (Some(_), Some(_)) => UnsubAckReasonCode::Success,
                    (None, None) => UnsubAckReasonCode::NoSubscriptionExisted,
                    (Some(_), None) => {
                        let msg = format!("{:?} filter in trie, not in session", filter);
                        err_inconsistent_subscription(&active.prefix, &msg)?;
                        UnsubAckReasonCode::UnspecifiedError
                    }
                    (None, Some(_)) => {
                        let msg = format!("{:?} filter in session, not in trie", filter);
                        err_inconsistent_subscription(&active.prefix, &msg)?;
                        UnsubAckReasonCode::UnspecifiedError
                    }
                }
            }
            SessionState::Replica(replica) => {
                replica.cs_subscriptions.remove(filter);
                UnsubAckReasonCode::Success
            }
            ss => unreachable!("{:?}", ss),
        };

        Ok(code)
    }

    fn commit_cs_oug_back_log(&mut self, msgs: Vec<Message>) -> Result<QueueMsg> {
        let active = match self {
            SessionState::Active(active) => active,
            ss => unreachable!("{:?}", ss),
        };

        // TODO: separate back-log limit from mqtt_pkt_batch_size.
        let m = active.cs_oug_back_log.len();
        let n = (active.config.mqtt_pkt_batch_size as usize) * 4;
        if m > n {
            // TODO: if back-pressure is increasing due to a slow receiving client,
            // we will have to take drastic steps, like, closing this connection.
            error!("{} cs_oug_back_log {} pressure > {}", active.prefix, m, n);
            return Ok(QueueStatus::Disconnected(Vec::new()));
        }

        active.cs_oug_back_log.extend(msgs.into_iter());

        let cs_oug_back_log = mem::replace(&mut active.cs_oug_back_log, vec![]);
        let (ids, mut status) =
            flush_to_miot(&active.prefix, &active.miot_tx, cs_oug_back_log);
        let _empty = mem::replace(&mut active.cs_oug_back_log, status.take_values());

        debug_assert!(ids.len() == 0);

        Ok(status)
    }

    fn book_oug_qos12(&mut self, msgs: Vec<Message>) -> Result<()> {
        let active = match self {
            SessionState::Active(active) => active,
            ss => unreachable!("{:?}", ss),
        };

        for msg in msgs.into_iter() {
            match &msg {
                Message::Packet { packet_id: Some(packet_id), .. } => {
                    let packet_id = *packet_id;
                    if let Some(_) = active.oug_retry_qos12.insert(packet_id, msg) {
                        err!(
                            Fatal,
                            code: ImplementationError,
                            "packet_id:{} duplicate in oug_retry_qos12",
                            packet_id
                        )?;
                    }
                }
                msg => unreachable!("{:?}", msg),
            }
        }

        Ok(())
    }
}

//impl SessionState {
//    fn oug_qos12(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
//        match self {
//            SessionState::Active { .. } => self.oug_qos12_a(msgs),
//            SessionState::Replica { .. } => self.oug_qos12_r(msgs),
//            SessionState::Reconnect { .. } => QueueStatus::Ok(Vec::new()),
//            ss => unreachable!("{:?}", ss),
//        }
//    }
//
//    fn oug_qos12_a(&mut self, msgs: Vec<Message>) -> QueueMsg {
//        let (prefix, config, oug_retry_qos12, cs_oug_back_log) = match self {
//            SessionState::Active {
//                prefix, config, oug_retry_qos12, cs_oug_back_log, ..
//            } => (prefix, config, oug_retry_qos12, cs_oug_back_log),
//            ss => unreachable!("{:?}", ss),
//        };
//        let mqtt_pkt_batch_size = config.mqtt_pkt_batch_size;
//
//        let m = cs_oug_back_log.len();
//        // TODO: separate back-log limit from mqtt_pkt_batch_size.
//        let n = (mqtt_pkt_batch_size as usize) * 4;
//        if m > n {
//            // TODO: if back-pressure is increasing due to a slow receiving client,
//            // we will have to take drastic steps, like, closing this connection.
//            error!("{} session.cs_oug_back_log {} pressure > {}", prefix, m, n);
//            return QueueStatus::Disconnected(Vec::new());
//        }
//
//        let mqtt_receive_maximum = config.mqtt_receive_maximum;
//        if oug_retry_qos12.len() >= usize::from(mqtt_receive_maximum) {
//            return QueueStatus::Block(Vec::new());
//        }
//
//        mem::drop(prefix);
//        mem::drop(config);
//        mem::drop(oug_retry_qos12);
//        mem::drop(cs_oug_back_log);
//
//        for msg in msgs.into_iter() {
//            let packet_id = self.incr_packet_id();
//            let msg = msg.into_packet(packet_id);
//            self.as_mut_cs_oug_back_log().insert(msg.to_out_seqno(), msg);
//        }
//
//        let (prefix, miot_tx, oug_retry_qos12, cs_oug_back_log) = match self {
//            SessionState::Active {
//                prefix, miot_tx, oug_retry_qos12, cs_oug_back_log, ..
//            } => (prefix, miot_tx, oug_retry_qos12, cs_oug_back_log),
//            ss => unreachable!("{:?}", ss),
//        };
//
//        let max = usize::try_from(mqtt_pkt_batch_size).unwrap();
//        let mut msgs = Vec::default();
//        while msgs.len() < max {
//            match cs_oug_back_log.pop_first() {
//                Some((_, msg)) => msgs.push(msg),
//                None => break,
//            }
//        }
//        for msg in msgs.clone().into_iter() {
//            oug_retry_qos12.insert(msg.to_packet_id(), msg);
//        }
//
//        let mut status = flush_to_miot(prefix, miot_tx, msgs);
//
//        // re-insert, cleanup for remaining messages.
//        for msg in status.take_values().into_iter() {
//            let packet_id = msg.to_packet_id();
//            cs_oug_back_log.insert(msg.to_out_seqno(), msg);
//            oug_retry_qos12.remove(&packet_id);
//        }
//
//        status
//    }
//
//
//    fn commit_acks(&mut self, out_seqnos: Vec<OutSeqno>) {
//        let cs_oug_back_log = match self {
//            SessionState::Active { cs_oug_back_log, .. } => cs_oug_back_log,
//            SessionState::Replica { cs_oug_back_log, .. } => cs_oug_back_log,
//            SessionState::Reconnect { cs_oug_back_log, .. } => cs_oug_back_log,
//            ss => unreachable!("{:?}", ss),
//        };
//        for out_seqno in out_seqnos.into_iter() {
//            cs_oug_back_log.remove(&out_seqno);
//        }
//    }
//
//    fn oug_acks_publish(&mut self, packet_id: PacketID) {
//        match self {
//            SessionState::Active { oug_acks, .. } => {
//                oug_acks.push(Message::new_pub_ack(v5::Pub::new_pub_ack(packet_id)));
//            }
//            SessionState::Reconnect { .. } => (),
//            ss => unreachable!("{:?}", ss),
//        }
//    }
//
//}

//impl SessionState {
//    fn out_qos_replica(&mut self, msgs: Vec<Message>) -> QueueMsg {
//        let cs_oug_back_log = match self {
//            SessionState::Active { cs_oug_back_log, .. } => cs_oug_back_log,
//            ss => unreachable!("{:?}", ss),
//        };
//
//        for msg in msgs.into_iter() {
//            // TODO: packet_id shall be inserted into the message when replica gets
//            //       promoted to active _and_ remote is request for msgs in back_log.
//            let msg = msg.into_packet(None);
//            cs_oug_back_log.insert(msg.to_out_seqno(), msg);
//        }
//
//        QueueStatus::Ok(Vec::new())
//    }
//}

impl SessionState {
    fn as_mut_subscriptions(&mut self) -> &mut BTreeMap<TopicFilter, v5::Subscription> {
        match self {
            SessionState::Active(active) => &mut active.cs_subscriptions,
            ss => unreachable!("{:?}", ss),
        }
    }

    fn as_mut_oug_acks(&mut self) -> &mut Vec<Message> {
        match self {
            SessionState::Active(active) => &mut active.oug_acks,
            ss => unreachable!("{:?}", ss),
        }
    }

    fn as_mut_oug_back_log(&mut self) -> &mut Vec<Message> {
        match self {
            SessionState::Active(active) => &mut active.oug_back_log,
            ss => unreachable!("{:?}", ss),
        }
    }
}

// Flush `Message::ClientAck` and `Message::Packet` downstream
fn flush_to_miot(
    prefix: &str,
    miot_tx: &PktTx,
    mut msgs: Vec<Message>,
) -> (Vec<PacketID>, QueueMsg) {
    let pkts: Vec<v5::Packet> = msgs.iter().map(|m| m.to_v5_packet()).collect();
    let mut status = miot_tx.try_sends(&prefix, pkts);
    let pkts = status.take_values();

    let m = msgs.len();
    let n = pkts.len();

    let mut packet_ids = Vec::default();
    for msg in msgs.drain(..(m - n)) {
        if let Message::ClientAck { packet } = msg {
            match packet {
                v5::Packet::PubAck(puback) => packet_ids.push(puback.packet_id),
                v5::Packet::SubAck(suback) => packet_ids.push(suback.packet_id),
                v5::Packet::UnsubAck(unsuback) => packet_ids.push(unsuback.packet_id),
                _ => (),
            }
        }
    }

    (packet_ids, status.map(msgs))
}

type PubMatches = BTreeMap<ClientID, (v5::Subscription, Vec<u32>)>;

// a. Only one message is sent to a client, even with multiple matches.
// b. subscr_qos is maximum of all matching-subscribption.
// c. final qos is min(server_qos, publish_qos, subscr_qos)
// d. retain if _any_ of the matching-subscription is calling for retain_as_published.
// e. no_local if _all_ of the matching-subscription is calling for no_local.
pub fn match_subscribers<S>(shard: &mut S, topic_name: &TopicName) -> PubMatches
where
    S: Shard,
{
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
    err!(Fatal, code: UnspecifiedError, "{} {}", prefix, msg)
}

fn dbg_replace_topal(prefix: &str, alias: u16, old: &TopicName, topic_name: &TopicName) {
    debug!(
        concat!("{} topic_alias:{} old_topic:{:?} new_topic:{:?}", "replacing ... "),
        prefix, alias, old, topic_name
    )
}
