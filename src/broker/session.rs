use log::{debug, error, trace};

use std::collections::{BTreeMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::{cmp, fmt, mem, net, result};

use crate::broker::{Config, RouteIO, SubscribedTrie};
use crate::broker::{KeepAlive, Message, OutSeqno, ShardAPI};
use crate::PacketType;
use crate::{ClientID, PacketID, Protocol, Subscription, Timer, TopicFilter, TopicName};
use crate::{Error, ErrorKind, ReasonCode, Result};
use crate::{PacketRx, PacketTx, QPacket, QoS, QueueStatus, RetainForwardRule};

/// Arguments to create a master-session.
pub struct SessionArgsMaster {
    pub shard_id: u32,
    pub client_id: ClientID,
    pub raddr: net::SocketAddr,
    pub config: Config,
    pub proto: Protocol,

    pub miot_tx: PacketTx,
    pub session_rx: PacketRx,

    pub client_keep_alive: u16,
    pub client_receive_maximum: u16,
    pub client_session_expiry_interval: Option<u32>,
}

/// Arguments to create a replica-session.
pub struct SessionArgsReplica {
    pub shard_id: u32,
    pub client_id: ClientID,
    pub raddr: net::SocketAddr,
    pub config: Config,
    pub proto: Protocol,
    pub clean_start: bool,
}

type QueueMsg = QueueStatus<Message>;
type HandleArgs<'a, S> = (&'a mut S, &'a mut RouteIO, QPacket);

//   Initial/None  --------> Master ----+---------> Reconnect
//          |                   ^       |              | ^
//          |                   |       |              | |
//          |                   +-------+--------------+ |
//          |                   |                      | |
//          |                   |                      | |
//          +--------------> Replica <-----------------+ |
//                              |                        |
//                              +------------------------+
//
pub enum SessionState {
    Master(Master),
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
            SessionState::Master(_) => write!(f, "SessionState::Master"),
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
    cs_subscriptions: BTreeMap<TopicFilter, Subscription>,
    /// Monotonically increasing `seqno`, starting from 1, that is bumped up for
    /// every outgoing publish packet.
    cs_oug_seqno: OutSeqno,
    /// Message::Oug outgoing PUBLISH > QoS-1/2, first land here.
    ///
    /// qos12-publish messages are commited after consensus loop.
    /// qos12-retain messages are commited while receiving new subscriptions.
    cs_oug_back_log: Vec<Message>,
    /// Message::Oug outgoing QoS-1/2 PUBLISH messages.
    /// Message::Oug outgoing QoS-1/2 Retain messages.
    ///
    /// This index is a set of un-acked collection of inflight PUBLISH (QoS-1 & 2)
    /// messages sent to subscribed clients. Entry is deleted from `oug_retry_qos12`
    /// when ACK is received for PacketID and commited here only after replicating
    /// ACK packet-ids in consensus loop.
    ///
    /// Note that length of this collection can't be more than the allowed limit of
    /// concurrent PUBLISH specified by client.
    oug_retry_qos12: Timer<PacketID, Message>,
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
            oug_retry_qos12: Timer::default(),
        }
    }
}

pub struct Master {
    common: Common,

    // Immutable set of parameters for this session, after handshake.
    keep_alive: KeepAlive, // Negotiated keep-alive.
    miot_tx: PacketTx,     // Outbound channel to Miot thread.
    session_rx: PacketRx,  // Inbound channel from Miot thread.

    // Sorted list of QoS-1 & QoS-2, SUB, UNSUB packet_id for managing incoming
    // duplicate publish and/or  Subscribe and UnSubscribe packets
    inc_packet_ids: Vec<PacketID>,
    // Message::ClientAck that needs to be sent to remote client.
    oug_acks: Vec<Message>,
    // Message::Oug outgoing QoS-0 PUBLISH messages.
    // Message::Oug outgoing QoS-0 Retain messages.
    oug_back_log: Vec<Message>,
    // Array of available packet-ids. size is determined by `receive_maximum`.
    next_packet_ids: VecDeque<PacketID>,
    // Pending qos12 publish (Routed and Retain) messages waiting for next_packet_ids.
    pending_qos12: VecDeque<Message>,
}

impl From<SessionArgsMaster> for Master {
    fn from(args: SessionArgsMaster) -> Master {
        let next_packet_ids: VecDeque<PacketID> =
            (1..args.client_receive_maximum).collect();

        let keep_alive = KeepAlive::new(&args.proto, args.raddr, args.client_keep_alive);

        Master {
            common: Common { config: args.config, ..Common::default() },

            keep_alive,
            miot_tx: args.miot_tx,
            session_rx: args.session_rx,

            inc_packet_ids: Vec::default(),
            oug_acks: Vec::default(),
            oug_back_log: Vec::default(),
            next_packet_ids,
            pending_qos12: VecDeque::default(),
        }
    }
}

impl Deref for Master {
    type Target = Common;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

impl DerefMut for Master {
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

    // arrives from SessionState::Master, and lays dormant
    next_packet_ids: VecDeque<PacketID>,
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
    /// Protocol used by this session
    pub proto: Protocol,
    /// Broker Configuration.
    pub config: Config,
    /// Remote address,
    pub raddr: net::SocketAddr,
    /// Client's ClientID that created this session.
    pub client_id: ClientID,
    /// Shard hosting this session.
    pub shard_id: u32,

    /// Client configuration
    pub client_session_expiry_interval: Option<u32>,

    pub state: SessionState,
    pub stats: Stats,
}

impl Default for Session {
    fn default() -> Session {
        Session {
            prefix: String::default(),
            proto: Protocol::default(),
            config: Config::default(),
            raddr: "0.0.0.0:0".parse().unwrap(),
            client_id: ClientID::default(),
            shard_id: u32::default(),

            client_session_expiry_interval: None,

            state: SessionState::default(),
            stats: Stats::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Stats {
    n_pings: usize,
}

impl Session {
    pub fn into_master(mut self, args: SessionArgsMaster) -> Session {
        let prefix = format!("session:master:{}", args.raddr);
        let config = args.config.clone();
        let client_id = args.client_id.clone();
        let raddr = args.raddr;
        let shard_id = args.shard_id;
        let proto = args.proto.clone();
        let client_session_expiry_interval = args.client_session_expiry_interval;

        let mut master = Master::from(args);
        master.prefix = prefix.clone();

        let state = mem::replace(&mut self.state, SessionState::None);
        let state = match state {
            SessionState::None => SessionState::Master(master),
            SessionState::Replica(replica) => {
                master.common.cs_oug_back_log = replica.common.oug_retry_qos12.values();
                master.cs_oug_back_log.extend(replica.common.cs_oug_back_log.into_iter());

                master.common.cs_subscriptions = replica.common.cs_subscriptions;
                master.common.cs_oug_seqno = replica.common.cs_oug_seqno;
                master.common.oug_retry_qos12 = replica.common.oug_retry_qos12;
                SessionState::Master(master)
            }
            SessionState::Reconnect(reconnect) => {
                master.common.cs_oug_back_log = reconnect.common.oug_retry_qos12.values();
                master
                    .cs_oug_back_log
                    .extend(reconnect.common.cs_oug_back_log.into_iter());

                master.common.topic_aliases = reconnect.common.topic_aliases;
                master.common.cs_subscriptions = reconnect.common.cs_subscriptions;
                master.common.cs_oug_seqno = reconnect.common.cs_oug_seqno;
                master.common.oug_retry_qos12 = reconnect.common.oug_retry_qos12;
                master.next_packet_ids = match reconnect.next_packet_ids.len() {
                    0 => master.next_packet_ids,
                    _ => reconnect.next_packet_ids,
                };
                SessionState::Master(master)
            }
            ss => unreachable!("{:?}", ss),
        };

        Session {
            prefix,
            proto,
            config,
            client_id,
            raddr,
            shard_id,

            client_session_expiry_interval,

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

        let mut replica = Replica::from(args);
        replica.prefix = prefix.clone();

        let state = match self.state {
            SessionState::None => SessionState::Replica(replica),
            SessionState::Master(master) => {
                replica.common.cs_subscriptions = master.common.cs_subscriptions;
                replica.common.cs_oug_seqno = master.common.cs_oug_seqno;
                replica.common.cs_oug_back_log = master.common.cs_oug_back_log;
                replica.common.oug_retry_qos12 = master.common.oug_retry_qos12;
                SessionState::Replica(replica)
            }
            SessionState::Reconnect(reconnect) => {
                replica.common.cs_subscriptions = reconnect.common.cs_subscriptions;
                replica.common.cs_oug_seqno = reconnect.common.cs_oug_seqno;
                replica.common.cs_oug_back_log = reconnect.common.cs_oug_back_log;
                replica.common.oug_retry_qos12 = reconnect.common.oug_retry_qos12;
                SessionState::Replica(replica)
            }
            ss => unreachable!("{:?}", ss),
        };

        Session {
            prefix,
            proto: self.proto,
            config,
            client_id,
            raddr,
            shard_id,

            client_session_expiry_interval: None,

            state,
            stats: Stats::default(),
        }
    }

    pub fn into_reconnect(self) -> Session {
        let prefix = format!("session:reconnect:{}", self.raddr);

        let state = match self.state {
            SessionState::Master(master) => {
                let reconnect = Reconnect {
                    common: Common {
                        prefix: prefix.clone(),
                        config: master.common.config,
                        topic_aliases: master.common.topic_aliases,
                        cs_subscriptions: master.common.cs_subscriptions,
                        cs_oug_seqno: master.common.cs_oug_seqno,
                        cs_oug_back_log: master.common.cs_oug_back_log,
                        oug_retry_qos12: master.common.oug_retry_qos12,
                    },
                    next_packet_ids: master.next_packet_ids,
                };
                SessionState::Reconnect(reconnect)
            }
            SessionState::Replica(replica) => {
                let reconnect = Reconnect {
                    common: Common {
                        prefix: prefix.clone(),
                        config: replica.common.config,
                        topic_aliases: replica.common.topic_aliases,
                        cs_subscriptions: replica.common.cs_subscriptions,
                        cs_oug_seqno: replica.common.cs_oug_seqno,
                        cs_oug_back_log: replica.common.cs_oug_back_log,
                        oug_retry_qos12: replica.common.oug_retry_qos12,
                    },
                    next_packet_ids: VecDeque::default(),
                };
                SessionState::Reconnect(reconnect)
            }
            ss => unreachable!("{:?}", ss),
        };

        Session {
            prefix,
            proto: self.proto,
            config: self.config.clone(),
            client_id: self.client_id,
            raddr: self.raddr,
            shard_id: self.shard_id,

            client_session_expiry_interval: None,

            state,
            stats: self.stats,
        }
    }

    pub fn close(self) -> Result<Stats> {
        self.state.close()?;
        Ok(self.stats)
    }
}

impl Session {
    #[inline]
    pub fn is_replica(&self) -> bool {
        matches!(&self.state, SessionState::Replica(_))
    }

    pub fn to_cs_oug_seqno(&self) -> OutSeqno {
        match &self.state {
            SessionState::Master(master) => master.cs_oug_seqno,
            SessionState::Replica(replica) => replica.cs_oug_seqno,
            SessionState::Reconnect(reconnect) => reconnect.cs_oug_seqno,
            ss => unreachable!("{} {:?}", self.prefix, ss),
        }
    }

    pub fn to_session_expiry_interval(&self) -> Option<u32> {
        self.client_session_expiry_interval
    }

    pub fn as_protocol(&self) -> &Protocol {
        &self.proto
    }
}

impl Session {
    pub fn remove_topic_filters(&mut self, filters: &mut SubscribedTrie) {
        match &self.state {
            SessionState::Master(master) => {
                for (topic_filter, value) in master.cs_subscriptions.iter() {
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
        S: ShardAPI,
    {
        let master = match &mut self.state {
            SessionState::Master(master) => master,
            ss => unreachable!("{} {:?}", self.prefix, ss),
        };

        let mut status = master.session_rx.try_recvs(&self.prefix);

        match status.take_values() {
            pkts if pkts.len() == 0 => {
                master.keep_alive.check_expired()?;
            }
            pkts => {
                master.keep_alive.live();

                for pkt in pkts.into_iter() {
                    self.handle_packet(shard, rio, pkt)?;
                }
            }
        };
        rio.disconnected = status.is_disconnected();

        Ok(())
    }

    // Disconnected
    // ProtocolError
    fn handle_packet<S>(&mut self, s: &mut S, r: &mut RouteIO, p: QPacket) -> Result<()>
    where
        S: ShardAPI,
    {
        match p.to_packet_type() {
            PacketType::PingReq => self.rx_pingreq(r, p)?,
            PacketType::Subscribe => self.rx_subscribe((s, r, p))?,
            PacketType::UnSubscribe => self.rx_unsubscribe((s, r, p))?,
            PacketType::Publish => self.rx_publish((s, r, p))?,
            PacketType::PubAck => self.rx_puback((s, r, p))?,
            PacketType::PubRec => self.rx_pubrec((s, r, p))?,
            PacketType::PubRel => self.rx_pubrel((s, r, p))?,
            PacketType::PubComp => self.rx_pubcomp((s, r, p))?,
            PacketType::Disconnect => self.rx_disconnect((s, r, p))?,
            PacketType::Auth => self.rx_auth((s, r, p))?,
            // Errors
            PacketType::Connect => err_dup_connect(&self.prefix)?,
            PacketType::ConnAck => err_invalid_pkt(&self.prefix, &p)?,
            PacketType::SubAck => err_invalid_pkt(&self.prefix, &p)?,
            PacketType::UnsubAck => err_invalid_pkt(&self.prefix, &p)?,
            PacketType::PingResp => err_invalid_pkt(&self.prefix, &p)?,
        }

        Ok(())
    }

    fn rx_pingreq(&mut self, route_io: &mut RouteIO, ping: QPacket) -> Result<()> {
        self.stats.n_pings += 1;
        trace!("{} received PingReq", self.prefix);
        route_io.oug_msgs.push(Message::new_ping_resp(self.proto.new_ping_resp(ping)));

        Ok(())
    }

    fn rx_subscribe<S>(&mut self, (shard, route_io, sub): HandleArgs<S>) -> Result<()>
    where
        S: ShardAPI,
    {
        if self.is_duplicate(&sub) {
            return Ok(());
        }
        self.book_packet_id(&sub)?;

        let mut retain_msgs = Vec::default();
        for subscr in sub.to_subscriptions() {
            let topic_filter = &subscr.topic_filter;
            let is_new = self.state.as_mut_subscriptions().get(topic_filter).is_none();

            let publs = match subscr.retain_forward_rule {
                RetainForwardRule::OnEverySubscribe => {
                    shard.as_retained_topics().match_topic_filter(topic_filter)
                }
                RetainForwardRule::OnNewSubscribe if is_new => {
                    shard.as_retained_topics().match_topic_filter(topic_filter)
                }
                RetainForwardRule::OnNewSubscribe => Vec::default(),
                RetainForwardRule::Never => Vec::default(),
            };

            retain_msgs.extend(publs.into_iter().map(|mut p| {
                p.set_retain(true);
                Message::new_retain_publish(p)
            }));
        }

        route_io.oug_msgs.push(Message::new_sub(sub.clone()));
        route_io.oug_msgs.extend(retain_msgs.into_iter());

        Ok(())
    }

    fn rx_unsubscribe<S>(&mut self, (_s, route_io, unsub): HandleArgs<S>) -> Result<()>
    where
        S: ShardAPI,
    {
        if self.is_duplicate(&unsub) {
            return Ok(());
        }
        self.book_packet_id(&unsub)?;

        route_io.oug_msgs.push(Message::new_unsub(unsub));

        Ok(())
    }

    fn rx_publish<S>(&mut self, (shard, route_io, publish): HandleArgs<S>) -> Result<()>
    where
        S: ShardAPI,
    {
        let server_qos = self.proto.maximum_qos();
        let publish_qos = publish.to_qos();

        if self.is_duplicate(&publish) {
            return Ok(());
        }
        self.book_packet_id(&publish)?;

        if publish_qos > server_qos {
            err_unsup_qos(&self.prefix, publish_qos)?
        }

        self.book_retain(&publish)?;

        let inp_seqno = shard.incr_inp_seqno();
        let topic_name = self.publish_topic_name(&publish)?;

        if publish.is_qos12() {
            route_io.oug_msgs.push(Message::new_index(
                &self.client_id,
                inp_seqno,
                &publish,
            ));
        }

        let mut n_subscrs = 0;
        let publ_qos = cmp::min(server_qos, publish_qos);
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
                let retain = subscr.retain_as_published && publish.is_retain();
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
                out_seqno: 0, // NOTE: shall by the receiving session.
                publish,
            });

            n_subscrs += 1;
        }

        match (publish_qos, n_subscrs) {
            (QoS::AtMostOnce, _) => (),
            (QoS::AtLeastOnce, 0) => {
                let puback = self.proto.new_pub_ack(publish.to_packet_id().unwrap());
                route_io.oug_msgs.push(Message::new_pub_ack(puback));
            }
            (QoS::AtLeastOnce, _) => (),
            (QoS::ExactlyOnce, _) => todo!(),
        }

        Ok(())
    }

    fn rx_puback<S>(&mut self, (_s, _route_io, _pkt): HandleArgs<S>) -> Result<()>
    where
        S: ShardAPI,
    {
        todo!()
    }

    fn rx_pubrec<S>(&mut self, (_s, _route_io, _pkt): HandleArgs<S>) -> Result<()>
    where
        S: ShardAPI,
    {
        todo!()
    }

    fn rx_pubrel<S>(&mut self, (_s, _route_io, _pkt): HandleArgs<S>) -> Result<()>
    where
        S: ShardAPI,
    {
        todo!()
    }

    fn rx_pubcomp<S>(&mut self, (_s, _route_io, _pkt): HandleArgs<S>) -> Result<()>
    where
        S: ShardAPI,
    {
        todo!()
    }

    fn rx_disconnect<S>(&mut self, (_shard, _route_io, dis): HandleArgs<S>) -> Result<()>
    where
        S: ShardAPI,
    {
        let code = dis.to_disconnect_code();
        let reason_string = dis.to_reason_string();

        debug!("{} client disconnected code:{}", self.prefix, code);
        let code = ReasonCode::Success;

        if let Some(txt) = reason_string {
            debug!("{} disconnect with reason string {}", self.prefix, txt);
        }

        err_disconnect(ReasonCode::try_from(code).ok())
    }

    fn rx_auth<S>(&mut self, (_shard, _route_io, _auth): HandleArgs<S>) -> Result<()>
    where
        S: ShardAPI,
    {
        todo!()
    }

    fn is_duplicate(&self, pkt: &QPacket) -> bool {
        let master = match &self.state {
            SessionState::Master(master) => master,
            ss => unreachable!("{:?}", ss),
        };

        let pt = pkt.to_packet_type();
        match pt {
            PacketType::Publish => match pkt.to_qos() {
                QoS::AtMostOnce => false,
                QoS::AtLeastOnce | QoS::ExactlyOnce => {
                    let packet_id = pkt.to_packet_id().unwrap();
                    matches!(master.inc_packet_ids.binary_search(&packet_id), Ok(_))
                }
            },
            PacketType::Subscribe => {
                let packet_id = pkt.to_packet_id().unwrap();
                matches!(master.inc_packet_ids.binary_search(&packet_id), Ok(_))
            }
            PacketType::UnSubscribe => {
                let packet_id = pkt.to_packet_id().unwrap();
                matches!(master.inc_packet_ids.binary_search(&packet_id), Ok(_))
            }
            pt => unreachable!("{:?} packet type", pt),
        }
    }

    fn book_packet_id(&mut self, pkt: &QPacket) -> Result<()> {
        let master = match &mut self.state {
            SessionState::Master(master) => master,
            ss => unreachable!("{:?}", ss),
        };

        let pt = pkt.to_packet_type();
        match pt {
            PacketType::Publish => match pkt.to_qos() {
                QoS::AtMostOnce => (),
                QoS::AtLeastOnce | QoS::ExactlyOnce => {
                    let packet_id = pkt.to_packet_id().unwrap();
                    if let Err(off) = master.inc_packet_ids.binary_search(&packet_id) {
                        master.inc_packet_ids.insert(off, packet_id);
                    } else {
                        unreachable!() // is_duplicate() would have filtered this path.
                    }
                }
            },
            PacketType::Subscribe => {
                let packet_id = pkt.to_packet_id().unwrap();

                if let Err(off) = master.inc_packet_ids.binary_search(&packet_id) {
                    master.inc_packet_ids.insert(off, packet_id);
                } else {
                    unreachable!() // is_duplicate() would have filtered this path.
                }
            }
            PacketType::UnSubscribe => {
                let packet_id = pkt.to_packet_id().unwrap();

                if let Err(off) = master.inc_packet_ids.binary_search(&packet_id) {
                    master.inc_packet_ids.insert(off, packet_id);
                } else {
                    unreachable!() // is_duplicate() would have filtered this path.
                }
            }
            pt => unreachable!("{:?} packet type", pt),
        }

        Ok(())
    }

    fn book_retain(&mut self, pkt: &QPacket) -> Result<()> {
        let is_retain = pkt.is_retain();
        if is_retain && !self.proto.retain_available() {
            err_unsup_retain(&self.prefix)?;
        } else if is_retain {
            todo!()
        }

        Ok(())
    }

    fn publish_topic_name(&mut self, pkt: &QPacket) -> Result<TopicName> {
        let server_alias_max = self.proto.topic_alias_max();

        let master = match &mut self.state {
            SessionState::Master(master) => master,
            ss => unreachable!("{:?}", ss),
        };

        let topic_name = pkt.as_topic_name();
        let topic_alias = pkt.to_topic_alias();

        let topic_name = match topic_alias {
            Some(_alias) if server_alias_max.is_none() => {
                err_unsup_topal(&master.prefix)?;
                TopicName::default()
            }
            Some(alias) if alias > server_alias_max.unwrap() => {
                err_exceed_topal(&master.prefix, alias, server_alias_max)?;
                TopicName::default()
            }
            Some(alias) if topic_name.len() > 0 => {
                let name = topic_name.clone();
                if let Some(old) = master.topic_aliases.insert(alias, name) {
                    dbg_replace_topal(&master.prefix, alias, &old, topic_name);
                }
                topic_name.clone()
            }
            Some(alias) => match master.topic_aliases.get(&alias) {
                Some(topic_name) => topic_name.clone(),
                None => {
                    err_missing_alias(&master.prefix, alias)?;
                    TopicName::default()
                }
            },
            None if topic_name.len() == 0 => {
                err_missing_topal(&master.prefix)?;
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
    pub fn incr_oug_qos12(&mut self, msgs: Vec<Message>) -> Result<QueueMsg> {
        self.state.incr_oug_qos12(msgs)
    }

    #[inline]
    pub fn tx_oug_acks(&mut self, msgs: Vec<Message>) -> QueueMsg {
        self.state.tx_oug_acks(msgs)
    }

    #[inline]
    pub fn tx_oug_back_log(&mut self, msgs: Vec<Message>) -> QueueMsg {
        self.state.tx_oug_back_log(msgs)
    }

    pub fn commit_subs<S>(&mut self, s: &mut S, msgs: Vec<Message>) -> Result<QueueMsg>
    where
        S: ShardAPI,
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
        S: ShardAPI,
    {
        let server_qos = self.proto.maximum_qos();

        let sub = match msg {
            Message::Subscribe { sub } => sub,
            _ => unreachable!(),
        };

        let mut subscrs = sub.to_subscriptions();
        let mut rcodes = Vec::with_capacity(subscrs.len());

        for subscr in subscrs.iter_mut() {
            subscr.qos = cmp::min(server_qos, subscr.qos);

            let topic_filter = subscr.topic_filter.clone();
            self.state.commit_sub(shard, (topic_filter, &subscr))?;

            let rc = match subscr.qos {
                QoS::AtMostOnce => ReasonCode::Success,
                QoS::AtLeastOnce => ReasonCode::QoS1,
                QoS::ExactlyOnce => ReasonCode::QoS2,
            };
            rcodes.push(rc)
        }

        match &self.state {
            SessionState::Master(_) => {
                Ok(Message::new_sub_ack(self.proto.new_sub_ack(&sub, rcodes)))
            }
            ss => unreachable!("{:?}", ss),
        }
    }

    pub fn commit_unsubs<S>(&mut self, s: &mut S, msgs: Vec<Message>) -> Result<QueueMsg>
    where
        S: ShardAPI,
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
        S: ShardAPI,
    {
        let unsub = match msg {
            Message::UnSubscribe { unsub } => unsub,
            _ => unreachable!(),
        };

        let subscrs = unsub.to_unsubscriptions(self.client_id.clone());
        let mut rcodes = Vec::with_capacity(subscrs.len());

        for subscr in subscrs.iter() {
            let code = self.state.commit_unsub(shard, subscr)?;
            rcodes.push(code);
        }

        match &self.state {
            SessionState::Master(_) => {
                Ok(Message::new_unsub_ack(self.proto.new_unsub_ack(&unsub, rcodes)))
            }
            ss => unreachable!("{:?}", ss),
        }
    }

    pub fn commit_cs_oug_back_log(&mut self, msgs: Vec<Message>) -> Result<QueueMsg> {
        self.state.commit_cs_oug_back_log(msgs)
    }

    pub fn retry_publish(&mut self) -> Result<QueueMsg> {
        self.state.retry_publish()
    }
}

type HandleSubArgs<'a> = (TopicFilter, &'a Subscription);

impl SessionState {
    pub fn incr_oug_qos0(&mut self) -> OutSeqno {
        match self {
            SessionState::Master(master) => {
                let seqno = master.cs_oug_seqno;
                master.cs_oug_seqno = seqno.saturating_add(1);
                seqno
            }
            ss => unreachable!("{:?}", ss),
        }
    }

    // msgs carry Message::Retain or Message::Routed of qos12-publish
    pub fn incr_oug_qos12(&mut self, msgs: Vec<Message>) -> Result<QueueMsg> {
        let master = match self {
            SessionState::Master(master) => master,
            ss => unreachable!("{:?}", ss),
        };

        master.pending_qos12.extend(msgs.into_iter());

        // TODO: separate back-log limit from pkt_batch_size.
        let m = master.pending_qos12.len();
        let n = (master.config.pkt_batch_size as usize) * 4;
        if m > n {
            // TODO: if back-pressure is increasing due to a slow receiving client,
            // we will have to take drastic steps, like, closing this connection.
            error!("{} pending_qos12 {} pressure > {}", master.prefix, m, n);
            return Ok(QueueStatus::Disconnected(Vec::new()));
        }

        let mut oug_msgs = Vec::default();
        while let Some(msg) = master.pending_qos12.pop_front() {
            match master.next_packet_ids.pop_front() {
                Some(packet_id) => {
                    let out_seqno = master.cs_oug_seqno;
                    master.cs_oug_seqno = out_seqno.saturating_add(1);
                    oug_msgs.push(msg.into_oug(out_seqno, packet_id));
                }
                None => {
                    master.pending_qos12.push_back(msg);
                    break;
                }
            };
        }

        Ok(QueueStatus::Ok(oug_msgs))
    }

    fn tx_oug_acks(&mut self, msgs: Vec<Message>) -> QueueMsg {
        let master = match self {
            SessionState::Master(master) => master,
            ss => unreachable!("{:?}", ss),
        };

        // TODO: separate back-log limit from pkt_batch_size.
        let m = master.oug_acks.len();
        let n = (master.config.pkt_batch_size as usize) * 4;
        if m > n {
            // TODO: if back-pressure is increasing due to a slow receiving client,
            // we will have to take drastic steps, like, closing this connection.
            error!("{} oug_acks {} pressure > {}", master.prefix, m, n);
            return QueueStatus::Disconnected(Vec::new());
        }

        master.oug_acks.extend(msgs.into_iter());

        let oug_acks = mem::replace(&mut master.oug_acks, vec![]);
        let (ids, mut status) = flush_to_miot(&master.prefix, &master.miot_tx, oug_acks);
        let _empty = mem::replace(&mut master.oug_acks, status.take_values());

        for packet_id in ids.into_iter() {
            match master.inc_packet_ids.binary_search(&packet_id) {
                Ok(off) => {
                    master.inc_packet_ids.remove(off);
                }
                Err(_off) => {
                    error!("{} packet_id:{} not booked", master.prefix, packet_id)
                }
            }
        }

        status
    }

    fn tx_oug_back_log(&mut self, msgs: Vec<Message>) -> QueueMsg {
        let master = match self {
            SessionState::Master(master) => master,
            ss => unreachable!("{:?}", ss),
        };

        // TODO: separate back-log limit from pkt_batch_size.
        let m = master.oug_back_log.len();
        let n = (master.config.pkt_batch_size as usize) * 4;
        if m > n {
            // TODO: if back-pressure is increasing due to a slow receiving client,
            // we will have to take drastic steps, like, closing this connection.
            error!("{} oug_back_log {} pressure > {}", master.prefix, m, n);
            return QueueStatus::Disconnected(Vec::new());
        }

        master.oug_back_log.extend(msgs.into_iter());

        let msgs = mem::replace(&mut master.oug_back_log, vec![]);
        let (ids, mut status) = flush_to_miot(&master.prefix, &master.miot_tx, msgs);
        let _empty = mem::replace(&mut master.oug_back_log, status.take_values());

        debug_assert!(ids.len() == 0);

        status
    }

    fn commit_sub<'a, S>(&mut self, shard: &mut S, args: HandleSubArgs<'a>) -> Result<()>
    where
        S: ShardAPI,
    {
        let (topic_filter, subscr) = args;

        match self {
            SessionState::Master(master) => {
                master.cs_subscriptions.insert(topic_filter.clone(), subscr.clone());
                match shard.as_topic_filters().subscribe(&topic_filter, subscr.clone()) {
                    Some(old_subscr) => trace!(
                        "{} topic_filter:{:?} client_id:{:?}",
                        master.prefix,
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
        subr: &Subscription,
    ) -> Result<ReasonCode>
    where
        S: ShardAPI,
    {
        let filter = &subr.topic_filter;
        let code = match self {
            SessionState::Master(master) => {
                let res1 = shard.as_topic_filters().unsubscribe(filter, &subr);
                let res2 = master.cs_subscriptions.remove(filter);
                match (&res1, &res2) {
                    (Some(_), Some(_)) => ReasonCode::Success,
                    (None, None) => ReasonCode::NoSubscriptionExisted,
                    (Some(_), None) => {
                        let msg = format!("{:?} filter in trie, not in session", filter);
                        err_inconsistent_subscription(&master.prefix, &msg)?;
                        ReasonCode::UnspecifiedError
                    }
                    (None, Some(_)) => {
                        let msg = format!("{:?} filter in session, not in trie", filter);
                        err_inconsistent_subscription(&master.prefix, &msg)?;
                        ReasonCode::UnspecifiedError
                    }
                }
            }
            SessionState::Replica(replica) => {
                replica.cs_subscriptions.remove(filter);
                ReasonCode::Success
            }
            ss => unreachable!("{:?}", ss),
        };

        Ok(code)
    }

    // msgs carry Message::Retain or Message::Routed of qos12-publish
    // also carries Message::Oug as part of retry publish logic.
    fn commit_cs_oug_back_log(&mut self, msgs: Vec<Message>) -> Result<QueueMsg> {
        let master = match self {
            SessionState::Master(master) => master,
            ss => unreachable!("{:?}", ss),
        };

        master.cs_oug_back_log.extend(msgs.into_iter());

        // TODO: separate back-log limit from pkt_batch_size.
        let m = master.cs_oug_back_log.len();
        let n = (master.config.pkt_batch_size as usize) * 4;
        if m > n {
            // TODO: if back-pressure is increasing due to a slow receiving client,
            // we will have to take drastic steps, like, closing this connection.
            error!("{} cs_oug_back_log {} pressure > {}", master.prefix, m, n);
            return Ok(QueueStatus::Disconnected(Vec::new()));
        }

        let retry_interval = master.config.publish_retry_interval as u64;
        for msg in mem::replace(&mut master.cs_oug_back_log, vec![]).into_iter() {
            let packet_id = msg.to_packet_id().unwrap();
            master.oug_retry_qos12.add_timeout(retry_interval, packet_id, msg.clone());
        }

        let msgs = mem::replace(&mut master.cs_oug_back_log, Vec::default());

        let (ids, mut status) = flush_to_miot(&master.prefix, &master.miot_tx, msgs);
        debug_assert!(ids.len() == 0);

        let _empty = mem::replace(&mut master.cs_oug_back_log, status.take_values());
        Ok(status)
    }

    fn retry_publish(&mut self) -> Result<QueueMsg> {
        let master = match self {
            SessionState::Master(master) => master,
            ss => unreachable!("{:?}", ss),
        };

        {
            let msgs: Vec<Message> = master.oug_retry_qos12.gc().collect();
            debug!("{} gc:{} msgs in oug_retry_qos12", master.prefix, msgs.len());
        }

        let msgs: Vec<Message> = master.oug_retry_qos12.expired().collect();
        self.commit_cs_oug_back_log(msgs)
    }

    fn close(self) -> Result<()> {
        match self {
            SessionState::Master(master) => mem::drop(master),
            SessionState::Replica(replica) => mem::drop(replica),
            SessionState::Reconnect(reconnect) => mem::drop(reconnect),
            SessionState::None => (),
        }

        Ok(())
    }
}

impl SessionState {
    fn as_mut_subscriptions(&mut self) -> &mut BTreeMap<TopicFilter, Subscription> {
        match self {
            SessionState::Master(master) => &mut master.cs_subscriptions,
            ss => unreachable!("{:?}", ss),
        }
    }
}

// Flush `Message::ClientAck` and `Message::Oug` downstream
fn flush_to_miot(
    prefix: &str,
    miot_tx: &PacketTx,
    mut msgs: Vec<Message>,
) -> (Vec<PacketID>, QueueMsg) {
    let pkts: Vec<QPacket> = msgs.iter().map(|m| m.to_packet()).collect();
    let mut status = miot_tx.try_sends(&prefix, pkts);
    let pkts = status.take_values();

    let m = msgs.len();
    let n = pkts.len();

    let mut packet_ids = Vec::default();
    for msg in msgs.drain(..(m - n)) {
        if let Message::ClientAck { packet } = msg {
            packet_ids.push(packet.to_packet_id().unwrap())
        }
    }

    (packet_ids, status.replace(msgs))
}

type PubMatches = BTreeMap<ClientID, (Subscription, Vec<u32>)>;

// a. Only one message is sent to a client, even with multiple matches.
// b. subscr_qos is maximum of all matching-subscribption.
// c. final qos is min(server_qos, publish_qos, subscr_qos)
// d. retain if _any_ of the matching-subscription is calling for retain_as_published.
// e. no_local if _all_ of the matching-subscription is calling for no_local.
pub fn match_subscribers<S>(shard: &mut S, topic_name: &TopicName) -> PubMatches
where
    S: ShardAPI,
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

fn err_unsup_qos(prefix: &str, qos: QoS) -> Result<()> {
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

fn err_invalid_pkt(prefix: &str, pkt: &QPacket) -> Result<()> {
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
