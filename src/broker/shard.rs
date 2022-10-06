use log::{debug, error, info, trace, warn};
use uuid::Uuid;

use std::{collections::BTreeMap, fmt, mem, net, result, sync::Arc, time};

use crate::broker::thread::{Rx, Thread, Threadable, Tx};
use crate::broker::{message, session, ClusterAPI, ShardAPI};
use crate::broker::{AppTx, Config, RetainedTrie, Session, Shardable, SubscribedTrie};
use crate::broker::{ConsensIO, Flusher, InpSeqno, Message, Miot, MsgRx, Timestamp};
use crate::broker::{PQueue, RouteIO, SessionArgsMaster, SessionArgsReplica};
use crate::{ClientID, PacketRx, PacketTx, QoS, QueueStatus, Socket, Timer, ToJson};
use crate::{Error, ErrorKind, ReasonCode, Result};

type ThreadRx<C> = Rx<Request<C>, Result<Response>>;
type QueueReq<C> = crate::broker::thread::QueueReq<Request<C>, Result<Response>>;

macro_rules! append_index {
    ($index:expr, $key:expr, $val:expr) => {{
        match ($index).get_mut($key) {
            Some(values) => values.push($val),
            None => {
                ($index).insert(($key).clone(), vec![$val]);
            }
        }
    }};
}

macro_rules! appends_index {
    ($index:expr, $key:expr, $val:expr) => {{
        match ($index).get_mut($key) {
            Some(values) => values.extend($val.into_iter()),
            None => {
                ($index).insert(($key).clone(), $val);
            }
        }
    }};
}

/// Type is the workhorse of MQTT, and shall host one or more sessions.
///
/// Handle incoming MQTT packets, route them to other shards, send back acknowledgement,
/// and publish them to other clients.
pub struct Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    /// Human readable name for shard.
    pub name: String,
    /// Shard id, must unique withing the Cluster.
    pub shard_id: u32,
    /// Unique id for this shard. All shards in a cluster MUST be unique.
    pub uuid: Uuid,

    prefix: String,
    config: Config,

    inner: Inner<C>,
}

pub enum Inner<C>
where
    C: 'static + Send + ClusterAPI,
{
    Init,
    // Held by Cluster.
    Handle(Handle<C>),
    // Held by Miot and Ticker
    Tx(Arc<mio::Waker>, Tx<Request<C>, Result<Response>>),
    // Held by all Shard threads.
    MsgTx(Arc<mio::Waker>, message::MsgTx),
    // Thread.
    MainMaster(MasterLoop<C>),
    MainReplica(ReplicaLoop<C>),
    // Held by Cluster, replacing both Handle and Main.
    Close(FinState<C>),
}

impl<C> fmt::Debug for Inner<C>
where
    C: 'static + Send + ClusterAPI,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Inner::Init => write!(f, "Shard::Inner::Init"),
            Inner::Handle(_) => write!(f, "Shard::Inner::Handle"),
            Inner::Tx(_, _) => write!(f, "Shard::Inner::Handle"),
            Inner::MsgTx(_, _) => write!(f, "Shard::Inner::Handle"),
            Inner::MainMaster(_) => write!(f, "Shard::Inner::MainMaster"),
            Inner::MainReplica(_) => write!(f, "Shard::Inner::MainReplica"),
            Inner::Close(_) => write!(f, "Shard::Inner::Close"),
        }
    }
}

pub struct Handle<C>
where
    C: 'static + Send + ClusterAPI,
{
    waker: Arc<mio::Waker>,
    thrd: Thread<Shard<C>, Request<C>, Result<Response>>,
    msg_tx: Option<message::MsgTx>,
}

pub struct MasterLoop<C>
where
    C: 'static + Send + ClusterAPI,
{
    /// Mio poller for asynchronous handling, all events are from consensus port and
    /// thread-waker.
    poll: mio::Poll,
    /// Self waker.
    waker: Arc<mio::Waker>,
    /// Cluster::Tx handle to communicate back to cluster. Shall be dropped after
    /// close_wait call, when the thread returns, will point to Inner::Init.
    cluster: Box<C>,
    /// Flusher::Tx handle to communicate with flusher.
    flusher: Flusher,
    /// Inner::Handle to corresponding miot-thread. Shall be dropped after close_wait
    /// call, when the thread returns, will point to Inner::Init.
    miot: Miot<Shard<C>>,

    /// Collection of sessions and corresponding clients managed by this shard. Shall be
    /// dropped after close_wait call, when the thread returns it will be empty.
    sessions: BTreeMap<ClientID, Session>,
    reconnects: Timer<ClientID, Session>,

    /// Monotonically increasing `seqno`, starting from 1, that is bumped up for every
    /// incoming PUBLISH (QoS-1 & 2) packet.
    inp_seqno: InpSeqno,
    /// Message::ShardIndex
    ///
    /// Index of all incoming PUBLISH QoS-1 and QoS-2 messages. Happens along with
    /// `shard_back_log`. QoS-0 is not indexed here.
    ///
    /// All entries whose InpSeqno is < min(Timestamp::last_acked) in ack_timestamps
    /// shall be deleted from this index and ACK shall be sent to publishing client.
    index: BTreeMap<InpSeqno, Message>,
    /// Message::{Routed, LocalAck}
    ///
    /// Back log of messages that needs to be flushed to other local-shards.
    shard_back_log: BTreeMap<u32, Vec<Message>>,
    /// For N shards in this node, there can be upto be N-1 Timestamp-entries
    /// in this list.
    ///
    /// While routing the messages and pushing them in target's shards msg-queue, for
    /// each message, [Timestamp::last_routed] for [Timestamp::shard_id] value is
    /// updated with InpSeqno.
    ///
    /// For every incoming Message::LocalAck, [Timestamp::last_acked] for
    /// [Timestamp::shard_id] matching [Message::LocalAck::shard_id] is updated with
    /// InpSeqno.
    ///
    /// * [Timestamp::last_routed] shall always be >= [Timestamp::last_acked].
    /// * If [Timestamp::last_routed] == [Timestamp::last_acked], then there are no
    ///   outstanding ACKs.
    /// * All InpSeqno entries in `index` < min(last_acked) in ack_timestamps are
    ///   removed from `index` and session book-keeping. ACK is sent back to publishing
    ///   client.
    ack_timestamps: Vec<Timestamp>,

    /// Message::Routed, will messages published by broken sessions.
    will_messages: Timer<ClientID, Message>,
    /// Corresponding MsgTx handle for all other shards, as Shard::MsgTx,
    shard_queues: BTreeMap<u32, Shard<C>>,
    /// MVCC clone of Cluster::cc_topic_filters
    cc_topic_filters: SubscribedTrie,
    /// MVCC clone of Cluster::cc_retained_topics
    cc_retained_topics: RetainedTrie,

    /// statistics
    stats: Stats,
    /// Back channel communicate with application.
    app_tx: AppTx,
}

#[allow(dead_code)]
pub struct ReplicaLoop<C>
where
    C: 'static + Send + ClusterAPI,
{
    /// Mio poller for asynchronous handling, all events are from consensus port and
    /// thread-waker.
    poll: mio::Poll,
    /// Self waker.
    waker: Arc<mio::Waker>,
    /// Cluster::Tx handle to communicate back to cluster. Shall be dropped after
    /// close_wait call, when the thread returns, will point to Inner::Init.
    cluster: Box<C>,

    /// Collection of sessions and corresponding clients managed by this shard. Shall be
    /// dropped after close_wait call, when the thread returns it will be empty.
    sessions: BTreeMap<ClientID, Session>,
    reconnects: Timer<ClientID, Session>,

    /// statistics
    stats: Stats,
    /// Back channel communicate with application.
    app_tx: AppTx,
}

pub struct FinState<C>
where
    C: 'static + Send + ClusterAPI,
{
    pub miot: Miot<Shard<C>>,
    pub sessions: BTreeMap<ClientID, session::Stats>,
    pub reconnects: Vec<session::Stats>,
    pub inp_seqno: InpSeqno,
    pub shard_back_log: BTreeMap<u32, usize>,
    pub ack_timestamps: Vec<Timestamp>,
    pub stats: Stats,
}

impl<C> FinState<C>
where
    C: 'static + Send + ClusterAPI,
{
    fn to_json(&self) -> String {
        format!(
            concat!("{{ {:?}: {}, {:?}: {}, {:?}: {}, {:?}: {} }}"),
            "n_sessions",
            self.sessions.len(),
            "inp_seqno",
            self.inp_seqno,
            "n_events",
            self.stats.n_events,
            "n_requests",
            self.stats.n_requests,
        )
    }
}

#[derive(Clone, Copy, Default)]
pub struct Stats {
    pub n_events: usize,
    pub n_requests: usize,
}

impl<C> Default for Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    fn default() -> Shard<C> {
        let config = Config::default();
        let mut def = Shard {
            name: config.name.clone(),
            shard_id: u32::default(),
            uuid: Uuid::new_v4(),

            prefix: String::default(),
            config,

            inner: Inner::Init,
        };
        def.prefix = def.prefix();
        def
    }
}

impl<C> Shardable for Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    fn uuid(&self) -> Uuid {
        self.uuid
    }
}

impl<C> Drop for Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    fn drop(&mut self) {
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => trace!("{} drop ...", self.prefix),
            Inner::Handle(_hndl) => debug!("{} drop ...", self.prefix),
            Inner::Tx(_waker, _tx) => debug!("{} drop ...", self.prefix),
            Inner::MsgTx(_waker, _tx) => debug!("{} drop ...", self.prefix),
            Inner::MainMaster(_master) => info!("{} drop ...", self.prefix),
            Inner::MainReplica(_replica) => info!("{} drop ...", self.prefix),
            Inner::Close(_fin_state) => debug!("{} drop ...", self.prefix),
        }
    }
}

impl<C> ToJson for Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    fn to_config_json(&self) -> String {
        format!(
            concat!("{{ {:?}: {:?}, {:?}: {}, {:?}: {} }}"),
            "name",
            self.config.name,
            "num_shards",
            self.config.num_shards,
            "pkt_batch_size",
            self.config.pkt_batch_size,
        )
    }

    fn to_stats_json(&self) -> String {
        "{{}}".to_string()
    }
}

pub struct SpawnArgs<C>
where
    C: 'static + Send + ClusterAPI,
{
    pub cluster: C,
    pub flusher: Flusher,
    pub cc_topic_filters: SubscribedTrie,
    pub cc_retained_topics: RetainedTrie,
}

impl<C> Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    const WAKE_TOKEN: mio::Token = mio::Token(1);

    pub fn from_config(config: &Config, shard_id: u32) -> Result<Shard<C>> {
        let def: Shard<C> = Shard::default();
        let mut val = Shard {
            name: config.name.clone(),
            shard_id,
            uuid: def.uuid,

            prefix: def.prefix.clone(),
            config: config.clone(),

            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn_master(self, args: SpawnArgs<C>, app_tx: &AppTx) -> Result<Shard<C>> {
        let num_shards = self.config.num_shards;

        let poll = mio::Poll::new()?;
        let waker = Arc::new(mio::Waker::new(poll.registry(), Self::WAKE_TOKEN)?);

        // This is the local queue that carries [Message] from one local-session
        // to another local-session. Note that the queue is shared by all the sessions
        // in this shard, hence the queue-capacity is correspondingly large.
        let (msg_tx, msg_rx) = {
            let size = self.config.pkt_batch_size * num_shards;
            message::msg_channel(self.shard_id, size as usize, Arc::clone(&waker))
        };
        let mut shard = Shard {
            name: self.config.name.clone(),
            shard_id: self.shard_id,
            uuid: self.uuid,
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::MainMaster(MasterLoop {
                poll,
                waker: Arc::clone(&waker),
                cluster: Box::new(args.cluster),
                flusher: args.flusher,
                miot: Miot::default(),

                sessions: BTreeMap::default(),
                reconnects: Timer::default(),
                inp_seqno: 1,
                shard_back_log: BTreeMap::default(),
                index: BTreeMap::default(),
                ack_timestamps: Vec::default(),

                will_messages: Timer::default(),

                shard_queues: BTreeMap::default(),
                cc_topic_filters: args.cc_topic_filters,
                cc_retained_topics: args.cc_retained_topics,

                stats: Stats::default(),
                app_tx: app_tx.clone(),
            }),
        };
        shard.prefix = shard.prefix();
        let mut thrd = Thread::spawn(&self.prefix, shard);
        thrd.set_waker(Arc::clone(&waker));

        let mut shard = Shard {
            name: self.config.name.clone(),
            shard_id: self.shard_id,
            uuid: self.uuid,
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::Handle(Handle { waker, thrd, msg_tx: Some(msg_tx) }),
        };
        shard.prefix = shard.prefix();

        {
            let (config, miot_id) = (self.config.clone(), self.shard_id);
            let miot = {
                let miot = Miot::from_config(config, miot_id)?;
                miot.spawn(shard.to_tx("miot"), app_tx.clone())?
            };
            match &shard.inner {
                Inner::Handle(Handle { thrd, .. }) => {
                    thrd.request(Request::SetMiot(miot, msg_rx))??;
                }
                inner => unreachable!("{} {:?}", self.prefix, inner),
            }
        }

        Ok(shard)
    }

    pub fn spawn_replica(self, args: SpawnArgs<C>, app_tx: &AppTx) -> Result<Shard<C>> {
        let poll = mio::Poll::new()?;
        let waker = Arc::new(mio::Waker::new(poll.registry(), Self::WAKE_TOKEN)?);

        let mut shard = Shard {
            name: self.config.name.clone(),
            shard_id: self.shard_id,
            uuid: self.uuid,
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::MainReplica(ReplicaLoop {
                poll,
                waker: Arc::clone(&waker),
                cluster: Box::new(args.cluster),

                sessions: BTreeMap::default(),
                reconnects: Timer::default(),
                stats: Stats::default(),
                app_tx: app_tx.clone(),
            }),
        };
        shard.prefix = shard.prefix();
        let mut thrd = Thread::spawn(&self.prefix, shard);
        thrd.set_waker(Arc::clone(&waker));

        let mut shard = Shard {
            name: self.config.name.clone(),
            shard_id: self.shard_id,
            uuid: self.uuid,
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::Handle(Handle { waker, thrd, msg_tx: None }),
        };
        shard.prefix = shard.prefix();

        Ok(shard)
    }

    pub fn to_tx(&self, who: &str) -> Self {
        let inner = match &self.inner {
            Inner::Handle(Handle { waker, thrd, .. }) => {
                Inner::Tx(Arc::clone(waker), thrd.to_tx())
            }
            Inner::Tx(waker, tx) => Inner::Tx(Arc::clone(waker), tx.clone()),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let mut shard = Shard {
            name: self.config.name.clone(),
            shard_id: self.shard_id,
            uuid: self.uuid,
            prefix: String::default(),
            config: self.config.clone(),
            inner,
        };
        shard.prefix = shard.prefix();

        debug!("{} cloned for {}", shard.prefix, who);
        shard
    }

    pub fn to_msg_tx(&self) -> Self {
        let inner = match &self.inner {
            Inner::Handle(Handle { waker, msg_tx: Some(msg_tx), .. }) => {
                Inner::MsgTx(Arc::clone(waker), msg_tx.clone())
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let mut shard = Shard {
            name: self.config.name.clone(),
            shard_id: self.shard_id,
            uuid: self.uuid,
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner,
        };
        shard.prefix = shard.prefix();

        shard
    }
}

pub enum Request<C>
where
    C: 'static + Send + ClusterAPI,
{
    SetMiot(Miot<Shard<C>>, MsgRx),
    SetShardQueues(BTreeMap<u32, Shard<C>>),
    AddSession(Socket),
    FlushSession { pq: PQueue, err: Option<Error> },
    Close,
}

pub enum Response {
    Ok,
}

// calls to interface with shard-thread.
impl<C> Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    pub fn wake(&self) -> Result<()> {
        match &self.inner {
            Inner::Handle(Handle { waker, .. }) => Ok(waker.wake()?),
            Inner::Tx(waker, _) => Ok(waker.wake()?),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn set_shard_queues(&self, shards: BTreeMap<u32, Shard<C>>) {
        match &self.inner {
            Inner::Handle(Handle { thrd, .. }) => {
                let req = Request::SetShardQueues(shards);
                app_fatal!(self, thrd.request(req).flatten());
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn add_session(&self, sock: Socket) {
        match &self.inner {
            Inner::Handle(Handle { thrd, .. }) => {
                let req = Request::AddSession(sock);
                app_fatal!(self, thrd.request(req).flatten());
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn flush_session(&self, pq: PQueue, err: Option<Error>) {
        match &self.inner {
            Inner::Tx(_waker, tx) => {
                let req = Request::FlushSession { pq, err };
                app_fatal!(self, tx.post(req));
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn send_messages(&mut self, msgs: Vec<Message>) -> QueueStatus<Message> {
        match &mut self.inner {
            Inner::MsgTx(_waker, msg_tx) => msg_tx.try_sends(msgs),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn close_wait(mut self) -> Shard<C> {
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(Handle { thrd, .. }) => {
                app_fatal!(self, thrd.request(Request::Close));
                thrd.close_wait()
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

impl<C> Threadable for Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    type Req = Request<C>;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: ThreadRx<C>) -> Self {
        match &self.inner {
            Inner::MainMaster(_) => {
                // this a work around to wire up all the threads without using unsafe.
                let msg_rx = match rx.recv().unwrap() {
                    (Request::SetMiot(miot, msg_rx), Some(tx)) => {
                        let master_loop = match &mut self.inner {
                            Inner::MainMaster(master_loop) => master_loop,
                            inner => unreachable!("{} {:?}", self.prefix, inner),
                        };
                        master_loop.miot = miot;
                        app_fatal!(&self, tx.send(Ok(Response::Ok)));
                        msg_rx
                    }
                    _ => unreachable!(),
                };

                self.master_loop(rx, msg_rx)
            }
            Inner::MainReplica(_) => self.replica_loop(rx),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

struct MasterContext<C>
where
    C: 'static + Send + ClusterAPI,
{
    msg_rx: MsgRx,
    inner: Inner<C>,
    session: Session,
    cons_io: ConsensIO,
    disconnected: bool,
}

impl<C> MasterContext<C>
where
    C: 'static + Send + ClusterAPI,
{
    fn new(msg_rx: MsgRx, inner: Inner<C>) -> MasterContext<C> {
        MasterContext {
            msg_rx,
            inner,
            session: Session::default(),
            cons_io: ConsensIO::default(),
            disconnected: false,
        }
    }
}

impl<C> Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    fn master_loop(mut self, rx: ThreadRx<C>, mut msg_rx: MsgRx) -> Self {
        use crate::broker::POLL_EVENTS_SIZE;

        info!("{} spawn config:{}", self.prefix, self.to_config_json());

        let timeout: Option<time::Duration> = None;
        let mut events = mio::Events::with_capacity(POLL_EVENTS_SIZE);
        loop {
            if let Err(err) = self.as_mut_poll().poll(&mut events, timeout) {
                self.as_app_tx().send("exit".to_string()).ok();
                error!("{} thread error exit {} ", self.prefix, err);
                break;
            }

            let mut cons_io = ConsensIO::default();

            match self.mio_events(&rx, &events, &mut cons_io) {
                true => break,
                _exit => (),
            };

            let mut inner = mem::replace(&mut self.inner, Inner::Init);
            let mut args = MasterContext::new(msg_rx, inner);

            self.master_pre_cs(&mut args, &mut cons_io);
            self.master_cs(&mut args, &mut cons_io);
            self.return_local_acks(&mut args, &mut cons_io);
            self.master_post_cs(&mut args, &mut cons_io);
            self.inc_publish_ack(&mut args, &mut cons_io);
            self.expire_reconnects();

            msg_rx = args.msg_rx;
            inner = args.inner;
            let _init = mem::replace(&mut self.inner, inner);

            // wake up miot every time shard wakes up
            self.as_miot().wake()
        }

        match &self.inner {
            Inner::MainMaster(_) => self.handle_close_a(Request::Close),
            Inner::Close(_) => Response::Ok,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        info!("{} thread exit", self.prefix);
        self
    }

    fn master_pre_cs(&mut self, args: &mut MasterContext<C>, cons_io: &mut ConsensIO) {
        let sessions = match &mut args.inner {
            Inner::MainMaster(MasterLoop { sessions, .. }) => {
                mem::replace(sessions, BTreeMap::default())
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let mut fail_sessions: Vec<FailSession> = Vec::default();
        let mut route_io = {
            let mut val = RouteIO::default();
            val.cons_io = mem::replace(cons_io, ConsensIO::default());
            val
        };
        let mut sessions1: BTreeMap<ClientID, Session> = BTreeMap::default();
        for (client_id, session) in sessions.into_iter() {
            let _empty_session = mem::replace(&mut args.session, session);

            match args.session.route_packets(self, &mut route_io) {
                Ok(()) if !route_io.disconnected => {
                    self.book_incoming_msgs(args, &mut route_io);
                    self.send_to_shards();

                    let session = mem::replace(&mut args.session, _empty_session);
                    sessions1.insert(client_id, session);
                }
                Ok(()) => {
                    let session = mem::replace(&mut args.session, _empty_session);
                    let err = err_disconnected(&self.prefix).err();
                    fail_sessions.push(FailSession { session, err });
                }
                Err(err) => {
                    let session = mem::replace(&mut args.session, _empty_session);
                    fail_sessions.push(FailSession { session, err: Some(err) });
                }
            };
            route_io = route_io.reset_session();
            args.disconnected = false;
        }

        self.clean_failed_sessions(fail_sessions, cons_io);
        let _empty = match &mut args.inner {
            Inner::MainMaster(MasterLoop { sessions, .. }) => {
                mem::replace(sessions, sessions1)
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let _empty = mem::replace(cons_io, route_io.cons_io);
    }

    fn master_cs(&mut self, args: &mut MasterContext<C>, cons_io: &mut ConsensIO) {
        let sessions = match &mut args.inner {
            Inner::MainMaster(MasterLoop { sessions, .. }) => {
                mem::replace(sessions, BTreeMap::default())
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let mut fail_sessions: Vec<FailSession> = Vec::default();
        let mut sessions1: BTreeMap<ClientID, Session> = BTreeMap::default();
        for (client_id, session) in sessions.into_iter() {
            let _empty_session = mem::replace(&mut args.session, session);

            self.rx_messages(args, cons_io);

            let oug_qos0 = cons_io.oug_qos0.remove(&client_id).unwrap_or(Vec::default());
            if args.session.tx_oug_back_log(oug_qos0).is_disconnected() {
                let session = mem::replace(&mut args.session, _empty_session);
                let err = err_disconnected(&self.prefix).err();
                fail_sessions.push(FailSession { session, err });
            } else if args.disconnected {
                let session = mem::replace(&mut args.session, _empty_session);
                let err = err_disconnected(&self.prefix).err();
                fail_sessions.push(FailSession { session, err });
            } else {
                let session = mem::replace(&mut args.session, _empty_session);
                cons_io.oug_seqno.insert(client_id.clone(), session.to_cs_oug_seqno());
                sessions1.insert(client_id, session);
            }
            args.disconnected = false;
        }

        // TODO: replicate relevant fields of cons_io in the consensus loop.
        // TODO: fetch msgs from consensus loop and start commiting.

        self.clean_failed_sessions(fail_sessions, cons_io);
        let _empty = match &mut args.inner {
            Inner::MainMaster(MasterLoop { sessions, .. }) => {
                mem::replace(sessions, sessions1)
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };
    }

    fn master_post_cs(&mut self, args: &mut MasterContext<C>, cons_io: &mut ConsensIO) {
        let sessions = match &mut args.inner {
            Inner::MainMaster(MasterLoop { sessions, .. }) => {
                mem::replace(sessions, BTreeMap::default())
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let mut fail_sessions: Vec<FailSession> = Vec::default();
        let mut sessions1: BTreeMap<ClientID, Session> = BTreeMap::default();
        for (client_id, session) in sessions.into_iter() {
            let _empty_session = mem::replace(&mut args.session, session);

            let subs = match args.cons_io.oug_subs.remove(&client_id) {
                Some(subs) => subs,
                None => Vec::default(),
            };
            let unsubs = match args.cons_io.oug_unsubs.remove(&client_id) {
                Some(unsubs) => unsubs,
                None => Vec::default(),
            };
            let oug_qos12 = match args.cons_io.oug_qos12.remove(&client_id) {
                Some(oug_qos12) => oug_qos12,
                None => Vec::default(),
            };

            let (disconnected, err) = match args.session.commit_subs(self, subs) {
                Ok(status) => (status.is_disconnected(), None),
                Err(err) => (true, Some(err)),
            };

            let (disconnected, err) = if !disconnected {
                match args.session.commit_unsubs(self, unsubs) {
                    Ok(status) => (status.is_disconnected(), None),
                    Err(err) => (true, Some(err)),
                }
            } else {
                (disconnected, err)
            };

            let (disconnected, err) = if !disconnected {
                match args.session.commit_cs_oug_back_log(oug_qos12) {
                    Ok(QueueStatus::Disconnected(_)) => (true, None),
                    Ok(_) => (false, None),
                    Err(err) => (true, Some(err)),
                }
            } else {
                (disconnected, err)
            };

            let (disconnected, err) = if !disconnected {
                match args.session.retry_publish() {
                    Ok(status) => (status.is_disconnected(), None),
                    Err(err) => (true, Some(err)),
                }
            } else {
                (disconnected, err)
            };

            if disconnected {
                let session = mem::replace(&mut args.session, _empty_session);
                fail_sessions.push(FailSession { session, err });
            } else {
                let session = mem::replace(&mut args.session, _empty_session);
                sessions1.insert(client_id, session);
            }

            args.disconnected = false;
        }

        self.clean_failed_sessions(fail_sessions, cons_io);
        let _empty = match &mut args.inner {
            Inner::MainMaster(MasterLoop { sessions, .. }) => {
                mem::replace(sessions, sessions1)
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };
    }

    fn book_incoming_msgs(&mut self, args: &mut MasterContext<C>, rio: &mut RouteIO) {
        use crate::PacketType;

        let MasterLoop { index, shard_back_log, ack_timestamps, .. } =
            match &mut self.inner {
                Inner::MainMaster(master_loop) => master_loop,
                inner => unreachable!("{} {:?}", self.prefix, inner),
            };

        let n = rio.oug_msgs.len();
        let oug_msgs = mem::replace(&mut rio.oug_msgs, Vec::with_capacity(n));

        let mut oug_acks = Vec::default();
        let mut oug_retain0 = Vec::default();
        let mut oug_retain12 = Vec::default();

        for mut msg in oug_msgs.into_iter() {
            match &mut msg {
                Message::ClientAck { packet } => match packet.to_packet_type() {
                    PacketType::PingResp => oug_acks.push(msg),
                    PacketType::PubAck => oug_acks.push(msg),
                    _ => unreachable!(),
                },
                Message::Retain { out_seqno, publish } if publish.is_qos0() => {
                    *out_seqno = args.session.incr_oug_qos0();
                    oug_retain0.push(msg);
                }
                Message::Retain { .. } => oug_retain12.push(msg),
                Message::Subscribe { .. } => {
                    let client_id = &args.session.client_id;
                    append_index!(&mut rio.cons_io.oug_subs, client_id, msg);
                }
                Message::UnSubscribe { .. } => {
                    let client_id = &args.session.client_id;
                    append_index!(&mut rio.cons_io.oug_unsubs, client_id, msg);
                }
                Message::ShardIndex { inp_seqno, qos, .. } => match qos {
                    QoS::AtMostOnce => (),
                    QoS::AtLeastOnce => {
                        debug_assert!(index.insert(*inp_seqno, msg).is_none());
                    }
                    QoS::ExactlyOnce => {
                        debug_assert!(index.insert(*inp_seqno, msg).is_none());
                    }
                },
                Message::Routed { inp_seqno, dst_shard_id, .. } => {
                    match ack_timestamps
                        .binary_search_by_key(dst_shard_id, |t| t.dst_shard_id)
                    {
                        Ok(off) => ack_timestamps[off].last_routed = *inp_seqno,
                        Err(off) => {
                            let t = Timestamp::new(*dst_shard_id, *inp_seqno);
                            ack_timestamps.insert(off, t);
                        }
                    }

                    append_index!(shard_back_log, dst_shard_id, msg);
                }
                msg => unreachable!("{:?}", msg),
            }
        }

        let status = args.session.tx_oug_acks(oug_acks);
        rio.disconnected = matches!(status, QueueStatus::Disconnected(_));

        let status = args.session.tx_oug_back_log(oug_retain0);
        rio.disconnected = matches!(status, QueueStatus::Disconnected(_));

        rio.disconnected = match args.session.incr_oug_qos12(oug_retain12) {
            Ok(QueueStatus::Ok(msgs)) => {
                let client_id = &args.session.client_id;
                appends_index!(&mut rio.cons_io.oug_qos12, client_id, msgs);
                false
            }
            Ok(QueueStatus::Block(_)) | Ok(QueueStatus::Disconnected(_)) => true,
            Err(_err) => true,
        };
    }

    // Flush outgoing messages, in `shard_back_log` from this shard to other shards.
    fn send_to_shards(&mut self) {
        let MasterLoop { shard_back_log, shard_queues, .. } = match &mut self.inner {
            Inner::MainMaster(master_loop) => master_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let back_log = mem::replace(shard_back_log, BTreeMap::default());
        for (shard_id, msgs) in back_log.into_iter() {
            let shard = shard_queues.get_mut(&shard_id).unwrap();

            let mut status = shard.send_messages(msgs);
            // re-index the remaining messages, may be the other shard is busy.
            shard_back_log.insert(shard_id, status.take_values());

            if matches!(status, QueueStatus::Disconnected(_)) {
                error!(
                    "{} shard_id:{} fatal !! shard-msg-rx has closed",
                    self.prefix, shard_id
                )
            }
        }
    }

    fn rx_messages(&mut self, args: &mut MasterContext<C>, cons_io: &mut ConsensIO) {
        let MasterLoop { ack_timestamps, .. } = match &mut args.inner {
            Inner::MainMaster(master_loop) => master_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        // receive messages targeting all the sessions hosted in this shard.
        let mut status = args.msg_rx.try_recvs();

        let mut oug_qos12 = Vec::default();
        for mut msg in status.take_values().into_iter() {
            match &mut msg {
                Message::LocalAck { shard_id, last_acked } => {
                    match ack_timestamps
                        .binary_search_by_key(shard_id, |t| t.dst_shard_id)
                    {
                        Ok(off) => ack_timestamps[off].last_acked = *last_acked,
                        Err(_) => unreachable!(),
                    }
                }
                Message::Routed { publish, .. } if publish.is_qos12() => {
                    oug_qos12.push(msg);
                }
                Message::Routed { client_id, out_seqno, .. } => {
                    *out_seqno = args.session.incr_oug_qos0();
                    append_index!(&mut cons_io.oug_qos0, client_id, msg);
                }
                _ => unreachable!(),
            };
        }

        args.disconnected = match args.session.incr_oug_qos12(oug_qos12) {
            Ok(QueueStatus::Ok(msgs)) => {
                let client_id = &args.session.client_id;
                appends_index!(&mut cons_io.oug_qos12, client_id, msgs);
                false
            }
            Ok(QueueStatus::Block(_)) | Ok(QueueStatus::Disconnected(_)) => true,
            Err(_err) => true,
        };
    }

    fn return_local_acks(
        &mut self,
        args: &mut MasterContext<C>,
        cons_io: &mut ConsensIO,
    ) {
        let MasterLoop { shard_back_log, .. } = match &mut args.inner {
            Inner::MainMaster(master_loop) => master_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let mut acks = vec![0; self.config.num_shards as usize];
        for (_client_id, oug_qos12) in cons_io.oug_qos12.iter() {
            for msg in oug_qos12.iter() {
                match msg {
                    Message::Routed { src_shard_id, inp_seqno, .. } => {
                        acks[*src_shard_id as usize] = *inp_seqno;
                    }
                    _ => unreachable!(),
                }
            }
        }

        for (src_shard_id, inp_seqno) in acks.into_iter().enumerate() {
            let src_shard_id = src_shard_id as u32;
            if inp_seqno > 0 {
                let msg = Message::new_local_ack(src_shard_id, inp_seqno);
                append_index!(shard_back_log, &src_shard_id, msg);
            }
        }

        self.send_to_shards();
    }

    fn inc_publish_ack(&mut self, args: &mut MasterContext<C>, cons_io: &mut ConsensIO) {
        let (index, sessions, ack_timestamps) = match &mut args.inner {
            Inner::MainMaster(MasterLoop { index, sessions, ack_timestamps, .. }) => {
                (index, sessions, ack_timestamps)
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let last_acked: InpSeqno =
            ack_timestamps.iter().map(|ts| ts.last_acked).min().unwrap_or(0);

        let mut inp_seqnos = Vec::default();
        let mut oug_acks: BTreeMap<ClientID, Vec<Message>> = BTreeMap::default();
        for (inp_seqno, msg) in index.iter() {
            if *inp_seqno > last_acked {
                continue;
            }

            inp_seqnos.push(*inp_seqno);
            if let Message::ShardIndex { src_client_id, packet_id, .. } = msg {
                let msg = {
                    let session = sessions.get(src_client_id).unwrap();
                    let pkt = session.as_protocol().new_pub_ack(*packet_id);
                    Message::new_pub_ack(pkt)
                };
                match oug_acks.get_mut(src_client_id) {
                    Some(val) => val.push(msg),
                    None => {
                        oug_acks.insert(src_client_id.clone(), vec![msg]);
                    }
                }
            }
        }

        for inp_seqno in inp_seqnos.into_iter() {
            index.remove(&inp_seqno);
        }

        mem::drop(index);
        mem::drop(sessions);
        mem::drop(ack_timestamps);

        let sessions = match &mut args.inner {
            Inner::MainMaster(MasterLoop { sessions, .. }) => {
                mem::replace(sessions, BTreeMap::default())
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let mut fail_sessions: Vec<FailSession> = Vec::default();
        let mut sessions1: BTreeMap<ClientID, Session> = BTreeMap::default();
        for (client_id, mut session) in sessions.into_iter() {
            let disconnected = match oug_acks.remove(&client_id) {
                Some(msgs) => {
                    matches!(session.tx_oug_acks(msgs), QueueStatus::Disconnected(_))
                }
                None => false,
            };
            if disconnected {
                let err = err_disconnected(&self.prefix).err();
                fail_sessions.push(FailSession { session, err });
            } else {
                sessions1.insert(client_id, session);
            }
        }

        assert!(oug_acks.len() == 0);

        self.clean_failed_sessions(fail_sessions, cons_io);
        let _empty = match &mut args.inner {
            Inner::MainMaster(MasterLoop { sessions, .. }) => {
                mem::replace(sessions, sessions1)
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };
    }

    fn expire_reconnects(&mut self) {
        let MasterLoop { reconnects, .. } = match &mut self.inner {
            Inner::MainMaster(master_loop) => master_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        for session in reconnects.expired() {
            info!("{} session:{} reconnect expired", self.prefix, *session.client_id);
        }
    }

    fn remove_session(&mut self, client_id: &ClientID) {
        match &mut self.inner {
            Inner::MainMaster(MasterLoop {
                index,
                sessions,
                reconnects,
                cc_topic_filters,
                ..
            }) => {
                let mut inp_seqnos = Vec::default();
                for (_seqno, msg) in index.iter() {
                    if let Message::ShardIndex { src_client_id, inp_seqno, .. } = msg {
                        if src_client_id == client_id {
                            inp_seqnos.push(*inp_seqno);
                        }
                    }
                }
                for inp_seqno in inp_seqnos.into_iter() {
                    index.remove(&inp_seqno);
                }

                if let Some(mut session) = sessions.remove(client_id) {
                    session.remove_topic_filters(cc_topic_filters);
                    reconnects.add_timeout(
                        u64::from(session.to_session_expiry_interval().unwrap_or(0)),
                        client_id.clone(),
                        session.into_reconnect(),
                    );
                } else {
                    warn!("{} client_id:{} session not found", self.prefix, client_id);
                }
            }
            Inner::MainReplica(ReplicaLoop { sessions, reconnects, .. }) => {
                if let Some(session) = sessions.remove(client_id) {
                    reconnects.add_timeout(
                        0, // NOTE: reconnect never expires on the replica.
                        client_id.clone(),
                        session.into_reconnect(),
                    );
                } else {
                    warn!("{} client_id:{} session not found", self.prefix, client_id);
                }
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };
    }
}

impl<C> Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    fn replica_loop(mut self, rx: ThreadRx<C>) -> Self {
        use crate::broker::POLL_EVENTS_SIZE;

        info!("{} spawn config:{}", self.prefix, self.to_config_json());

        let mut events = mio::Events::with_capacity(POLL_EVENTS_SIZE);
        loop {
            let timeout: Option<time::Duration> = None;
            if let Err(err) = self.as_mut_poll().poll(&mut events, timeout) {
                self.as_app_tx().send("exit".to_string()).ok();
                error!("{} thread error exit {} ", self.prefix, err);
                break;
            }

            let mut cons_io = ConsensIO::default();

            match self.mio_events(&rx, &events, &mut cons_io) {
                true => break,
                _exit => (),
            };

            // TODO: fetch ack_out_seqnos and qos_msgs from consensus loop
            // and commit here.
            // TODO uncomment the following two blocks
            //let ack_out_seqnos = BTreeMap::<ClientID, Vec<OutSeqno>>::default();
            //let qos_msgs: Vec<Message> = Vec::default();

            //self.commit_acks(ack_out_seqnos);
            //self.commit_messages(qos_msgs, &mut BTreeMap::default());
        }

        match &self.inner {
            Inner::MainReplica(_) => self.handle_close_r(Request::Close),
            Inner::Close(_) => Response::Ok,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        info!("{} thread exit", self.prefix);
        self
    }
}

impl<C> Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    // (exit,)
    fn mio_events(
        &mut self,
        rx: &ThreadRx<C>,
        events: &mio::Events,
        cons_io: &mut ConsensIO,
    ) -> bool {
        let mut count = 0;
        for _event in events.iter() {
            count += 1;
        }
        self.incr_n_events(count);

        loop {
            // keep repeating until all control requests are drained.
            match self.drain_control_chan(rx, cons_io) {
                (_, true) => break true,
                (QueueStatus::Ok(_), false) => (),
                (QueueStatus::Block(_), _) => break false,
                (QueueStatus::Disconnected(_), _) => break true,
            }
        }
    }

    // Return (queue-status, disconnected)
    fn drain_control_chan(
        &mut self,
        rx: &ThreadRx<C>,
        cons_io: &mut ConsensIO,
    ) -> (QueueReq<C>, bool) {
        use crate::broker::{thread::pending_requests, CONTROL_CHAN_SIZE};
        use Request::*;

        let mut status = pending_requests(&self.prefix, &rx, CONTROL_CHAN_SIZE);
        let reqs = status.take_values();

        self.incr_n_requests(reqs.len());

        let mut closed = false;
        for req in reqs.into_iter() {
            match req {
                (req @ SetShardQueues(_), Some(tx)) => {
                    let resp = self.handle_set_shard_queues(req);
                    err!(IPCFail, try: tx.send(Ok(resp))).ok();
                }
                (req @ AddSession { .. }, Some(tx)) => {
                    let resp = self.handle_add_session_a(req, cons_io);
                    err!(IPCFail, try: tx.send(Ok(resp))).ok();
                }
                (req @ FlushSession { .. }, None) => {
                    let _resp = self.handle_flush_session(req, cons_io);
                }
                (req @ Close, Some(tx)) => {
                    let resp = self.handle_close_a(req);
                    err!(IPCFail, try: tx.send(Ok(resp))).ok();
                    closed = true;
                }

                (_, _) => unreachable!(),
            };
        }

        (status, closed)
    }
}

// Handle incoming messages
impl<C> Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    fn clean_failed_sessions(
        &mut self,
        mut fail_sessions: Vec<FailSession>,
        cons_io: &mut ConsensIO,
    ) {
        fail_sessions.sort_by_key(|a| a.session.client_id.clone());
        fail_sessions.dedup_by_key(|a| a.session.client_id.clone());

        for FailSession { session, err } in fail_sessions.into_iter() {
            let miot = self.as_mut_miot();

            match miot.remove_connection(&session.client_id) {
                Ok(Some(pq)) => {
                    let req = Request::FlushSession { pq, err };
                    self.handle_flush_session(req, cons_io);
                }
                Ok(None) => {
                    debug!(
                        "{} client_id:{} connection already removed",
                        self.prefix, *session.client_id
                    );
                }
                Err(err) => {
                    self.as_app_tx().send("fatal".to_string()).ok();
                    error!("{} fatal error {} ", self.prefix, err);
                }
            }
        }
    }
}

// Handle out-going messages
impl<C> Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    fn handle_set_shard_queues(&mut self, req: Request<C>) -> Response {
        let shard_queues = match req {
            Request::SetShardQueues(shard_queues) => shard_queues,
            _ => unreachable!(),
        };

        match &mut self.inner {
            Inner::MainMaster(master_loop) => {
                master_loop.shard_queues = shard_queues;
                Response::Ok
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn handle_add_session_a(
        &mut self,
        req: Request<C>,
        cons_io: &mut ConsensIO,
    ) -> Response {
        use crate::broker::PQueueArgs;

        let sock = match req {
            Request::AddSession(sock) => sock,
            _ => unreachable!(),
        };

        // create the session
        let (mut session, session_tx, miot_rx) = {
            let (args, upstream, downstream) = self.to_args_master(&sock);
            let session = self.new_session_a(&sock, args, cons_io);
            (session, upstream, downstream)
        };

        let raddr = sock.peer_addr();

        // send back the connection acknowledgment CONNACK here.
        {
            let packet = sock.success_ack();
            let status = session.tx_oug_acks(vec![Message::new_conn_ack(packet)]);

            match status {
                QueueStatus::Ok(_) | QueueStatus::Block(_) => {
                    info!("{} raddr:{} send CONNACK", self.prefix, raddr);
                }
                QueueStatus::Disconnected(_) => {
                    error!("{} raddr:{} fail to send CONNACK", self.prefix, raddr);
                    return Response::Ok;
                }
            }
        }

        // add_connection to miot
        {
            let args = PQueueArgs {
                config: self.config.clone(),
                socket: sock,
                session_tx,
                miot_rx,
            };
            self.as_miot().add_connection(args);
        }

        let MasterLoop { sessions, .. } = match &mut self.inner {
            Inner::MainMaster(master_loop) => master_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };
        sessions.insert(session.client_id.clone(), session);
        info!("{} raddr:{} adding new session to shard", self.prefix, raddr);

        Response::Ok
    }

    #[allow(dead_code)] // TODO: wire this up with consensus loop
    fn handle_add_session_r(&mut self, msg: Message) -> Response {
        let session = {
            let args = msg.into_session_args_replica();
            self.new_session_r(args)
        };
        let raddr = session.raddr;

        let ReplicaLoop { sessions, .. } = match &mut self.inner {
            Inner::MainReplica(replica_loop) => replica_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };
        sessions.insert(session.client_id.clone(), session);
        info!("{} raddr:{} adding new replica session to shard", self.prefix, raddr);

        Response::Ok
    }

    fn handle_flush_session(
        &mut self,
        r: Request<C>,
        cons_io: &mut ConsensIO,
    ) -> Response {
        let (pq, err) = match r {
            Request::FlushSession { pq, err } => (pq, err),
            _ => unreachable!(),
        };

        let client_id = pq.to_client_id();

        self.remove_session(&client_id);

        let MasterLoop { flusher, .. } = match &mut self.inner {
            Inner::MainMaster(master_loop) => master_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };
        flusher.flush_connection(pq, err);

        let msg = Message::new_rem_session(self.shard_id, client_id);
        cons_io.ctrl_msgs.push(msg);
        Response::Ok
    }

    fn handle_close_a(&mut self, _req: Request<C>) -> Response {
        let mut master_loop = match mem::replace(&mut self.inner, Inner::Init) {
            Inner::MainMaster(master_loop) => master_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        info!("{} closing shard", self.prefix);

        let miot = mem::replace(&mut master_loop.miot, Miot::default()).close_wait();

        mem::drop(master_loop.poll);
        mem::drop(master_loop.waker);
        mem::drop(master_loop.cluster);
        mem::drop(master_loop.flusher);

        mem::drop(master_loop.shard_queues);
        mem::drop(master_loop.cc_topic_filters);
        mem::drop(master_loop.cc_retained_topics);

        let mut new_sessions = BTreeMap::default();
        for (client_id, sess) in master_loop.sessions.into_iter() {
            match sess.close() {
                Ok(stat) => {
                    new_sessions.insert(client_id, stat);
                }
                Err(err) => {
                    error!(
                        "{} session:{} err:{} fail closing",
                        self.prefix, *client_id, err
                    );
                }
            }
        }

        let mut stats = Vec::default();
        for session in master_loop.reconnects.close() {
            let client_id = session.client_id.clone();
            match session.close() {
                Ok(stat) => stats.push(stat),
                Err(err) => {
                    error!(
                        "{} session:{} err:{} fail closing",
                        self.prefix, *client_id, err
                    );
                }
            }
        }
        let fin_state = FinState {
            miot,
            sessions: new_sessions,
            reconnects: stats,
            inp_seqno: master_loop.inp_seqno,
            shard_back_log: BTreeMap::default(), // TODO
            ack_timestamps: master_loop.ack_timestamps,
            stats: master_loop.stats,
        };
        info!("{} stats:{}", self.prefix, fin_state.to_json());

        let _init = mem::replace(&mut self.inner, Inner::Close(fin_state));
        self.prefix = self.prefix();

        Response::Ok
    }

    #[allow(dead_code)] // TODO: wire this up with consensus loop
    fn handle_close_r(&mut self, _req: Request<C>) -> Response {
        let replica_loop = match mem::replace(&mut self.inner, Inner::Init) {
            Inner::MainReplica(replica_loop) => replica_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        info!("{} closing shard", self.prefix);

        mem::drop(replica_loop.poll);
        mem::drop(replica_loop.waker);
        mem::drop(replica_loop.cluster);

        let mut new_sessions = BTreeMap::default();
        for (client_id, sess) in replica_loop.sessions.into_iter() {
            match sess.close() {
                Ok(stat) => {
                    new_sessions.insert(client_id, stat);
                }
                Err(err) => {
                    error!(
                        "{} session:{} err:{} fail closing",
                        self.prefix, *client_id, err
                    );
                }
            }
        }

        let mut stats = Vec::default();
        for session in replica_loop.reconnects.close() {
            let client_id = session.client_id.clone();
            match session.close() {
                Ok(stat) => stats.push(stat),
                Err(err) => {
                    error!(
                        "{} session:{} err:{} fail closing",
                        self.prefix, *client_id, err
                    );
                }
            }
        }
        let fin_state = FinState {
            miot: Miot::default(),
            sessions: new_sessions,
            reconnects: stats,
            inp_seqno: 0,
            shard_back_log: BTreeMap::default(), // TODO
            ack_timestamps: Vec::default(),
            stats: Stats::default(),
        };

        info!("{} stats:{}", self.prefix, fin_state.to_json());

        let _init = mem::replace(&mut self.inner, Inner::Close(fin_state));
        self.prefix = self.prefix();

        Response::Ok
    }
}

impl<C> Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    // Return (new_session, session_tx (upstream), miot_rx (downstream))
    fn to_args_master(
        &mut self,
        sock: &Socket,
    ) -> (SessionArgsMaster, PacketTx, PacketRx) {
        use crate::new_packet_queue;

        // This queue is wired up with miot-thread. This queue carries QPacket,
        // and there is a separate queue for every session.
        let (upstream, session_rx) = {
            let size = self.config.pkt_batch_size as usize;
            let waker = self.to_waker();
            new_packet_queue(self.shard_id, size, waker)
        };
        // This queue is wired up with miot-thread. This queue carries QPacket,
        // and there is a separate queue for every session.
        let (miot_tx, downstream) = {
            let size = self.config.pkt_batch_size as usize;
            let waker = self.as_miot().to_waker();
            new_packet_queue(self.shard_id, size, waker)
        };

        let args = SessionArgsMaster {
            shard_id: self.shard_id,
            client_id: sock.to_client_id(),
            raddr: sock.peer_addr(),
            config: self.config.clone(),
            proto: sock.to_protocol(),

            miot_tx,
            session_rx,

            client_receive_maximum: sock.receive_maximum(),
            client_keep_alive: sock.keep_alive(),
            client_session_expiry_interval: sock.session_expiry_interval(),
        };

        (args, upstream, downstream)
    }

    fn new_session_a(
        &mut self,
        sock: &Socket,
        args: SessionArgsMaster,
        cons_io: &mut ConsensIO,
    ) -> Session {
        // add_connection further down shall wake miot-thread.
        let MasterLoop { reconnects, miot, .. } = match &mut self.inner {
            Inner::MainMaster(master_loop) => master_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let client_id = args.client_id.clone();
        let clean_start = sock.is_clean_start();
        let reconnect = reconnects.contains(&client_id);

        // new_add_session
        let mut msg = Message::AddSession {
            shard_id: args.shard_id,
            client_id: args.client_id.clone(),
            raddr: args.raddr,
            config: args.config.clone(),
            proto: args.proto.clone(),
            clean_start: false,
        };

        let (session, clean_start) = match miot.remove_connection(&client_id) {
            Ok(None) if reconnect && clean_start => {
                let msg = Message::new_rem_session(self.shard_id, client_id.clone());
                cons_io.ctrl_msgs.push(msg);

                reconnects.delete(&client_id).unwrap();
                let mut iter = reconnects.gc().filter(|s| &s.client_id == &client_id);
                let old_session = iter.next().unwrap();

                info!(
                    "{} old_raddr:{} new_raddr:{} session clean_start",
                    self.prefix, old_session.raddr, args.raddr
                );
                (Session::default().into_master(args), true)
            }
            Ok(None) if reconnect => {
                let msg = Message::new_rem_session(self.shard_id, client_id.clone());
                cons_io.ctrl_msgs.push(msg);

                reconnects.delete(&client_id).unwrap();
                let mut iter = reconnects.gc().filter(|s| &s.client_id == &client_id);
                let old_session = iter.next().unwrap();

                info!(
                    "{} old_raddr:{} new_raddr:{} session reconnect",
                    self.prefix, old_session.raddr, args.raddr
                );
                (old_session.into_master(args), false)
            }
            Ok(None) => {
                let msg = Message::new_rem_session(self.shard_id, client_id.clone());
                cons_io.ctrl_msgs.push(msg);

                (Session::default().into_master(args), true)
            }
            Ok(Some(pq)) => {
                let err: Result<()> = err_session_takeover(&self.prefix, args.raddr);
                let req = Request::FlushSession { pq, err: err.err() };
                self.handle_flush_session(req, cons_io);
                (Session::default().into_master(args), true)
            }
            Err(err) => {
                let msg = Message::new_rem_session(self.shard_id, client_id.clone());
                cons_io.ctrl_msgs.push(msg);

                self.as_app_tx().send("fatal".to_string()).ok();
                error!("{} fatal error {} ", self.prefix, err);
                (Session::default().into_master(args), true)
            }
        };

        msg.set_clean_start(clean_start);
        cons_io.ctrl_msgs.push(msg);

        session
    }

    fn new_session_r(&mut self, args: SessionArgsReplica) -> Session {
        let ReplicaLoop { sessions, .. } = match &mut self.inner {
            Inner::MainReplica(replica_loop) => replica_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let session = match sessions.remove(&args.client_id) {
            Some(session) => {
                info!(
                    "{} old_raddr:{} new_raddr:{} replica:{} session take over",
                    self.prefix,
                    session.raddr,
                    args.raddr,
                    session.is_replica()
                );
                session
            }
            None => Session::default(),
        };

        session.into_replica(args)
    }
}

impl<C> Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    pub fn as_cluster(&self) -> &C {
        match &self.inner {
            Inner::MainMaster(MasterLoop { cluster, .. }) => cluster,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

impl<C> ShardAPI for Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    fn to_shard_id(self) -> u32 {
        self.shard_id
    }

    fn as_topic_filters(&self) -> &SubscribedTrie {
        match &self.inner {
            Inner::MainMaster(MasterLoop { cc_topic_filters, .. }) => cc_topic_filters,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn as_retained_topics(&self) -> &RetainedTrie {
        match &self.inner {
            Inner::MainMaster(MasterLoop { cc_retained_topics, .. }) => {
                cc_retained_topics
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn incr_inp_seqno(&mut self) -> u64 {
        match &mut self.inner {
            Inner::MainMaster(MasterLoop { inp_seqno, .. }) => {
                let seqno = *inp_seqno;
                *inp_seqno = inp_seqno.saturating_add(1);
                seqno
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn wake(&self) -> Result<()> {
        self.wake()
    }

    fn flush_session(&self, pq: PQueue, err: Option<Error>) {
        self.flush_session(pq, err)
    }
}

impl<C> Shard<C>
where
    C: 'static + Send + ClusterAPI,
{
    fn prefix(&self) -> String {
        let state = match &self.inner {
            Inner::Init => "init",
            Inner::Handle(_) => "hndl",
            Inner::Tx(_, _) => "tx",
            Inner::MsgTx(_, _) => "msgtx",
            Inner::MainMaster(_) => "master",
            Inner::MainReplica(_) => "replica",
            Inner::Close(_) => "close",
        };
        format!("<s:{}:{}:{}>", self.name, self.shard_id, state)
    }

    fn incr_n_events(&mut self, count: usize) {
        match &mut self.inner {
            Inner::MainMaster(MasterLoop { stats, .. }) => stats.n_events += count,
            Inner::MainReplica(ReplicaLoop { stats, .. }) => stats.n_events += count,
            Inner::Close(finstate) => finstate.stats.n_events += count,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn incr_n_requests(&mut self, count: usize) {
        match &mut self.inner {
            Inner::MainMaster(MasterLoop { stats, .. }) => stats.n_requests += count,
            Inner::MainReplica(ReplicaLoop { stats, .. }) => stats.n_requests += count,
            Inner::Close(finstate) => finstate.stats.n_requests += count,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn as_mut_poll(&mut self) -> &mut mio::Poll {
        match &mut self.inner {
            Inner::MainMaster(MasterLoop { poll, .. }) => poll,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn as_app_tx(&self) -> &AppTx {
        match &self.inner {
            Inner::MainMaster(MasterLoop { app_tx, .. }) => app_tx,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn as_miot(&self) -> &Miot<Shard<C>> {
        match &self.inner {
            Inner::MainMaster(MasterLoop { miot, .. }) => miot,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn as_mut_miot(&mut self) -> &Miot<Shard<C>> {
        match &mut self.inner {
            Inner::MainMaster(MasterLoop { miot, .. }) => miot,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn to_waker(&self) -> Arc<mio::Waker> {
        match &self.inner {
            Inner::MainMaster(MasterLoop { waker, .. }) => Arc::clone(waker),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

struct FailSession {
    session: Session,
    err: Option<Error>,
}

fn err_session_takeover(prefix: &str, raddr: net::SocketAddr) -> Result<()> {
    err!(SessionTakenOver, code: SessionTakenOver, "{} client {}", prefix, raddr)
}

fn err_disconnected(prefix: &str) -> Result<()> {
    err!(Disconnected, code: ImplementationError, "{}", prefix)
}
