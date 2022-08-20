use log::{debug, error, info, trace, warn};
use uuid::Uuid;

use std::{cmp, collections::BTreeMap, fmt, mem, net, result, sync::Arc};

use crate::broker::session::{self, SessionArgsActive, SessionArgsReplica};
use crate::broker::thread::{Rx, Thread, Threadable, Tx};
use crate::broker::{message, socket};
use crate::broker::{AppTx, Config, RetainedTrie, Session, Shardable, SubscribedTrie};
use crate::broker::{Cluster, Flusher, Message, Miot, MsgRx, QueueStatus, Socket};
use crate::broker::{InpSeqno, OutSeqno, PktRx, PktTx, Timestamp};

use crate::{v5, ClientID, ToJson, TopicName};
use crate::{Error, ErrorKind, ReasonCode, Result};

type ThreadRx = Rx<Request, Result<Response>>;
type QueueReq = crate::broker::thread::QueueReq<Request, Result<Response>>;
type Acks = BTreeMap<u32, InpSeqno>;

macro_rules! append_index {
    ($index:expr, $key:expr, $val:expr) => {{
        match $index.get_mut(&$key) {
            Some(values) => values.push($val),
            None => {
                $index.insert($key, vec![$val]);
            }
        }
    }};
}

/// Type is the workhorse of MQTT, and shall host one or more sessions.
///
/// Handle incoming MQTT packets, route them to other shards, send back acknowledgement,
/// and publish them to other clients.
pub struct Shard {
    /// Human readable name for shard.
    pub name: String,
    /// Shard id, must unique withing the [Cluster].
    pub shard_id: u32,
    /// Unique id for this shard. All shards in a cluster MUST be unique.
    pub uuid: Uuid,
    prefix: String,
    config: Config,
    inner: Inner,
}

pub enum Inner {
    Init,
    // Held by Cluster.
    Handle(Handle),
    // Held by Miot and Ticker
    Tx(Arc<mio::Waker>, Tx<Request, Result<Response>>),
    // Held by all Shard threads.
    MsgTx(Arc<mio::Waker>, message::MsgTx),
    // Thread.
    MainActive(ActiveLoop),
    MainReplica(ReplicaLoop),
    // Held by Cluster, replacing both Handle and Main.
    Close(FinState),
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Inner::Init => write!(f, "Shard::Inner::Init"),
            Inner::Handle(_) => write!(f, "Shard::Inner::Handle"),
            Inner::Tx(_, _) => write!(f, "Shard::Inner::Handle"),
            Inner::MsgTx(_, _) => write!(f, "Shard::Inner::Handle"),
            Inner::MainActive(_) => write!(f, "Shard::Inner::MainActive"),
            Inner::MainReplica(_) => write!(f, "Shard::Inner::MainReplica"),
            Inner::Close(_) => write!(f, "Shard::Inner::Close"),
        }
    }
}

pub struct Handle {
    waker: Arc<mio::Waker>,
    thrd: Thread<Shard, Request, Result<Response>>,
    msg_tx: Option<message::MsgTx>,
}

pub struct ActiveLoop {
    /// Mio poller for asynchronous handling, all events are from consensus port and
    /// thread-waker.
    poll: mio::Poll,
    /// Self waker.
    waker: Arc<mio::Waker>,
    /// Cluster::Tx handle to communicate back to cluster. Shall be dropped after
    /// close_wait call, when the thread returns, will point to Inner::Init.
    cluster: Box<Cluster>,
    /// Flusher::Tx handle to communicate with flusher.
    flusher: Flusher,
    /// Inner::Handle to corresponding miot-thread. Shall be dropped after close_wait
    /// call, when the thread returns, will point to Inner::Init.
    miot: Miot,

    /// Collection of sessions and corresponding clients managed by this shard. Shall be
    /// dropped after close_wait call, when the thread returns it will be empty.
    sessions: BTreeMap<ClientID, Session>,
    /// Monotonically increasing `seqno`, starting from 1, that is bumped up for every
    /// incoming PUBLISH (QoS-1 & 2) packet.
    inp_seqno: InpSeqno,
    /// Back log of messages that needs to be flushed to other local-shards.
    /// Message::Routed and Message::LocalAck shall first land here, the order of the
    /// messages are preserved for each shard.
    shard_back_log: BTreeMap<u32, Vec<Message>>,
    /// Index of all incoming PUBLISH QoS-1 and QoS-2messages. Happens along with
    /// `shard_back_log`. QoS-0 is not indexed here.
    ///
    /// All entries whose InpSeqno is < min(Timestamp::last_acked) in ack_timestamps
    /// shall be deleted from this index and ACK shall be sent to publishing client.
    index: BTreeMap<InpSeqno, Message>,
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

    /// Corresponding MsgTx handle for all other shards, as Shard::MsgTx,
    shard_queues: BTreeMap<u32, Shard>,
    /// MVCC clone of Cluster::topic_filters
    topic_filters: SubscribedTrie,
    /// MVCC clone of Cluster::retained_messages
    retained_messages: RetainedTrie,

    /// statistics
    stats: Stats,
    /// Back channel communicate with application.
    app_tx: AppTx,
    flush_queue: Vec<FailedSession>, // temporary cache of failed sessions.
}

#[allow(dead_code)]
pub struct ReplicaLoop {
    /// Mio poller for asynchronous handling, all events are from consensus port and
    /// thread-waker.
    poll: mio::Poll,
    /// Self waker.
    waker: Arc<mio::Waker>,
    /// Cluster::Tx handle to communicate back to cluster. Shall be dropped after
    /// close_wait call, when the thread returns, will point to Inner::Init.
    cluster: Box<Cluster>,

    /// Collection of sessions and corresponding clients managed by this shard. Shall be
    /// dropped after close_wait call, when the thread returns it will be empty.
    sessions: BTreeMap<ClientID, Session>,

    /// statistics
    stats: Stats,
    /// Back channel communicate with application.
    app_tx: AppTx,
}

pub struct FinState {
    pub miot: Miot,
    pub sessions: BTreeMap<ClientID, session::Stats>,
    pub inp_seqno: InpSeqno,
    pub shard_back_log: BTreeMap<u32, usize>,
    pub ack_timestamps: Vec<Timestamp>,
    pub stats: Stats,
}

#[derive(Clone, Copy, Default)]
pub struct Stats {
    pub n_events: usize,
    pub n_requests: usize,
}

impl FinState {
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

impl Default for Shard {
    fn default() -> Shard {
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

impl Shardable for Shard {
    fn uuid(&self) -> Uuid {
        self.uuid
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => trace!("{} drop ...", self.prefix),
            Inner::Handle(_hndl) => debug!("{} drop ...", self.prefix),
            Inner::Tx(_waker, _tx) => debug!("{} drop ...", self.prefix),
            Inner::MsgTx(_waker, _tx) => debug!("{} drop ...", self.prefix),
            Inner::MainActive(_active_loop) => info!("{} drop ...", self.prefix),
            Inner::MainReplica(_replica_loop) => info!("{} drop ...", self.prefix),
            Inner::Close(_fin_state) => debug!("{} drop ...", self.prefix),
        }
    }
}

impl ToJson for Shard {
    fn to_config_json(&self) -> String {
        format!(
            concat!("{{ {:?}: {}, {:?}: {} }}"),
            "num_shards",
            self.config.num_shards,
            "mqtt_pkt_batch_size",
            self.config.mqtt_pkt_batch_size,
        )
    }

    fn to_stats_json(&self) -> String {
        "{{}}".to_string()
    }
}

pub struct SpawnArgs {
    pub cluster: Cluster,
    pub flusher: Flusher,
    pub topic_filters: SubscribedTrie,
    pub retained_messages: RetainedTrie,
}

impl Shard {
    const WAKE_TOKEN: mio::Token = mio::Token(1);

    pub fn from_config(config: &Config, shard_id: u32) -> Result<Shard> {
        let def = Shard::default();
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

    pub fn spawn_active(self, args: SpawnArgs, app_tx: &AppTx) -> Result<Shard> {
        let num_shards = self.config.num_shards;

        let poll = mio::Poll::new()?;
        let waker = Arc::new(mio::Waker::new(poll.registry(), Self::WAKE_TOKEN)?);

        // This is the local queue that carries [Message] from one local-session
        // to another local-session. Note that the queue is shared by all the sessions
        // in this shard, hence the queue-capacity is correspondingly large.
        let (msg_tx, msg_rx) = {
            let size = self.config.mqtt_pkt_batch_size * num_shards;
            message::msg_channel(self.shard_id, size as usize, Arc::clone(&waker))
        };
        let mut shard = Shard {
            name: self.config.name.clone(),
            shard_id: self.shard_id,
            uuid: self.uuid,
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::MainActive(ActiveLoop {
                poll,
                waker: Arc::clone(&waker),
                cluster: Box::new(args.cluster),
                flusher: args.flusher,
                miot: Miot::default(),

                sessions: BTreeMap::default(),
                inp_seqno: 1,
                shard_back_log: BTreeMap::default(),
                index: BTreeMap::default(),
                ack_timestamps: Vec::default(),

                shard_queues: BTreeMap::default(),
                topic_filters: args.topic_filters,
                retained_messages: args.retained_messages,

                stats: Stats::default(),
                app_tx: app_tx.clone(),
                flush_queue: Vec::default(),
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

    pub fn spawn_replica(self, args: SpawnArgs, app_tx: &AppTx) -> Result<Shard> {
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

pub enum Request {
    SetMiot(Miot, MsgRx),
    SetShardQueues(BTreeMap<u32, Shard>),
    AddSession(AddSessionArgs),
    FlushSession { socket: Socket, err: Option<Error> },
    Close,
}

pub enum Response {
    Ok,
}

pub struct AddSessionArgs {
    pub sock: mio::net::TcpStream,
    pub connect: v5::Connect,
}

#[derive(Clone)]
pub struct ReplicaSessionArgs {
    pub raddr: net::SocketAddr,
    pub client_id: ClientID,
}

// calls to interface with shard-thread.
impl Shard {
    pub fn wake(&self) -> Result<()> {
        match &self.inner {
            Inner::Handle(Handle { waker, .. }) => Ok(waker.wake()?),
            Inner::Tx(waker, _) => Ok(waker.wake()?),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn set_shard_queues(&self, shards: BTreeMap<u32, Shard>) {
        match &self.inner {
            Inner::Handle(Handle { thrd, .. }) => {
                let req = Request::SetShardQueues(shards);
                app_fatal!(self, thrd.request(req).flatten());
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn add_session(&self, args: AddSessionArgs) -> Result<()> {
        match &self.inner {
            Inner::Handle(Handle { thrd, .. }) => {
                let req = Request::AddSession(args);
                match thrd.request(req)?? {
                    Response::Ok => Ok(()),
                }
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn flush_session(&self, socket: Socket, err: Option<Error>) -> Result<()> {
        match &self.inner {
            Inner::Tx(_waker, tx) => {
                let req = Request::FlushSession { socket, err };
                tx.post(req)?;
                Ok(())
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

    pub fn close_wait(mut self) -> Shard {
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(Handle { thrd, .. }) => {
                let req = Request::Close;
                match thrd.request(req).ok().map(|x| x.ok()).flatten() {
                    Some(Response::Ok) => thrd.close_wait(),
                    _ => unreachable!("{} unxpected response", self.prefix),
                }
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

impl Threadable for Shard {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(self, rx: ThreadRx) -> Self {
        match &self.inner {
            Inner::MainActive(_) => self.active_loop(rx),
            Inner::MainReplica(_) => self.replica_loop(rx),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

impl Shard {
    fn active_loop(mut self, rx: ThreadRx) -> Self {
        use crate::broker::POLL_EVENTS_SIZE;
        use std::time;

        info!("{} spawn config:{}", self.prefix, self.to_config_json());

        // this a work around to wire up all the threads without using unsafe.
        let msg_rx = match rx.recv().unwrap() {
            (Request::SetMiot(miot, msg_rx), Some(tx)) => {
                let active_loop = match &mut self.inner {
                    Inner::MainActive(active_loop) => active_loop,
                    inner => unreachable!("{} {:?}", self.prefix, inner),
                };
                active_loop.miot = miot;
                app_fatal!(&self, tx.send(Ok(Response::Ok)));
                msg_rx
            }
            _ => unreachable!(),
        };

        let mut events = mio::Events::with_capacity(POLL_EVENTS_SIZE);
        loop {
            let timeout: Option<time::Duration> = None;
            if let Err(err) = self.as_mut_poll().poll(&mut events, timeout) {
                self.as_app_tx().send("exit".to_string()).ok();
                error!("{} thread error exit {} ", self.prefix, err);
                break;
            }

            match self.mio_events(&rx, &events) {
                true => break,
                _exit => (),
            };

            // This is where we do routing for all packets received from all session/sock
            // owned by this shard.
            let ack_out_seqnos = self.route_packets();
            self.send_to_shards();

            // Other shards might have routed messages to a session owned by this shard,
            // we will handle it here and push them down to the socket.
            let mut qos_acks = BTreeMap::<u32, InpSeqno>::default();
            let mut status = self.out_messages(&msg_rx, &mut qos_acks);
            let qos_msgs = status.take_values(); // QoS-1 and QoS2 messages.
            if let QueueStatus::Disconnected(_) = status {
                error!("{:?} cascading shutdown via out_messages", self.prefix);
                break;
            }

            // TODO: replicate ack_out_seqnos and qos_msgs in consensus loop
            // TODO: fetch msgs from consensus loop and start commiting.
            let ack_out_seqnos = ack_out_seqnos;
            let qos_msgs = qos_msgs;

            self.commit_acks(ack_out_seqnos);
            self.commit_messages(qos_msgs, &mut qos_acks);
            self.out_acks_publish();
            self.out_acks_flush();

            self.return_local_acks(qos_acks);
            self.send_to_shards();

            // cleanup sessions
            self.clean_failed_sessions();

            // wake up miot every time shard wakes up
            self.as_miot().wake()
        }

        match &self.inner {
            Inner::MainActive(_) => self.handle_close_a(Request::Close),
            Inner::Close(_) => Response::Ok,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        info!("{} thread exit", self.prefix);
        self
    }

    fn replica_loop(mut self, rx: ThreadRx) -> Self {
        use crate::broker::POLL_EVENTS_SIZE;
        use std::time;

        info!("{} spawn config:{}", self.prefix, self.to_config_json());

        let mut events = mio::Events::with_capacity(POLL_EVENTS_SIZE);
        loop {
            let timeout: Option<time::Duration> = None;
            if let Err(err) = self.as_mut_poll().poll(&mut events, timeout) {
                self.as_app_tx().send("exit".to_string()).ok();
                error!("{} thread error exit {} ", self.prefix, err);
                break;
            }

            match self.mio_events(&rx, &events) {
                true => break,
                _exit => (),
            };

            // TODO: fetch ack_out_seqnos and qos_msgs from consensus loop
            // and commit here.
            let ack_out_seqnos = BTreeMap::<ClientID, Vec<OutSeqno>>::default();
            let qos_msgs: Vec<Message> = Vec::default();

            self.commit_acks(ack_out_seqnos);
            self.commit_messages(qos_msgs, &mut BTreeMap::default());
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

impl Shard {
    // (exit,)
    fn mio_events(&mut self, rx: &ThreadRx, events: &mio::Events) -> bool {
        let mut count = 0;
        for _event in events.iter() {
            count += 1;
        }
        self.incr_n_events(count);

        loop {
            // keep repeating until all control requests are drained.
            match self.drain_control_chan(rx) {
                (_, true) => break true,
                (QueueStatus::Ok(_), false) => (),
                (QueueStatus::Block(_), _) => break false,
                (QueueStatus::Disconnected(_), _) => break true,
            }
        }
    }

    // Return (queue-status, disconnected)
    fn drain_control_chan(&mut self, rx: &ThreadRx) -> (QueueReq, bool) {
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
                    let resp = self.handle_add_session_a(req);
                    err!(IPCFail, try: tx.send(Ok(resp))).ok();
                }
                (req @ FlushSession { .. }, None) => {
                    self.handle_flush_session(req);
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
impl Shard {
    // For each session, convert incoming packets to messages and route them to other
    // sessions/bridges.
    fn route_packets(&mut self) -> BTreeMap<ClientID, Vec<OutSeqno>> {
        let mut inner = mem::replace(&mut self.inner, Inner::Init);

        let ActiveLoop { flush_queue, sessions, .. } = match &mut inner {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let mut ack_out_seqnos = BTreeMap::<ClientID, Vec<OutSeqno>>::default();
        let iter = sessions.iter_mut().filter(|(_, s)| s.is_active());
        for (client_id, session) in iter {
            let out_seqnos = match session.route_packets(self) {
                Ok((QueueStatus::Ok(_), out_seqnos)) => out_seqnos,
                Ok((QueueStatus::Block(_), out_seqnos)) => out_seqnos,
                Ok((QueueStatus::Disconnected(_), out_seqnos)) => {
                    info!(
                        "{} raddr:{} disconnected connection",
                        self.prefix, session.raddr
                    );
                    let err: Result<()> = err!(Disconnected, desc: "{}", self.prefix);
                    let val = FailedSession::new(client_id.clone(), err.unwrap_err());
                    flush_queue.push(val);
                    out_seqnos
                }
                Err(err) if err.kind() == ErrorKind::Disconnected => {
                    flush_queue.push(FailedSession::new(client_id.clone(), err));
                    Vec::default()
                }
                Err(err) if err.kind() == ErrorKind::ProtocolError => {
                    flush_queue.push(FailedSession::new(client_id.clone(), err));
                    Vec::default()
                }
                Err(err) => unreachable!("{} unexpected err: {}", self.prefix, err),
            };
            ack_out_seqnos.insert(client_id.clone(), out_seqnos);
        }

        mem::drop(sessions);
        let _init = mem::replace(&mut self.inner, inner);

        ack_out_seqnos
    }

    // Flush outgoing messages, in `shard_back_log` from this shard to other shards.
    fn send_to_shards(&mut self) {
        let ActiveLoop { shard_back_log, shard_queues, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let back_log = mem::replace(shard_back_log, BTreeMap::default());
        for (shard_id, msgs) in back_log.into_iter() {
            let shard = shard_queues.get_mut(&shard_id).unwrap();

            let mut status = shard.send_messages(msgs);
            // re-index the remaining messages, may be the other shard is busy.
            shard_back_log.insert(shard_id, status.take_values());

            match status {
                QueueStatus::Ok(_) | QueueStatus::Block(_) => (),
                QueueStatus::Disconnected(_) => {
                    // TODO: should this be logged at error-level
                    error!(
                        "{} shard_id:{} shard-msg-rx has closed",
                        self.prefix, shard_id
                    );
                }
            }
        }
    }

    fn clean_failed_sessions(&mut self) {
        let mut flush_queue = {
            let ActiveLoop { flush_queue, .. } = match &mut self.inner {
                Inner::MainActive(active_loop) => active_loop,
                inner => unreachable!("{} {:?}", self.prefix, inner),
            };
            mem::replace(flush_queue, Vec::default())
        };

        flush_queue.dedup_by_key(|a| a.client_id.clone());

        for FailedSession { client_id, err } in flush_queue.into_iter() {
            let miot = self.as_mut_miot();

            match miot.remove_connection(&client_id) {
                Ok(Some(socket)) => {
                    let req = Request::FlushSession { socket, err };
                    self.handle_flush_session(req);
                }
                Ok(None) => {
                    debug!(
                        "{} client_id:{} connection already removed",
                        self.prefix, *client_id
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
impl Shard {
    // Receive messages for this shard, flush qos0 messages downstream and return
    // qos12_msgs
    fn out_messages(&mut self, msg_rx: &MsgRx, acks: &mut Acks) -> QueueStatus<Message> {
        // receive messages targeting all the sessions.
        let mut status = msg_rx.try_recvs();

        let mut qos0_msgs = BTreeMap::<ClientID, Vec<Message>>::default();
        let mut qos12_msgs: Vec<Message> = Vec::default();
        for mut msg in status.take_values().into_iter() {
            let ActiveLoop { sessions, .. } = match &mut self.inner {
                Inner::MainActive(active_loop) => active_loop,
                inner => unreachable!("{} {:?}", self.prefix, inner),
            };
            let cid = msg.as_client_id();
            match sessions.get_mut(cid) {
                Some(session) if session.is_active() => session.incr_out_seqno(&mut msg),
                Some(session) => error!(
                    "{} client_id:{} msg-rx session is {:?}",
                    self.prefix, **cid, session.state,
                ),
                None => {
                    error!("{} client_id:{} msg-rx session is gone", self.prefix, **cid,)
                }
            }

            match &msg {
                Message::LocalAck { shard_id, last_acked } => {
                    self.book_acked_timestamps(*shard_id, *last_acked);
                    continue;
                }
                Message::Routed {
                    src_shard_id,
                    client_id,
                    inp_seqno,
                    publish,
                    ack_needed,
                    ..
                } => {
                    if publish.qos == v5::QoS::AtMostOnce {
                        if *ack_needed {
                            acks.insert(*src_shard_id, *inp_seqno);
                        }
                        append_index!(qos0_msgs, client_id.clone(), msg);
                    } else {
                        qos12_msgs.push(msg)
                    }
                }
                _ => unreachable!(),
            };
        }

        self.out_qos0(qos0_msgs);

        status.set_values(qos12_msgs);
        status
    }

    fn out_qos0(&mut self, qos0_msgs: BTreeMap<ClientID, Vec<Message>>) {
        let ActiveLoop { sessions, flush_queue, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        for (client_id, msgs) in qos0_msgs.into_iter() {
            match sessions.get_mut(&client_id) {
                Some(session) if session.is_active() => {
                    if let QueueStatus::Disconnected(_) = session.out_qos0(msgs) {
                        let err = {
                            let err: Result<()> = err!(
                                SlowClient,
                                code: UnspecifiedError,
                                "{} client_id:{}",
                                self.prefix,
                                *client_id
                            );
                            err.unwrap_err()
                        };
                        flush_queue.push(FailedSession::new(client_id, err));
                    }
                }
                Some(session) => error!(
                    "{} client_id:{} msg-rx session is {:?}",
                    self.prefix, *client_id, session.state,
                ),
                None => error!(
                    "{} client_id:{} msg-rx session is gone",
                    self.prefix, *client_id,
                ),
            }
        }
    }

    // replicated messages comming from consensus loop, commit them.
    // acknowldegments for outgoing PUBLISH QoS-1 & QoS-2
    fn commit_acks(&mut self, ack_out_seqnos: BTreeMap<ClientID, Vec<OutSeqno>>) {
        let ActiveLoop { sessions, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        for (client_id, out_seqnos) in ack_out_seqnos.into_iter() {
            if let Some(session) = sessions.get_mut(&client_id) {
                session.commit_acks(out_seqnos);
            }
        }
    }

    fn commit_messages(&mut self, qos_msgs: Vec<Message>, acks: &mut Acks) {
        match &self.inner {
            Inner::MainActive { .. } => self.commit_messages_active(qos_msgs, acks),
            Inner::MainReplica { .. } => self.commit_messages_replica(qos_msgs, acks),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    // replicated messages comming from consensus loop, commit them.
    // Message::Routed, QoS-1 & QoS-2
    fn commit_messages_active(&mut self, msgs: Vec<Message>, acks: &mut Acks) {
        let mut qos_msgs = BTreeMap::<ClientID, Vec<Message>>::default();
        for msg in msgs.into_iter() {
            match &msg {
                Message::Routed { src_shard_id, client_id, inp_seqno, .. } => {
                    acks.insert(*src_shard_id, *inp_seqno);
                    append_index!(qos_msgs, client_id.clone(), msg);
                }
                _ => unreachable!(),
            }
        }

        let ActiveLoop { sessions, flush_queue, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        for (client_id, msgs) in qos_msgs.into_iter() {
            if let Some(session) = sessions.get_mut(&client_id) {
                if let QueueStatus::Disconnected(_) = session.out_qos(msgs) {
                    let err = {
                        let err: Result<()> = err!(
                            SlowClient,
                            code: UnspecifiedError,
                            "{} client_id:{}",
                            self.prefix,
                            *client_id
                        );
                        err.unwrap_err()
                    };
                    flush_queue.push(FailedSession::new(client_id.clone(), err))
                }
            }
        }
    }

    fn commit_messages_replica(&mut self, msgs: Vec<Message>, _acks: &mut Acks) {
        let mut qos_msgs = BTreeMap::<ClientID, Vec<Message>>::default();
        for msg in msgs.into_iter() {
            match &msg {
                Message::Routed { client_id, .. } => {
                    append_index!(qos_msgs, client_id.clone(), msg);
                }
                _ => unreachable!(),
            }
        }

        let ActiveLoop { sessions, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        for (client_id, msgs) in qos_msgs.into_iter() {
            if let Some(session) = sessions.get_mut(&client_id) {
                session.out_qos(msgs);
            }
        }
    }

    fn return_local_acks(&mut self, qos_acks: BTreeMap<u32, InpSeqno>) {
        let ActiveLoop { shard_back_log, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let shard_id = self.shard_id;
        for (target_shard_id, inp_seqno) in qos_acks.into_iter() {
            let msg = Message::LocalAck { shard_id, last_acked: inp_seqno };
            append_index!(shard_back_log, target_shard_id, msg);
        }
    }

    fn out_acks_publish(&mut self) {
        let ActiveLoop { sessions, index, ack_timestamps, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let min: Option<InpSeqno> = ack_timestamps.iter().map(|t| t.last_acked).min();
        if let Some(min) = min {
            let seqnos = index
                .iter()
                .take_while(|(s, _)| **s <= min)
                .map(|(s, _)| *s)
                .collect::<Vec<InpSeqno>>();

            for seqno in seqnos.into_iter() {
                match index.remove(&seqno).unwrap() {
                    Message::Index { src_client_id, packet_id } => {
                        let session = sessions.get_mut(&src_client_id).unwrap();
                        session.out_acks_publish(packet_id);
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    fn out_acks_flush(&mut self) {
        let ActiveLoop { sessions, flush_queue, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let iter = sessions.iter_mut().filter(|(_, s)| s.is_active());
        for (client_id, session) in iter {
            if let QueueStatus::Disconnected(_) = session.out_acks_flush() {
                let err = {
                    let err: Result<()> = err!(
                        SlowClient,
                        code: UnspecifiedError,
                        "{} client_id:{}",
                        self.prefix,
                        **client_id
                    );
                    err.unwrap_err()
                };
                flush_queue.push(FailedSession::new(client_id.clone(), err));
            }
        }
    }
}

// sub-functions that work for handling incoming publish
impl Shard {
    // a. Only one message is sent to a client, even with multiple matches.
    // b. subscr_qos is maximum of all matching-subscribption.
    // c. final qos is min(server_qos, publish_qos, subscr_qos)
    // d. retain if any of the matching-subscription is calling for retain_as_published.
    // e. no_local is all of the matching-subscription is calling for no_local.
    pub fn match_subscribers(
        &self,
        topic_name: &TopicName,
    ) -> BTreeMap<ClientID, (v5::Subscription, Vec<u32>)> {
        // group subscriptions based on client-id.
        let mut subscrs: BTreeMap<ClientID, (v5::Subscription, Vec<u32>)> =
            BTreeMap::default();

        for subscr in self.as_topic_filters().match_topic_name(topic_name).into_iter() {
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

    pub fn incr_inp_seqno(&mut self) -> u64 {
        match &mut self.inner {
            Inner::MainActive(ActiveLoop { inp_seqno, .. }) => {
                let seqno = *inp_seqno;
                *inp_seqno = inp_seqno.saturating_add(1);
                seqno
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn book_index(&mut self, qos: v5::QoS, msg: Message) {
        let ActiveLoop { index, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        match qos {
            v5::QoS::AtMostOnce => (),
            v5::QoS::AtLeastOnce => {
                let inp_seqno = match &msg {
                    Message::Routed { inp_seqno, .. } => *inp_seqno,
                    _ => unreachable!(),
                };
                index.insert(inp_seqno, msg);
            }
            v5::QoS::ExactlyOnce => todo!(),
        }
    }

    pub fn route_to_client(&mut self, target_shard_id: u32, msg: Message) {
        let ActiveLoop { shard_back_log, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let inp_seqno = match &msg {
            Message::Routed { inp_seqno, .. } => *inp_seqno,
            _ => unreachable!(),
        };

        append_index!(shard_back_log, target_shard_id, msg);
        self.book_routed_timestamps(target_shard_id, inp_seqno);
    }

    fn book_routed_timestamps(&mut self, shard_id: u32, inp_seqno: InpSeqno) {
        let ActiveLoop { ack_timestamps, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        match ack_timestamps.binary_search_by_key(&shard_id, |t| t.shard_id) {
            Ok(off) => ack_timestamps[off].last_routed = inp_seqno,
            Err(off) => {
                let t = Timestamp { shard_id, last_routed: inp_seqno, last_acked: 0 };
                ack_timestamps[off] = t;
            }
        }
    }

    fn book_acked_timestamps(&mut self, shard_id: u32, last_acked: InpSeqno) {
        let ActiveLoop { ack_timestamps, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        match ack_timestamps.binary_search_by_key(&shard_id, |t| t.shard_id) {
            Ok(off) => ack_timestamps[off].last_acked = last_acked,
            Err(_) => unreachable!(),
        }
    }
}

impl Shard {
    fn handle_set_shard_queues(&mut self, req: Request) -> Response {
        let shard_queues = match req {
            Request::SetShardQueues(shard_queues) => shard_queues,
            _ => unreachable!(),
        };

        match &mut self.inner {
            Inner::MainActive(active_loop) => {
                active_loop.shard_queues = shard_queues;
                Response::Ok
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn handle_flush_session(&mut self, req: Request) -> Response {
        use crate::broker::flush::FlushConnectionArgs;

        let (socket, err) = match req {
            Request::FlushSession { socket, err } => (socket, err),
            _ => unreachable!(),
        };

        match &mut self.inner {
            Inner::MainActive(ActiveLoop { sessions, topic_filters, .. }) => {
                let cid = socket.client_id.clone();
                if let Some(mut session) = sessions.remove(&cid) {
                    session.remove_topic_filters(topic_filters);
                    sessions.insert(cid, session.into_reconnect());
                } else {
                    warn!("{} client_id:{} session not found", self.prefix, *cid);
                }
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let ActiveLoop { flusher, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };
        let args = FlushConnectionArgs { socket, err: err };
        app_fatal!(&self, flusher.flush_connection(args));

        // TODO: consensus loop: replicate flush_session so that replica shall remove
        // the session from its book-keeping.

        Response::Ok
    }

    fn handle_add_session_a(&mut self, req: Request) -> Response {
        use crate::broker::miot::AddConnectionArgs;

        // create the session
        let (mut session, upstream, downstream) = {
            let (args, upstream, downstream) = self.to_args_active(&req);
            let session = self.new_session_a(args);
            (session, upstream, downstream)
        };

        let AddSessionArgs { sock, connect } = match req {
            Request::AddSession(args) => args,
            _ => unreachable!(),
        };
        let raddr = sock.peer_addr().unwrap();
        let client_id = ClientID::from_connect(&connect.payload.client_id);

        // TODO: handle connect.flags.clean_start here.

        // send back the connection acknowledgment CONNACK here.
        {
            let packet = session.success_ack(&connect);
            let msgs = vec![Message::new_conn_ack(packet)];
            session.as_mut_out_acks().extend(msgs.into_iter());

            match session.out_acks_flush() {
                QueueStatus::Ok(_) => {
                    info!("{} raddr:{} send CONNACK", self.prefix, raddr);
                }
                QueueStatus::Disconnected(_) | QueueStatus::Block(_) => {
                    error!("{} raddr:{} fail to send CONNACK", self.prefix, raddr);
                    return Response::Ok;
                }
            }
        }

        // add_connection further down shall wake miot-thread.
        {
            let client_id = client_id.clone();
            let def = Config::DEF_MQTT_MAX_PACKET_SIZE;
            let args = AddConnectionArgs {
                client_id,
                conn: sock,
                upstream,
                downstream,
                max_packet_size: session.as_connect().max_packet_size(def),
            };
            app_fatal!(&self, self.as_miot().add_connection(args));
        }

        let ActiveLoop { sessions, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };
        sessions.insert(client_id.clone(), session);
        info!("{} raddr:{} adding new session to shard", self.prefix, raddr);

        // TODO: consensus loop: replicate add_session so that replica can be created
        // in remote nodes. We did not wait for add_session to be commited.

        Response::Ok
    }

    #[allow(dead_code)] // TODO: wire this up with consensus loop
    fn handle_add_session_r(&mut self, args: ReplicaSessionArgs) -> Response {
        // create the session
        let session = {
            let args = SessionArgsReplica {
                raddr: args.raddr.clone(),
                config: self.config.clone(),
                client_id: args.client_id.clone(),
                shard_id: self.shard_id,
            };
            self.new_session_r(args)
        };

        let ReplicaLoop { sessions, .. } = match &mut self.inner {
            Inner::MainReplica(replica_loop) => replica_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };
        sessions.insert(args.client_id.clone(), session);
        info!("{} raddr:{} adding new replica session to shard", self.prefix, args.raddr);

        Response::Ok
    }

    fn handle_close_a(&mut self, _req: Request) -> Response {
        let mut active_loop = match mem::replace(&mut self.inner, Inner::Init) {
            Inner::MainActive(active_loop) => active_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        info!("{} closing shard", self.prefix);

        let miot = mem::replace(&mut active_loop.miot, Miot::default()).close_wait();

        mem::drop(active_loop.poll);
        mem::drop(active_loop.waker);
        mem::drop(active_loop.cluster);
        mem::drop(active_loop.flusher);

        mem::drop(active_loop.shard_queues);
        mem::drop(active_loop.topic_filters);
        mem::drop(active_loop.retained_messages);

        let mut new_sessions = BTreeMap::default();
        for (client_id, sess) in active_loop.sessions.into_iter() {
            new_sessions.insert(client_id, sess.close());
        }

        let fin_state = FinState {
            miot,
            sessions: new_sessions,
            inp_seqno: active_loop.inp_seqno,
            shard_back_log: BTreeMap::default(), // TODO
            ack_timestamps: active_loop.ack_timestamps,
            stats: active_loop.stats,
        };
        info!("{} stats:{}", self.prefix, fin_state.to_json());

        let _init = mem::replace(&mut self.inner, Inner::Close(fin_state));
        self.prefix = self.prefix();

        Response::Ok
    }

    #[allow(dead_code)] // TODO: wire this up with consensus loop
    fn handle_close_r(&mut self, _req: Request) -> Response {
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
            new_sessions.insert(client_id, sess.close());
        }

        let fin_state = FinState {
            miot: Miot::default(),
            sessions: new_sessions,
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

impl Shard {
    // Return (new_session, session_tx (upstream), miot_rx (downstream))
    fn to_args_active(&mut self, req: &Request) -> (SessionArgsActive, PktTx, PktRx) {
        // This queue is wired up with miot-thread. This queue carries v5::Packet,
        // and there is a separate queue for every session.
        let (upstream, session_rx) = {
            let size = self.config.mqtt_pkt_batch_size as usize;
            let waker = self.to_waker();
            socket::pkt_channel(self.shard_id, size, waker)
        };
        // This queue is wired up with miot-thread. This queue carries v5::Packet,
        // and there is a separate queue for every session.
        let (miot_tx, downstream) = {
            let size = self.config.mqtt_pkt_batch_size as usize;
            let waker = self.as_miot().to_waker();
            socket::pkt_channel(self.shard_id, size, waker)
        };

        let args = match req {
            Request::AddSession(AddSessionArgs { sock, connect }) => SessionArgsActive {
                raddr: sock.peer_addr().unwrap(),
                config: self.config.clone(),
                client_id: ClientID::from_connect(&connect.payload.client_id),
                shard_id: self.shard_id,
                miot_tx,
                session_rx,
                connect: connect.clone(),
            },
            _ => unreachable!(),
        };

        (args, upstream, downstream)
    }

    fn new_session_a(&mut self, args: SessionArgsActive) -> Session {
        // add_connection further down shall wake miot-thread.
        let ActiveLoop { sessions, miot, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
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

                match miot.remove_connection(&args.client_id) {
                    Ok(Some(socket)) => {
                        let prefix = &self.prefix;
                        let err: Result<()> = err_session_takeover(prefix, args.raddr);
                        let arg = Request::FlushSession { socket, err: err.err() };
                        self.handle_flush_session(arg);
                    }
                    Ok(None) if session.is_replica() => (),
                    Ok(None) => {
                        error!(
                            "{} client_id:{} session missing",
                            self.prefix, *args.client_id
                        );
                    }
                    Err(err) => {
                        self.as_app_tx().send("fatal".to_string()).ok();
                        error!("{} fatal error {} ", self.prefix, err);
                    }
                }

                self.cleanup_index(&args.client_id);
                session
            }
            None => Session::default(),
        };

        session.into_active(args)
    }

    fn new_session_r(&mut self, args: session::SessionArgsReplica) -> Session {
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

    fn cleanup_index(&mut self, client_id: &ClientID) {
        let ActiveLoop { index, .. } = match &mut self.inner {
            Inner::MainActive(active_loop) => active_loop,
            _ => unreachable!(),
        };

        let mut inp_seqnos = Vec::default();
        for (inp_seqo, msg) in index.iter() {
            match msg {
                Message::Index { src_client_id, .. } => {
                    if src_client_id == client_id {
                        inp_seqnos.push(*inp_seqo);
                    }
                }
                _ => unreachable!(),
            }
        }

        for inp_seqno in inp_seqnos.into_iter() {
            index.remove(&inp_seqno);
        }
    }
}

impl Shard {
    pub fn as_cluster(&self) -> &Cluster {
        match &self.inner {
            Inner::MainActive(ActiveLoop { cluster, .. }) => cluster,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub fn as_topic_filters(&self) -> &SubscribedTrie {
        match &self.inner {
            Inner::MainActive(ActiveLoop { topic_filters, .. }) => topic_filters,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

impl Shard {
    fn prefix(&self) -> String {
        let state = match &self.inner {
            Inner::Init => "init",
            Inner::Handle(_) => "hndl",
            Inner::Tx(_, _) => "tx",
            Inner::MsgTx(_, _) => "msgtx",
            Inner::MainActive(_) => "active",
            Inner::MainReplica(_) => "replica",
            Inner::Close(_) => "close",
        };
        format!("<s:{}:{}:{}>", self.name, self.shard_id, state)
    }

    fn incr_n_events(&mut self, count: usize) {
        match &mut self.inner {
            Inner::MainActive(ActiveLoop { stats, .. }) => stats.n_events += count,
            Inner::MainReplica(ReplicaLoop { stats, .. }) => stats.n_events += count,
            Inner::Close(finstate) => finstate.stats.n_events += count,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn incr_n_requests(&mut self, count: usize) {
        match &mut self.inner {
            Inner::MainActive(ActiveLoop { stats, .. }) => stats.n_requests += count,
            Inner::MainReplica(ReplicaLoop { stats, .. }) => stats.n_requests += count,
            Inner::Close(finstate) => finstate.stats.n_requests += count,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn as_mut_poll(&mut self) -> &mut mio::Poll {
        match &mut self.inner {
            Inner::MainActive(ActiveLoop { poll, .. }) => poll,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn as_app_tx(&self) -> &AppTx {
        match &self.inner {
            Inner::MainActive(ActiveLoop { app_tx, .. }) => app_tx,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn as_miot(&self) -> &Miot {
        match &self.inner {
            Inner::MainActive(ActiveLoop { miot, .. }) => miot,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn as_mut_miot(&mut self) -> &Miot {
        match &mut self.inner {
            Inner::MainActive(ActiveLoop { miot, .. }) => miot,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn to_waker(&self) -> Arc<mio::Waker> {
        match &self.inner {
            Inner::MainActive(ActiveLoop { waker, .. }) => Arc::clone(waker),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

struct FailedSession {
    client_id: ClientID,
    err: Option<Error>,
}

impl FailedSession {
    fn new(client_id: ClientID, err: Error) -> FailedSession {
        FailedSession { client_id, err: Some(err) }
    }
}

fn err_session_takeover(prefix: &str, raddr: net::SocketAddr) -> Result<()> {
    err!(SessionTakenOver, code: SessionTakenOver, "{} client {}", prefix, raddr)
}
