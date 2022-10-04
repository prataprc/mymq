use log::{debug, error, info, trace};
use mio::event::Events;
use uuid::Uuid;

use std::sync::{mpsc, Arc};
use std::{collections::BTreeMap, fmt, mem, net, path, result, time};

use crate::broker::thread::{Rx, Thread, Threadable, Tx};
use crate::broker::{rebalance, ticker};
use crate::broker::{util, Timer, ToJson, TopicName};
use crate::broker::{AppTx, Config, ConfigNode, Hostable, RetainedTrie, SubscribedTrie};
use crate::broker::{Error, ErrorKind, Result};
use crate::broker::{Flusher, Listener, QueueStatus, Shard, Socket, Ticker};

use crate::v5;

type ThreadRx = Rx<Request, Result<Response>>;
type QueueReq = crate::broker::thread::QueueReq<Request, Result<Response>>;

/// Type is the entry point to start/restart an MQTT instance.
pub struct Cluster {
    /// Refer [Config::name]
    pub name: String,
    prefix: String,
    config: Config,
    inner: Inner,
}

enum Inner {
    Init,
    // Held by application.
    Handle(Arc<mio::Waker>, Thread<Cluster, Request, Result<Response>>),
    // Held by Listener, Handshake, Ticker and Shard.
    Tx(Arc<mio::Waker>, Tx<Request, Result<Response>>),
    // Thread
    Main(RunLoop),
    // Held by Application, replacing both Handle and Main.
    Close(FinState),
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Inner::Init => write!(f, "Cluster::Inner::Init"),
            Inner::Handle(_, _) => write!(f, "Cluster::Inner::Handle"),
            Inner::Tx(_, _) => write!(f, "Cluster::Inner::Handle"),
            Inner::Main(_) => write!(f, "Cluster::Inner::Main"),
            Inner::Close(_) => write!(f, "Cluster::Inner::Close"),
        }
    }
}

struct RunLoop {
    /// Mio pooler for asynchronous handling, aggregate events from consensus port and
    /// waker.
    poll: mio::Poll,
    /// Listener thread for MQTT connections from remote/local clients.
    listener: Listener,
    /// Flusher thread for MQTT connections from remote/local clients.
    flusher: Flusher,
    /// Ticker thread to periodically wake up other threads, defaul is 10ms.
    ticker: Ticker,
    /// Total number of shards within this node.
    active_shards: BTreeMap<u32, Shard>,

    /// Rebalancing algorithm.
    rebalancer: rebalance::Rebalancer,
    /// Index of subscribed topicfilters across all the sessions, local to this node.
    cc_topic_filters: SubscribedTrie, // key=TopicFilter, val=(client_id, shard_id)
    /// Index of retained messages for each topic-name, across all the sessions, local
    /// to this node.
    cc_retained_topics: RetainedTrie, // indexed by TopicName.
    /// Timer that managers expiry of retained messages.
    retain_timer: Timer<TopicName, v5::Publish>,

    /// Consensus state.
    state: ClusterState,

    /// Statistics
    stats: Stats,

    /// Back channel communicate with application.
    app_tx: AppTx,
}

pub enum ClusterState {
    /// Cluster is single-node.
    SingleNode { state: SingleNode },
}

pub struct FinState {
    pub state: ClusterState,
    pub listener: Listener,
    pub ticker: Ticker,
    pub flusher: Flusher,
    pub active_shards: Vec<Shard>,
    pub topic_filters: SubscribedTrie,
    pub retained_messages: RetainedTrie,
    pub stats: Stats,
}

#[derive(Clone, Copy, Default)]
pub struct Stats {
    n_events: usize,
    n_requests: usize,
}

impl FinState {
    fn to_json(&self) -> String {
        format!(
            concat!("{{ {:?}: {}, {:?}: {} }}"),
            "n_events", self.stats.n_events, "n_requests", self.stats.n_requests,
        )
    }
}

impl Default for Cluster {
    fn default() -> Cluster {
        let config = Config::default();
        let mut def = Cluster {
            name: config.name.to_string(),
            prefix: String::default(),
            config,
            inner: Inner::Init,
        };
        def.prefix = def.prefix();
        def
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => trace!("{} drop ...", self.prefix),
            Inner::Handle(_waker, _thrd) => debug!("{} drop ...", self.prefix),
            Inner::Tx(_waker, _tx) => debug!("{} drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
            Inner::Close(_fin_state) => debug!("{} drop ...", self.prefix),
        }
    }
}

impl ToJson for Cluster {
    fn to_config_json(&self) -> String {
        format!(
            concat!("{{ {:?}: {}, {:?}: {} }}"),
            "max_nodes", self.config.max_nodes, "num_shards", self.config.num_shards
        )
    }

    fn to_stats_json(&self) -> String {
        match &self.inner {
            Inner::Main(RunLoop { active_shards, .. }) => {
                format!(concat!("{{ {:?}: {} }}"), "active_shards", active_shards.len())
            }
            _ => "{{}}".to_string(),
        }
    }
}

struct SpawnListener<'a> {
    config: &'a Config,
    cluster: &'a Cluster,
    app_tx: &'a AppTx,
}
struct SpawnShards<'a> {
    config: &'a Config,
    cluster: &'a Cluster,
    flusher_tx: Flusher,
    cc_topic_filters: &'a SubscribedTrie,
    cc_retained_topics: &'a RetainedTrie,
    app_tx: &'a AppTx,
}
struct SpawnTicker<'a> {
    config: &'a Config,
    cluster: &'a Cluster,
    shards: Vec<Shard>,
    app_tx: &'a AppTx,
}

// Handle cluster
impl Cluster {
    /// Poll register token for waker event.
    pub const TOKEN_WAKE: mio::Token = mio::Token(1);
    /// Poll register for consensus TcpStream.
    pub const TOKEN_CONSENSUS: mio::Token = mio::Token(2);

    /// Create a cluster from configuration. Returned Cluster shall be in `Init` state.
    /// To start the cluster call [Cluster::spawn].
    pub fn from_config(config: &Config) -> Result<Cluster> {
        // validate
        if config.num_shards == 0 {
            err!(InvalidInput, desc: "num_shards can't be ZERO")?;
        } else if !util::is_power_of_2(config.num_shards) {
            err!(
                InvalidInput,
                desc: "num. of shards must be power of 2 {}",
                config.num_shards
            )?;
        }

        let mut val = Cluster {
            name: config.name.clone(),
            prefix: String::default(),
            config: config.clone(),
            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    /// Start this cluster instance
    pub fn spawn(self, app_tx: AppTx) -> Result<Cluster> {
        use mio::Waker;

        let poll = err!(IOError, try: mio::Poll::new(), "fail creating mio::Poll")?;
        let waker = Arc::new(Waker::new(poll.registry(), Self::TOKEN_WAKE)?);

        let rebalancer = rebalance::Rebalancer {
            config: self.config.clone(),
            algo: rebalance::Algorithm::SingleNode,
        };

        let state = match self.config.nodes.len() {
            1 => {
                let node = Node::try_from(self.config.nodes[0].clone())?;
                let topology = rebalancer.rebalance(&vec![node.clone()], Vec::new());
                ClusterState::SingleNode {
                    state: SingleNode { config: self.config.clone(), node, topology },
                }
            }
            _ => todo!(),
        };

        let flusher = Flusher::from_config(&self.config)?.spawn(app_tx.clone())?;
        let flusher_tx = flusher.to_tx("cluster-spawn");

        let cc_topic_filters = SubscribedTrie::default();
        let cc_retained_topics = RetainedTrie::default();

        let mut cluster = Cluster {
            name: self.config.name.clone(),
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                state,

                poll,
                listener: Listener::default(),
                flusher,
                ticker: Ticker::default(),
                active_shards: BTreeMap::default(),

                rebalancer,
                cc_topic_filters: cc_topic_filters.clone(),
                cc_retained_topics: cc_retained_topics.clone(),
                retain_timer: Timer::default(),

                stats: Stats::default(),

                app_tx: app_tx.clone(),
            }),
        };
        cluster.prefix = cluster.prefix();
        let mut thrd = Thread::spawn(&self.prefix, cluster);
        thrd.set_waker(Arc::clone(&waker));

        let mut cluster = Cluster {
            name: self.config.name.clone(),
            prefix: String::default(),
            config: self.config.clone(),
            inner: Inner::Handle(waker, thrd),
        };
        cluster.prefix = cluster.prefix();

        {
            let args = SpawnListener {
                config: &self.config,
                cluster: &cluster,
                app_tx: &app_tx,
            };
            let listener = Self::spawn_listener(args)?;

            let args = SpawnShards {
                config: &self.config,
                cluster: &cluster,
                flusher_tx,
                cc_topic_filters: &cc_topic_filters,
                cc_retained_topics: &cc_retained_topics,
                app_tx: &app_tx,
            };
            let active_shards = Self::spawn_active_shards(args)?;

            Self::set_shard_queues(&active_shards);

            let args = SpawnTicker {
                config: &self.config,
                cluster: &cluster,
                // TODO: include replica-shards in ticker_shards
                shards: active_shards
                    .iter()
                    .map(|(_, shard)| shard.to_tx("ticker"))
                    .collect(),
                app_tx: &app_tx,
            };
            let ticker = Self::spawn_ticker(args)?;

            match &cluster.inner {
                Inner::Handle(_waker, thrd) => {
                    thrd.request(Request::Set { listener, ticker, active_shards })??;
                }
                inner => unreachable!("{} {:?}", self.prefix, inner),
            }
        }

        // TODO uncomment this.
        // info!("{} port:{} listening ... ", self.prefix, self.config.port);

        Ok(cluster)
    }

    fn spawn_listener(args: SpawnListener) -> Result<Listener> {
        let listener = Listener::from_config(args.config)?;
        listener.spawn(args.cluster.to_tx("listener"), args.app_tx.clone())
    }

    fn spawn_active_shards(args: SpawnShards) -> Result<BTreeMap<u32, Shard>> {
        let mut active_shards = BTreeMap::default();
        for shard_id in 0..args.config.num_shards {
            let shard = {
                let spawn_args = crate::broker::shard::SpawnArgs {
                    cluster: args.cluster.to_tx("shard"),
                    flusher: args.flusher_tx.to_tx("shard"),
                    cc_topic_filters: args.cc_topic_filters.clone(),
                    cc_retained_topics: args.cc_retained_topics.clone(),
                };
                let shard = Shard::from_config(args.config, shard_id)?;
                shard.spawn_active(spawn_args, args.app_tx)?
            };

            active_shards.insert(shard_id, shard);
        }

        Ok(active_shards)
    }

    fn spawn_ticker(args: SpawnTicker) -> Result<Ticker> {
        let ticker_args = ticker::SpawnArgs {
            cluster: Box::new(args.cluster.to_tx("ticker")),
            shards: args.shards,
            app_tx: args.app_tx.clone(),
        };
        Ticker::from_config(args.config.clone())?.spawn(ticker_args)
    }

    fn set_shard_queues(active_shards: &BTreeMap<u32, Shard>) {
        for (_shard_id, shard) in active_shards.iter() {
            let iter = active_shards.iter().map(|(id, s)| (*id, s.to_msg_tx()));
            shard.set_shard_queues(BTreeMap::from_iter(iter));
        }
    }

    pub(crate) fn to_tx(&self, who: &str) -> Self {
        let inner = match &self.inner {
            Inner::Handle(waker, thrd) => Inner::Tx(Arc::clone(waker), thrd.to_tx()),
            Inner::Tx(waker, tx) => Inner::Tx(Arc::clone(waker), tx.clone()),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };
        let mut val = Cluster {
            name: self.config.name.clone(),
            prefix: String::default(),
            config: self.config.clone(),
            inner,
        };
        val.prefix = val.prefix();

        debug!("{} cloned for {}", val.prefix, who);
        val
    }
}

pub enum Request {
    Set {
        listener: Listener,
        ticker: Ticker,
        active_shards: BTreeMap<u32, Shard>,
    },
    SetRetainTopic {
        publish: v5::Publish,
    },
    ResetRetainTopic {
        topic_name: TopicName,
    },
    AddConnection(AddConnectionArgs),
    Close,
}

pub enum Response {
    Ok,
}

// calls to interface with cluster-thread.
impl Cluster {
    pub(crate) fn wake(&self) -> Result<()> {
        match &self.inner {
            Inner::Tx(waker, _) => Ok(waker.wake()?),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    pub(crate) fn add_connection(&self, sock: Socket) -> Result<()> {
        match &self.inner {
            Inner::Tx(_waker, tx) => {
                let req = Request::AddConnection(sock);
                tx.request(req)??;
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        Ok(())
    }

    pub(crate) fn set_retain_topic(&self, publish: v5::Publish) -> Result<()> {
        match &self.inner {
            Inner::Tx(_waker, tx) => {
                let req = Request::SetRetainTopic { publish };
                tx.post(req)?;
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }

        Ok(())
    }

    pub(crate) fn reset_retain_topic(&self, topic_name: TopicName) -> Result<()> {
        match &self.inner {
            Inner::Tx(_waker, tx) => {
                let req = Request::ResetRetainTopic { topic_name };
                tx.post(req)?;
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }

        Ok(())
    }

    /// Close this cluster and get back the statistics. Call return only after all the
    /// children threads are gracefully shutdown.
    pub fn close_wait(mut self) -> Cluster {
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(_waker, thrd) => {
                thrd.request(Request::Close).ok();
                thrd.close_wait()
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

impl Threadable for Cluster {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: Rx<Self::Req, Self::Resp>) -> Self {
        use crate::broker::POLL_EVENTS_SIZE;

        info!("{} spawn thread config:{}", self.prefix, self.to_config_json());

        let timeout: Option<time::Duration> = None;
        let mut events = Events::with_capacity(POLL_EVENTS_SIZE);

        loop {
            match self.as_mut_poll().poll(&mut events, timeout) {
                err @ Err(_) => {
                    app_fatal!(&self, err);
                    break;
                }
                _ => (),
            }

            match self.mio_events(&rx, &events) {
                true => break,
                _exit => (),
            };

            self.retain_expires();
        }

        match &self.inner {
            Inner::Main(_) => self.handle_close(Request::Close),
            Inner::Close(_) => Response::Ok,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        info!("{} thread exit", self.prefix);
        self
    }
}

impl Cluster {
    // return (exit,)
    fn mio_events(&mut self, rx: &ThreadRx, events: &Events) -> bool {
        let mut count = 0_usize;
        let mut iter = events.iter();
        let exit = 'outer: loop {
            match iter.next() {
                Some(event) => {
                    count += 1;

                    match event.token() {
                        Self::TOKEN_WAKE => loop {
                            // keep repeating until all control requests are drained
                            match self.drain_control_chan(rx) {
                                (_status, true) => break 'outer true,
                                (QueueStatus::Ok(_), _exit) => (),
                                (QueueStatus::Block(_), _) => break,
                                (QueueStatus::Disconnected(_), _) => break 'outer true,
                            }
                        },
                        Self::TOKEN_CONSENSUS => todo!(),
                        _ => unreachable!(),
                    }
                }
                None => break false,
            }
        };

        self.incr_n_events(count);

        exit
    }

    // Return (queue-status, exit)
    // IPCFail,
    fn drain_control_chan(&mut self, rx: &ThreadRx) -> (QueueReq, bool) {
        use crate::broker::{thread::pending_requests, CONTROL_CHAN_SIZE};
        use Request::*;

        let mut status = pending_requests(&self.prefix, &rx, CONTROL_CHAN_SIZE);
        let reqs = status.take_values();

        self.incr_n_requests(reqs.len());

        // TODO: review control-channel handling for all threads. Should we panic or
        // return error.
        let mut closed = false;
        for req in reqs.into_iter() {
            match req {
                (req @ Set { .. }, Some(tx)) => {
                    let resp = self.handle_set(req);
                    err!(IPCFail, try: tx.send(Ok(resp))).ok();
                }
                (req @ SetRetainTopic { .. }, None) => {
                    self.handle_set_retain_topic(req);
                }
                (req @ ResetRetainTopic { .. }, None) => {
                    self.handle_reset_retain_topic(req);
                }
                (req @ AddConnection(_), Some(tx)) => {
                    let resp = self.handle_add_connection(req);
                    err!(IPCFail, try: tx.send(Ok(resp))).ok();
                }
                (req @ Close, Some(tx)) => {
                    let resp = self.handle_close(req);
                    err!(IPCFail, try: tx.send(Ok(resp))).ok();
                    closed = true;
                }

                (_, _) => unreachable!(), // TODO: log meaning message.
            };
        }

        (status, closed)
    }

    fn retain_expires(&mut self) {
        let RunLoop { cc_retained_topics, retain_timer, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let pkts: Vec<v5::Publish> = retain_timer.gc().collect();
        debug!("{} gc:{} pkts in retain_timer", self.prefix, pkts.len());

        // gather all retained packets and cleanup the RetainedTrie.
        for pkt in retain_timer.expired().collect::<Vec<v5::Publish>>() {
            cc_retained_topics.remove(&pkt.topic_name);
        }
    }
}

// Main loop
impl Cluster {
    fn handle_set(&mut self, req: Request) -> Response {
        let run_loop = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        match req {
            Request::Set { listener, ticker, active_shards } => {
                run_loop.ticker = ticker;
                run_loop.listener = listener;
                run_loop.active_shards = active_shards;
            }
            _ => unreachable!(),
        }

        Response::Ok
    }

    fn handle_set_retain_topic(&mut self, req: Request) {
        let publish = match req {
            Request::SetRetainTopic { publish } => publish,
            _ => unreachable!(),
        };
        let key = publish.topic_name.clone();

        let RunLoop { cc_retained_topics, retain_timer, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };
        // set this retain message as the latest one.
        cc_retained_topics.set(&publish.topic_name, publish.clone());
        retain_timer.delete(&key).unwrap();

        // book keeping for message expiry.
        if let Some(secs) = publish.message_expiry_interval() {
            retain_timer.add_timeout(secs as u64, key, publish);
        }
    }

    fn handle_reset_retain_topic(&mut self, req: Request) {
        let topic_name = match req {
            Request::ResetRetainTopic { topic_name } => topic_name,
            _ => unreachable!(),
        };

        let RunLoop { cc_retained_topics, retain_timer, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        cc_retained_topics.remove(&topic_name);
        retain_timer.delete(&topic_name).unwrap();
    }

    // Errors - IPCFail,
    fn handle_add_connection(&mut self, req: Request) -> Response {
        use crate::broker::shard::AddSessionArgs;

        let sock = match req {
            Request::AddConnection(sock) => sock,
            _ => unreachable!(),
        };
        let raddr = sock.peer_addr();

        let RunLoop { active_shards, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let client_id = sock.to_client_id();
        let shard_id =
            rebalance::Rebalancer::session_partition(&*client_id, self.config.num_shards);

        let shard = match active_shards.get_mut(&shard_id) {
            Some(shard) => shard,
            None => {
                // multi-node cluster, look at the topology and redirect client using
                // connack::server_reference, and close the connection.
                todo!()
            }
        };
        info!(
            "{} raddr:{} shard_id:{} new connection mapped",
            self.prefix, raddr, shard_id
        );

        // Add session to the shard.
        if let Err(err) = shard.add_session(sock) {
            error!("{} error adding session err:{}", self.prefix, err);
        }

        Response::Ok
    }

    fn handle_close(&mut self, _: Request) -> Response {
        let mut run_loop = match mem::replace(&mut self.inner, Inner::Init) {
            Inner::Main(run_loop) => run_loop,
            Inner::Close(_) => return Response::Ok,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        info!("{} closing cluster", self.prefix);

        mem::drop(run_loop.poll);
        mem::drop(run_loop.rebalancer);

        let listener = {
            let val = mem::replace(&mut run_loop.listener, Listener::default());
            val.close_wait()
        };
        let ticker = mem::replace(&mut run_loop.ticker, Ticker::default()).close_wait();

        let ashards = mem::replace(&mut run_loop.active_shards, BTreeMap::default());
        let mut shards = vec![];
        for (_, shard) in ashards.into_iter() {
            shards.push(shard.close_wait())
        }

        let flusher = {
            let val = mem::replace(&mut run_loop.flusher, Flusher::default());
            match app_fatal!(self, val.close_wait()) {
                Some(flusher) => flusher,
                None => return Response::Ok,
            }
        };

        let fin_state = FinState {
            state: run_loop.state,
            listener,
            ticker,
            flusher,
            active_shards: shards,
            topic_filters: run_loop.cc_topic_filters,
            retained_messages: run_loop.cc_retained_topics,
            stats: run_loop.stats,
        };

        info!("{} stats:{}", self.prefix, fin_state.to_json());
        let _init = mem::replace(&mut self.inner, Inner::Close(fin_state));
        self.prefix = self.prefix();

        Response::Ok
    }
}

impl Cluster {
    fn incr_n_events(&mut self, count: usize) {
        match &mut self.inner {
            Inner::Main(RunLoop { stats, .. }) => stats.n_events += count,
            Inner::Close(finstate) => finstate.stats.n_events += count,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn incr_n_requests(&mut self, count: usize) {
        match &mut self.inner {
            Inner::Main(RunLoop { stats, .. }) => stats.n_requests += count,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn prefix(&self) -> String {
        let state = match &self.inner {
            Inner::Init => "init",
            Inner::Handle(_, _) => "hndl",
            Inner::Tx(_, _) => "tx",
            Inner::Main(_) => "main",
            Inner::Close(_) => "close",
        };
        format!("<c:{}:{}>", self.name, state)
    }

    fn as_mut_poll(&mut self) -> &mut mio::Poll {
        match &mut self.inner {
            Inner::Main(RunLoop { poll, .. }) => poll,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn as_app_tx(&self) -> &mpsc::SyncSender<String> {
        match &self.inner {
            Inner::Main(RunLoop { app_tx, .. }) => app_tx,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }
}

/// Represents a Node in the cluster.
///
/// `address` is the socket-address in which the Node is listening for MQTT. Application
/// must provide a valid address, other fields like `weight` and `uuid` shall be assigned
/// a meaningful default.
#[derive(Clone)]
pub struct Node {
    /// Unique id of the node.
    pub uuid: Uuid,
    /// Refer to [ConfigNode::path]
    pub path: path::PathBuf,
    /// Refer to [ConfigNode::weight]
    pub weight: u16,
    /// Refer to [ConfigNode::mqtt_address].
    pub mqtt_address: net::SocketAddr, // listen address
}

impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.uuid == other.uuid
    }
}

impl Eq for Node {}

impl TryFrom<ConfigNode> for Node {
    type Error = Error;

    fn try_from(c: ConfigNode) -> Result<Node> {
        let num_cores = u16::try_from(num_cpus::get()).unwrap();
        let val = Node {
            uuid: c.uuid.parse()?,
            mqtt_address: c.mqtt_address,
            path: c.path,
            weight: c.weight.unwrap_or(num_cores),
        };

        Ok(val)
    }
}

impl Hostable for Node {
    fn uuid(&self) -> uuid::Uuid {
        self.uuid
    }

    fn weight(&self) -> u16 {
        self.weight
    }

    fn path(&self) -> path::PathBuf {
        self.path.clone()
    }
}

// TODO: Do we really needs all this field for a single node cluster ?
#[allow(dead_code)]
pub struct SingleNode {
    config: Config,
    node: Node,
    topology: Vec<rebalance::Topology>,
}

#[allow(dead_code)]
pub struct MultiNode {
    config: Config,
    nodes: Vec<Node>, // TODO: should we split this into gods and nodes.
    topology: Vec<rebalance::Topology>, // list of shards mapped to node.
}

impl ClusterState {
    /// Return the list of shard-numbers that are hosted in this node.
    #[allow(dead_code)]
    fn shards_in_node(&self, node: &Uuid) -> Vec<u32> {
        use ClusterState::*;

        let topology = match self {
            SingleNode { state } if node == &state.node.uuid => &state.topology,
            _ => unreachable!(),
        };
        topology.iter().filter(|t| node == &t.master.uuid).map(|t| t.shard).collect()
    }
}
