use log::{debug, info, trace};
use mio::event::Events;
use uuid::Uuid;

use std::sync::{mpsc, Arc};
use std::{collections::BTreeMap, fmt, mem, net, path, result, time};

use crate::broker::thread::{Rx, Thread, Threadable, Tx};
use crate::broker::{rebalance, ClusterAPI, Flusher, Listener, Shard, Ticker};
use crate::broker::{AppTx, Config, ConfigNode, Hostable, RetainedTrie, SubscribedTrie};
use crate::{util, Protocol, QPacket, QueueStatus, Socket, Timer, ToJson, TopicName};
use crate::{Error, ErrorKind, Result};

type ThreadRx = Rx<Request, Result<Response>>;
type QueueReq = crate::broker::thread::QueueReq<Request, Result<Response>>;

/// Type is the entry point to start/restart an broker instance.
pub struct Cluster {
    /// Refer [Config::name]
    pub name: String,
    prefix: String,
    config: Config,
    protos: Vec<Protocol>,

    inner: Inner,
}

enum Inner {
    Init,
    // Thread
    Main(RunLoop),
    // Held by application.
    Handle(Arc<mio::Waker>, Thread<Cluster, Request, Result<Response>>),
    // Held by Listener, Handshake, Ticker and Shard.
    Tx(Arc<mio::Waker>, Tx<Request, Result<Response>>),
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
    /// Consensus state.
    state: ClusterState,
    /// Rebalancing algorithm.
    rebalancer: rebalance::Rebalancer,
    /// Mio pooler for asynchronous handling, aggregate events from consensus port and
    /// waker.
    poll: mio::Poll,

    /// Listener thread for MQTT connections from remote/local clients.
    listeners: Vec<Listener<Cluster>>,
    /// Flusher thread for MQTT connections from remote/local clients.
    flusher: Flusher,
    /// Ticker thread to periodically wake up other threads, defaul is 10ms.
    ticker: Ticker<Cluster, Shard<Cluster>>,
    /// Total number of master shards within this node.
    masters: BTreeMap<u32, Shard<Cluster>>,
    /// Total number of replica shards within this node.
    replicas: BTreeMap<u32, Shard<Cluster>>,

    /// Index of subscribed topicfilters across all the sessions, local to this node.
    cc_topic_filters: SubscribedTrie, // key=TopicFilter, val=(client_id, shard_id)
    /// Index of retained messages for each topic-name, across all the sessions, local
    /// to this node.
    cc_retained_topics: RetainedTrie, // indexed by TopicName.
    /// Timer that managers expiry of retained messages.
    retain_timer: Timer<TopicName, QPacket>,

    /// Statistics
    stats: Stats,
    /// Back channel communicate with application.
    app_tx: AppTx,
}

pub struct FinState {
    pub state: ClusterState,

    pub listeners: Vec<Listener<Cluster>>,
    pub flusher: Flusher,
    pub masters: Vec<Shard<Cluster>>,
    pub replicas: Vec<Shard<Cluster>>,
    pub ticker: Ticker<Cluster, Shard<Cluster>>,

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
            protos: Vec::default(),
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
            concat!("{{ {:?}: {:?}, {:?}: {}, {:?}: {}, {:?}: {:?} }}"),
            "name",
            self.config.name,
            "max_nodes",
            self.config.max_nodes,
            "num_shards",
            self.config.num_shards,
            "nodes",
            self.config.nodes,
        )
    }

    fn to_stats_json(&self) -> String {
        match &self.inner {
            Inner::Main(RunLoop { masters, .. }) => {
                format!(concat!("{{ {:?}: {} }}"), "masters", masters.len())
            }
            _ => "{{}}".to_string(),
        }
    }
}

struct SpawnListeners<'a> {
    config: &'a Config,
    cluster: &'a Cluster,
    protos: Vec<Protocol>,
    app_tx: &'a AppTx,
}
struct SpawnMasters<'a> {
    config: &'a Config,
    cluster: &'a Cluster, // Inner::Handle
    flusher_tx: Flusher,
    cc_topic_filters: &'a SubscribedTrie,
    cc_retained_topics: &'a RetainedTrie,
    app_tx: &'a AppTx,
}
#[allow(dead_code)]
struct SpawnReplicas<'a> {
    config: &'a Config,
    cluster: &'a Cluster, // Inner::Handle
    app_tx: &'a AppTx,
}
struct SpawnTicker<'a> {
    config: &'a Config,
    cluster: &'a Cluster,
    masters: Vec<Shard<Cluster>>,
    replicas: Vec<Shard<Cluster>>,
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
            protos: Vec::default(),

            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    /// Start this cluster instance
    pub fn spawn(self, protos: Vec<Protocol>, app_tx: AppTx) -> Result<Cluster> {
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
            protos: protos.clone(),

            inner: Inner::Main(RunLoop {
                state,
                rebalancer,
                poll,

                listeners: Vec::default(),
                flusher,
                ticker: Ticker::default(),
                masters: BTreeMap::default(),
                replicas: BTreeMap::default(),

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
            protos: protos.clone(),

            inner: Inner::Handle(waker, thrd),
        };
        cluster.prefix = cluster.prefix();

        {
            let args = SpawnListeners {
                config: &self.config,
                cluster: &cluster,
                protos,
                app_tx: &app_tx,
            };
            let listeners = Self::spawn_listeners(args)?;

            let args = SpawnMasters {
                config: &self.config,
                cluster: &cluster,
                flusher_tx,
                cc_topic_filters: &cc_topic_filters,
                cc_retained_topics: &cc_retained_topics,
                app_tx: &app_tx,
            };
            let masters = Self::spawn_masters(args)?;
            for (_shard_id, shard) in masters.iter() {
                let iter = masters.iter().map(|(id, s)| (*id, s.to_msg_tx()));
                shard.set_shard_queues(BTreeMap::from_iter(iter));
            }

            let args = SpawnReplicas {
                config: &self.config,
                cluster: &cluster,
                app_tx: &app_tx,
            };
            let replicas = Self::spawn_replicas(args)?;

            let args = SpawnTicker {
                config: &self.config,
                cluster: &cluster,
                masters: masters.iter().map(|(_, shard)| shard.to_tx("ticker")).collect(),
                replicas: replicas
                    .iter()
                    .map(|(_, shard)| shard.to_tx("ticker"))
                    .collect(),
                app_tx: &app_tx,
            };
            let ticker = Self::spawn_ticker(args)?;

            if let Inner::Handle(_waker, thrd) = &cluster.inner {
                thrd.request(Request::Set { listeners, ticker, masters, replicas })??;
            } else {
                unreachable!("{} {:?}", self.prefix, cluster.inner)
            }
        }

        Ok(cluster)
    }

    fn spawn_listeners(args: SpawnListeners) -> Result<Vec<Listener<Cluster>>> {
        let mut listeners = Vec::default();
        for proto in args.protos.into_iter() {
            if proto.is_listen() {
                let listener = {
                    let listnr = Listener::from_config(args.config, proto)?;
                    listnr.spawn(args.cluster.to_tx("listener"), args.app_tx.clone())?
                };
                listeners.push(listener)
            }
        }

        Ok(listeners)
    }

    fn spawn_masters(args: SpawnMasters) -> Result<BTreeMap<u32, Shard<Cluster>>> {
        let mut masters = BTreeMap::default();
        for shard_id in 0..args.config.num_shards {
            let shard = {
                let spawn_args = crate::broker::shard::SpawnArgs {
                    cluster: args.cluster.to_tx("shard"),
                    flusher: args.flusher_tx.to_tx("shard"),
                    cc_topic_filters: args.cc_topic_filters.clone(),
                    cc_retained_topics: args.cc_retained_topics.clone(),
                };
                let shard = Shard::from_config(args.config, shard_id)?;
                shard.spawn_master(spawn_args, args.app_tx)?
            };

            masters.insert(shard_id, shard);
        }

        Ok(masters)
    }

    fn spawn_replicas(_args: SpawnReplicas) -> Result<BTreeMap<u32, Shard<Cluster>>> {
        let replicas = BTreeMap::default();
        Ok(replicas)
    }

    fn spawn_ticker(args: SpawnTicker) -> Result<Ticker<Cluster, Shard<Cluster>>> {
        use crate::broker::ticker::SpawnArgs;

        let ticker_args = SpawnArgs {
            cluster: Box::new(args.cluster.to_tx("ticker")),
            masters: args.masters,
            replicas: args.replicas,
            app_tx: args.app_tx.clone(),
        };
        Ticker::from_config(args.config.clone())?.spawn(ticker_args)
    }
}

pub enum Request {
    Set {
        listeners: Vec<Listener<Cluster>>,
        ticker: Ticker<Cluster, Shard<Cluster>>,
        masters: BTreeMap<u32, Shard<Cluster>>,
        replicas: BTreeMap<u32, Shard<Cluster>>,
    },
    SetRetainTopic {
        publish: QPacket,
    },
    ResetRetainTopic {
        topic_name: TopicName,
    },
    AddConnection(Socket),
    Close,
}

pub enum Response {
    Ok,
}

// calls to interface with cluster-thread.
impl ClusterAPI for Cluster {
    fn wake(&self) -> Result<()> {
        match &self.inner {
            Inner::Tx(waker, _) => Ok(waker.wake()?),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn add_connection(&self, sock: Socket) -> Result<()> {
        match &self.inner {
            Inner::Tx(_waker, tx) => {
                app_fatal!(self, tx.request(Request::AddConnection(sock)).flatten());
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        Ok(())
    }

    fn set_retain_topic(&self, publish: QPacket) -> Result<()> {
        match &self.inner {
            Inner::Tx(_waker, tx) => {
                app_fatal!(self, tx.post(Request::SetRetainTopic { publish }));
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }

        Ok(())
    }

    fn reset_retain_topic(&self, topic_name: TopicName) -> Result<()> {
        match &self.inner {
            Inner::Tx(_waker, tx) => {
                app_fatal!(self, tx.post(Request::ResetRetainTopic { topic_name }));
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
        Ok(())
    }

    fn close_wait(mut self) -> Cluster {
        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(_waker, thrd) => {
                app_fatal!(self, thrd.request(Request::Close));
                thrd.close_wait()
            }
            inner => unreachable!("{} {:?}", self.prefix, inner),
        }
    }

    fn to_tx(&self, who: &str) -> Self {
        let inner = match &self.inner {
            Inner::Handle(waker, thrd) => Inner::Tx(Arc::clone(waker), thrd.to_tx()),
            Inner::Tx(waker, tx) => Inner::Tx(Arc::clone(waker), tx.clone()),
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let mut val = Cluster {
            name: self.config.name.clone(),
            prefix: String::default(),
            config: self.config.clone(),
            protos: self.protos.clone(),
            inner,
        };
        val.prefix = val.prefix();

        debug!("{} cloned for {}", val.prefix, who);
        val
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
            let res = self.as_mut_poll().poll(&mut events, timeout);
            if let Err(err) = err!(IOError, try: res) {
                app_fatal!(&self, Result::<()>::Err(err));
                break;
            }

            if self.mio_events(&rx, &events) {
                break;
            }

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
                    app_fatal!(self, tx.send(Ok(resp)));
                }
                (req @ SetRetainTopic { .. }, None) => {
                    self.handle_set_retain_topic(req);
                }
                (req @ ResetRetainTopic { .. }, None) => {
                    self.handle_reset_retain_topic(req);
                }
                (req @ AddConnection(_), Some(tx)) => {
                    let resp = self.handle_add_connection(req);
                    app_fatal!(self, tx.send(Ok(resp)));
                }
                (req @ Close, Some(tx)) => {
                    let resp = self.handle_close(req);
                    app_fatal!(self, tx.send(Ok(resp)));
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

        let pkts: Vec<QPacket> = retain_timer.gc().collect();
        debug!("{} gc:{} pkts in retain_timer", self.prefix, pkts.len());

        // gather all retained packets and cleanup the RetainedTrie.
        for pkt in retain_timer.expired().collect::<Vec<QPacket>>() {
            cc_retained_topics.remove(pkt.as_topic_name());
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

        if let Request::Set { listeners, ticker, masters, replicas } = req {
            run_loop.ticker = ticker;
            run_loop.listeners = listeners;
            run_loop.masters = masters;
            run_loop.replicas = replicas;
        }

        Response::Ok
    }

    fn handle_set_retain_topic(&mut self, req: Request) {
        let publish = match req {
            Request::SetRetainTopic { publish } => publish,
            _ => unreachable!(),
        };
        let topic_name = publish.as_topic_name();

        let RunLoop { cc_retained_topics, retain_timer, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };
        // set this retain message as the latest one.
        cc_retained_topics.set(topic_name, publish.clone());
        retain_timer.delete(topic_name).unwrap();

        // book keeping for message expiry.
        if let Some(secs) = publish.message_expiry_interval() {
            retain_timer.add_timeout(secs as u64, topic_name.clone(), publish);
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
        let num_shards = self.config.num_shards;

        let mut sock = match req {
            Request::AddConnection(sock) => sock,
            _ => unreachable!(),
        };
        let raddr = sock.peer_addr();

        let RunLoop { masters, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            inner => unreachable!("{} {:?}", self.prefix, inner),
        };

        let client_id = sock.to_client_id();
        let shard_id = rebalance::Rebalancer::session_partition(&*client_id, num_shards);
        sock.set_shard_id(shard_id);

        match masters.get_mut(&shard_id) {
            Some(shard) => {
                info!(
                    "{} raddr:{} shard_id:{} new connection mapped",
                    self.prefix, raddr, shard_id
                );
                shard.add_session(sock);
            }
            None => {
                // multi-node cluster, look at the topology and redirect client using
                // connack::server_reference, and close the connection.
                todo!()
            }
        };

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

        let listeners: Vec<Listener<Cluster>> = {
            let listeners = mem::replace(&mut run_loop.listeners, Vec::default());
            listeners.into_iter().map(|l| l.close_wait()).collect()
        };
        let ticker = mem::replace(&mut run_loop.ticker, Ticker::default()).close_wait();

        let masters = mem::replace(&mut run_loop.masters, BTreeMap::default());
        let mut fin_masters = vec![];
        for (_, shard) in masters.into_iter() {
            fin_masters.push(shard.close_wait())
        }

        let replicas = mem::replace(&mut run_loop.replicas, BTreeMap::default());
        let mut fin_replicas = vec![];
        for (_, shard) in replicas.into_iter() {
            fin_replicas.push(shard.close_wait())
        }

        let flusher = {
            let flusher = mem::replace(&mut run_loop.flusher, Flusher::default());
            flusher.close_wait()
        };

        let fin_state = FinState {
            state: run_loop.state,

            listeners,
            flusher,
            masters: fin_masters,
            replicas: fin_replicas,
            ticker,

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

pub enum ClusterState {
    /// Cluster is single-node.
    SingleNode { state: SingleNode },
}

// TODO: Do we really needs all this field for a single node cluster ?
#[allow(dead_code)]
pub struct SingleNode {
    config: Config,
    node: Node,
    topology: Vec<rebalance::Topology<Node>>,
}

#[allow(dead_code)]
pub struct MultiNode {
    config: Config,
    nodes: Vec<Node>, // TODO: should we split this into gods and nodes.
    topology: Vec<rebalance::Topology<Node>>, // list of shards mapped to node.
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
