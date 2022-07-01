use log::{debug, error, info, trace};
use mio::event::Events;
use uuid::Uuid;

use std::sync::{mpsc, Arc};
use std::{collections::BTreeMap, net, path, time};

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::{rebalance, util, v5};
use crate::{Config, ConfigNode, Flusher, Hostable, Listener, Shard, TopicTrie};
use crate::{Error, ErrorKind, Result};

// TODO: Review .ok() .unwrap() allow_panic!(), panic!() and unreachable!() calls.
// TODO: Review `as` type-casting for numbers.
// TODO: Validate and document all thread handles, cluster, listener, flusher, shard,
//       miot.

type ThreadRx = Rx<Request, Result<Response>>;

pub type AppTx = mpsc::SyncSender<String>;

/// Cluster is the global configuration state for multi-node MQTT cluster.
pub struct Cluster {
    /// Refer [Config::name]
    pub name: String,
    prefix: String,
    config: Config,
    inner: Inner,
}

enum Inner {
    Init,
    // Help by application.
    Handle(Arc<mio::Waker>, Thread<Cluster, Request, Result<Response>>),
    // Held by Listener, Handshake and Shard.
    Tx(Arc<mio::Waker>, Tx<Request, Result<Response>>),
    Main(RunLoop),
}

pub struct RunLoop {
    // Consensus state.
    state: ClusterState,

    /// Mio pooler for asynchronous handling, aggregate events from consensus port and
    /// waker.
    poll: mio::Poll,
    /// Listener thread for MQTT connections from remote/local clients.
    listener: Listener,
    /// Flusher thread for MQTT connections from remote/local clients.
    flusher: Flusher,
    /// Total number of shards within this node.
    shards: BTreeMap<u32, Shard>,
    /// Channel to interface with application.
    app_tx: mpsc::SyncSender<String>,

    /// Rebalancing algorithm.
    rebalancer: rebalance::Rebalancer,
    /// List of subscribed topicfilters across all the sessions, local to this node.
    // TODO: Should we make this part of the ClusterState ?
    topic_filters: TopicTrie,

    /// thread is already closed.
    closed: bool,
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
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => debug!("{} drop ...", self.prefix),
            Inner::Handle(_waker, _thrd) => {
                error!("{} invalid drop ...", self.prefix);
                panic!("{} invalid drop ...", self.prefix);
            }
            Inner::Tx(_waker, _tx) => info!("{} drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
        }
    }
}

// Handle cluster
impl Cluster {
    /// Poll register token for waker event, OTP calls makde to this thread shall trigger
    /// this event.
    pub const TOKEN_WAKE: mio::Token = mio::Token(1);
    /// Poll register for consensus TcpStream.
    pub const TOKEN_CONSENSUS: mio::Token = mio::Token(2);

    /// Create a cluster from configuration. Cluster shall be in `Init` state, to start
    /// the cluster call [Cluster::spawn]
    pub fn from_config(config: Config) -> Result<Cluster> {
        // validate
        if config.num_shards() == 0 {
            err!(InvalidInput, desc: "num_shards can't be ZERO")?;
        } else if !util::is_power_of_2(config.num_shards()) {
            err!(
                InvalidInput,
                desc: "num. of shards must be power of 2 {}",
                config.num_shards()
            )?;
        }

        let def = Cluster::default();
        let mut val = Cluster {
            name: format!("{}-cluster-init", config.name),
            prefix: def.prefix.clone(),
            config,
            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, node: Node, app_tx: AppTx) -> Result<Cluster> {
        use mio::Waker;

        if matches!(&self.inner, Inner::Handle(_, _) | Inner::Main(_)) {
            err!(InvalidInput, desc: "cluster can be spawned only in init-state ")?;
        }

        let poll = err!(IOError, try: mio::Poll::new(), "fail creating mio::Poll")?;
        let waker = Arc::new(Waker::new(poll.registry(), Self::TOKEN_WAKE)?);

        let rebalancer = rebalance::Rebalancer {
            config: self.config.clone(),
            algo: rebalance::Algorithm::SingleNode,
        };

        let state = {
            let topology = rebalancer.rebalance(&vec![node.clone()], vec![]);
            ClusterState::SingleNode {
                state: SingleNode { config: self.config.clone(), node, topology },
            }
        };

        let listener = Listener::default();
        let flusher = Flusher::from_config(self.config.clone())?.spawn()?;
        let flusher_tx = flusher.to_tx();
        let shards = BTreeMap::default();

        let topic_filters = TopicTrie::new();
        let cluster = Cluster {
            name: format!("{}-cluster-main", self.config.name),
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                state,

                poll,
                listener,
                flusher,
                shards,
                app_tx,

                rebalancer,
                topic_filters: topic_filters.clone(),

                closed: false,
            }),
        };
        let thrd = Thread::spawn(&self.prefix, cluster);

        let cluster = Cluster {
            name: format!("{}-cluster-handle", self.config.name),
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner: Inner::Handle(waker, thrd),
        };
        {
            let mut shards = BTreeMap::default();
            let mut shard_queues = BTreeMap::default();
            for shard_id in 0..self.config.num_shards() {
                let (config, cluster_tx) = (self.config.clone(), cluster.to_tx());
                let shard = {
                    let args = crate::shard::SpawnArgs {
                        cluster: cluster_tx,
                        flusher: flusher_tx.to_tx(),
                        topic_filters: topic_filters.clone(),
                    };
                    Shard::from_config(config, shard_id)?.spawn(args)?
                };
                shard_queues.insert(shard.shard_id, shard.to_msg_tx());
                shards.insert(shard_id, shard);
            }

            for (_shard_id, shard) in shards.iter() {
                let iter = shard_queues.iter().map(|(id, s)| (*id, s.to_msg_tx()));
                let shard_queues = BTreeMap::from_iter(iter);
                shard.set_shard_queues(shard_queues)?;
            }

            let (config, clust_tx) = (self.config.clone(), cluster.to_tx());
            let listener = Listener::from_config(config)?.spawn(clust_tx)?;

            match &cluster.inner {
                Inner::Handle(waker, thrd) => {
                    waker.wake()?;
                    thrd.request(Request::Set { shards, listener })??;
                }
                _ => unreachable!(),
            }
        }

        Ok(cluster)
    }

    pub fn to_tx(&self) -> Self {
        info!("{} cloning tx ...", self.prefix);

        let inner = match &self.inner {
            Inner::Handle(waker, thrd) => Inner::Tx(Arc::clone(waker), thrd.to_tx()),
            Inner::Tx(waker, tx) => Inner::Tx(Arc::clone(waker), tx.clone()),
            _ => unreachable!(),
        };
        Cluster {
            name: format!("{}-cluster-tx", self.config.name),
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner,
        }
    }
}

pub enum Request {
    Set {
        listener: Listener,
        shards: BTreeMap<u32, Shard>,
    },
    AddNodes {
        nodes: Vec<Node>,
    },
    RemoveNodes {
        uuids: Vec<Uuid>,
    },
    AddConnection {
        conn: mio::net::TcpStream,
        addr: net::SocketAddr,
        pkt: v5::Connect,
    },
    Close,
}

// calls to interfacw with cluster-thread.
impl Cluster {
    pub fn add_nodes(&self, nodes: Vec<Node>) -> Result<()> {
        match &self.inner {
            Inner::Handle(waker, thrd) => {
                waker.wake()?;
                thrd.request(Request::AddNodes { nodes })??
            }
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn remove_nodes(&self, uuids: Vec<Uuid>) -> Result<()> {
        match &self.inner {
            Inner::Handle(waker, thrd) => {
                waker.wake()?;
                thrd.request(Request::RemoveNodes { uuids })??
            }
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn add_connection(
        &self,
        conn: mio::net::TcpStream,
        addr: net::SocketAddr,
        pkt: v5::Connect,
    ) -> Result<()> {
        match &self.inner {
            Inner::Tx(waker, tx) => {
                waker.wake()?;
                tx.request(Request::AddConnection { conn, addr, pkt })??
            }
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn close_wait(mut self) -> Result<Cluster> {
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(waker, thrd) => {
                waker.wake()?;
                thrd.request(Request::Close)??;
                thrd.close_wait()
            }
            _ => unreachable!(),
        }
    }
}

pub enum Response {
    Ok,
    NodeUuid(Uuid),
}

impl Threadable for Cluster {
    type Req = Request;
    type Resp = Result<Response>;

    fn main_loop(mut self, rx: Rx<Self::Req, Self::Resp>) -> Self {
        info!(
            "{} spawn max_nodes:{} num_shards:{} ...",
            self.prefix,
            self.config.max_nodes(),
            self.config.num_shards(),
        );

        let mut events = Events::with_capacity(crate::POLL_EVENTS_SIZE);
        let res = loop {
            let timeout: Option<time::Duration> = None;
            match self.as_mut_poll().poll(&mut events, timeout) {
                Ok(()) => (),
                Err(err) => {
                    break err!(IOError, try: Err(err), "{} poll error", self.prefix)
                }
            };

            match self.mio_events(&rx, &events) {
                // Exit or not
                Ok(true) => break Ok(()),
                Ok(false) => (),
                Err(err) => break Err(err),
            };
        };

        // handle_close should be idempotent call.
        match self.handle_close(Request::Close) {
            Ok(Response::Ok) => (),
            Err(err) => {
                let msg = format!("handle_close, fatal error, {}", err.to_string());
                allow_panic!(self.prefix, self.as_app_tx().send(msg));
            }
            _ => unreachable!(),
        }

        match res {
            Ok(()) => info!("{}, thread exit ...", self.prefix),
            Err(err) => {
                let msg = format!("fatal error, {}", err.to_string());
                allow_panic!(self.prefix, self.as_app_tx().send(msg));
            }
        };

        self
    }
}

impl Cluster {
    // return (exit,)
    fn mio_events(&mut self, rx: &ThreadRx, events: &Events) -> Result<bool> {
        let mut count = 0_usize;
        let mut iter = events.iter();
        let res = 'outer: loop {
            match iter.next() {
                Some(event) => {
                    trace!("{}, poll-event token:{}", self.prefix, event.token().0);
                    count += 1;

                    match event.token() {
                        Self::TOKEN_WAKE => loop {
                            // keep repeating until all control requests are drained
                            match self.drain_control_chan(rx)? {
                                (_empty, true) => break 'outer Ok(true),
                                (true, _disconnected) => break,
                                (false, false) => (),
                            }
                        },
                        Self::TOKEN_CONSENSUS => todo!(),
                        _ => unreachable!(),
                    }
                }
                None => break Ok(false),
            }
        };

        debug!("{}, polled and got {} events", self.prefix, count);
        res
    }

    // Return (empty, disconnected)
    fn drain_control_chan(&mut self, rx: &ThreadRx) -> Result<(bool, bool)> {
        use crate::{thread::pending_requests, CONTROL_CHAN_SIZE};
        use Request::*;

        let closed = match &self.inner {
            Inner::Main(RunLoop { closed, .. }) => *closed,
            _ => unreachable!(),
        };

        let (mut qs, empty, disconnected) = pending_requests(rx, CONTROL_CHAN_SIZE);

        if closed {
            info!("{} skipping {} requests closed:{}", self.prefix, qs.len(), closed);
            qs.drain(..);
        } else {
            debug!("{} process {} requests closed:{}", self.prefix, qs.len(), closed);
        }

        // TODO: review control-channel handling for all threads. Should we panic or
        // return error.
        for q in qs.into_iter() {
            match q {
                (q @ Set { .. }, Some(tx)) => {
                    err!(IPCFail, try: tx.send(self.handle_set(q)))?;
                }
                (q @ AddNodes { .. }, Some(tx)) => {
                    err!(IPCFail, try: tx.send(self.handle_add_nodes(q)))?;
                }
                (q @ RemoveNodes { .. }, Some(tx)) => {
                    err!(IPCFail, try: tx.send(self.handle_remove_nodes(q)))?;
                }
                (q @ AddConnection { .. }, Some(tx)) => {
                    err!(IPCFail, try: tx.send(self.handle_add_connection(q)))?;
                }
                (q @ Close, Some(tx)) => {
                    err!(IPCFail, try: tx.send(self.handle_close(q)))?;
                }

                (_, _) => unreachable!(), // TODO: log meaning message.
            };
        }

        Ok((empty, disconnected))
    }
}

// Main loop
impl Cluster {
    fn handle_set(&mut self, req: Request) -> Result<Response> {
        let run_loop = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        match req {
            Request::Set { listener, shards } => {
                run_loop.listener = listener;
                run_loop.shards = shards;
            }
            _ => unreachable!(),
        }

        Ok(Response::Ok)
    }

    fn handle_add_nodes(&mut self, _req: Request) -> Result<Response> {
        todo!()
    }

    fn handle_remove_nodes(&mut self, _req: Request) -> Result<Response> {
        todo!()
    }

    fn handle_add_connection(&mut self, req: Request) -> Result<Response> {
        use crate::shard::AddSessionArgs;

        let (conn, addr, connect) = match req {
            Request::AddConnection { conn, addr, pkt } => (conn, addr, pkt),
            _ => unreachable!(),
        };

        let RunLoop { shards, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        let client_id = connect.payload.client_id.clone();
        let shard_id = rebalance::Rebalancer::session_parition(
            &*client_id,
            self.config.num_shards(),
        );

        let shard = match shards.get_mut(&shard_id) {
            Some(shard) => shard,
            None => {
                // multi-node cluster, look at the topology and redirect client using
                // connack::server_reference, and close the connection.
                todo!()
            }
        };
        info!("{}, new connection {:?} mapped to shard {}", self.prefix, addr, shard_id);

        // Add session to the shard.
        shard.add_session(AddSessionArgs { conn, addr, pkt: connect })?;

        Ok(Response::Ok)
    }

    fn handle_close(&mut self, _: Request) -> Result<Response> {
        use std::mem;

        let RunLoop { listener, flusher, shards, closed, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        if *closed == false {
            info!("{}, closing {} shards hosted", self.prefix, shards.len());

            *listener = mem::replace(listener, Listener::default()).close_wait()?;
            *flusher = mem::replace(flusher, Flusher::default()).close_wait()?;

            let hshards = mem::replace(shards, BTreeMap::default());
            for (uuid, shard) in hshards.into_iter() {
                let shard = shard.close_wait()?;
                shards.insert(uuid, shard);
            }

            *closed = true;
        }

        Ok(Response::Ok)
    }
}

impl Cluster {
    fn prefix(&self) -> String {
        format!("{}", self.name)
    }

    fn as_mut_poll(&mut self) -> &mut mio::Poll {
        match &mut self.inner {
            Inner::Main(RunLoop { poll, .. }) => poll,
            _ => unreachable!(),
        }
    }

    fn as_app_tx(&self) -> &mpsc::SyncSender<String> {
        match &self.inner {
            Inner::Main(RunLoop { app_tx, .. }) => app_tx,
            _ => unreachable!(),
        }
    }
}

/// Represents a Node in the cluster. `address` is the socket-address in which the
/// Node is listening for MQTT. Application must provide a valid address, other fields
/// like `weight` and `uuid` shall be assigned a meaningful default.
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

impl Default for Node {
    fn default() -> Node {
        let config = ConfigNode::default();
        Node {
            mqtt_address: config.mqtt_address.clone(),
            path: config.path.clone(),
            weight: config.weight.unwrap(),
            uuid: config.uuid.unwrap().parse().unwrap(),
        }
    }
}

impl TryFrom<ConfigNode> for Node {
    type Error = Error;

    fn try_from(c: ConfigNode) -> Result<Node> {
        let node = Node::default();
        let uuid = match c.uuid.clone() {
            Some(uuid) => err!(InvalidInput, try: uuid.parse::<Uuid>())?,
            None => node.uuid,
        };

        let val = Node {
            mqtt_address: c.mqtt_address,
            path: c.path,
            weight: c.weight.unwrap_or(node.weight),
            uuid,
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

enum ClusterState {
    /// Cluster is single-node.
    SingleNode { state: SingleNode },
    /// Cluster is in the process of updating its gods&nodes, and working out rebalance.
    Elastic { state: MultiNode },
    /// Cluster is stable.
    Stable { state: MultiNode },
}

struct MultiNode {
    config: Config,
    nodes: Vec<Node>, // TODO: should we split this into gods and nodes.
    topology: Vec<rebalance::Topology>, // list of shards mapped to node.
}

struct SingleNode {
    config: Config,
    node: Node,
    topology: Vec<rebalance::Topology>,
}

impl ClusterState {
    /// Return the list of shard-numbers that are hosted in this node.
    fn shards_in_node(&self, node: &Uuid) -> Vec<u32> {
        use ClusterState::*;

        let topology = match self {
            SingleNode { state } if node == &state.node.uuid => &state.topology,
            Stable { state } => &state.topology,
            _ => unreachable!(), // TODO: meaningful return.
        };
        topology.iter().filter(|t| node == &t.master.uuid).map(|t| t.shard).collect()
    }
}
