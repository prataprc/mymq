use log::{debug, error, info, warn};
use uuid::Uuid;

use std::{collections::BTreeMap, net, path, sync::mpsc};

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::{v5, Config, ConfigNode, Listener, Shard};
use crate::{Error, ErrorKind, Result};
use crate::{Hostable, NodeStore};

/// Cluster is the global configuration state for multi-node MQTT cluster.
///
/// TODO: at some point in time this shall be integrated with consensus protocol for
/// lossless replication and fault-tolerance.
pub struct Cluster {
    /// Refer [Config::name]
    pub name: String,
    /// Refer [Config::max_nodes]
    pub max_nodes: usize,
    /// Refer [Config::num_shards]
    pub num_shards: usize,
    /// Refer [Config::port]
    pub port: u16,
    /// Refer [Config::gods]
    pub gods: Vec<God>,
    /// Input channel size for the cluster thread.
    pub chan_size: usize,
    config: Config,
    inner: Inner,
}

enum Inner {
    Init,
    Handle(Thread<Cluster, Request, Result<Response>>),
    Tx(Tx<Request, Result<Response>>),
    Main(RunLoop),
}

pub struct RunLoop {
    /// God nodes, participate in cluster consensus.
    gods: Vec<God>,
    /// Nodes storage, TODO: we intend to handle very large number of nodes.
    nodes: Box<dyn NodeStore + 'static + Send>,
    /// Listener thread for MQTT connections from remote/local clients.
    listener: Listener,
    /// Total number of shards within this node.
    shards: BTreeMap<Uuid, Shard>,
    /// Channel to interface with application.
    app_tx: mpsc::SyncSender<String>,
}

impl Default for Cluster {
    fn default() -> Cluster {
        use crate::REQ_CHANNEL_SIZE;

        let config = Config::default();
        Cluster {
            name: config.name.to_string(),
            max_nodes: config.max_nodes.unwrap(),
            num_shards: config.num_shards.unwrap(),
            port: config.port.unwrap(),
            gods: Vec::default(),
            chan_size: REQ_CHANNEL_SIZE,
            config,
            inner: Inner::Init,
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Init => debug!("{} drop ...", self.pp()),
            Inner::Handle(_) => {
                error!("{} invalid drop ...", self.pp());
                panic!("{} invalid drop ...", self.pp());
            }
            Inner::Tx(_tx) => info!("{} drop ...", self.pp()),
            Inner::Main(_run_loop) => info!("{} drop ...", self.pp()),
        }
    }
}

// Handle cluster
impl Cluster {
    /// Create a cluster from configuration. Cluster shall be in `Init` state, to start
    /// the cluster call [Cluster::spawn]
    pub fn from_config(config: Config) -> Result<Cluster> {
        let c = Cluster::default();
        let val = Cluster {
            name: format!("{}-cluster-init", config.name),
            max_nodes: config.max_nodes.unwrap_or(c.max_nodes),
            num_shards: config.num_shards.unwrap_or(c.num_shards),
            port: config.port.unwrap_or(c.port),
            chan_size: c.chan_size,
            gods: Vec::default(),
            config,
            inner: Inner::Init,
        };

        Ok(val)
    }

    // should supply a restart location from disk.
    pub fn restart() -> Result<Cluster> {
        todo!()
    }

    pub fn spawn<N>(mut self, nodes: N, tx: mpsc::SyncSender<String>) -> Result<Cluster>
    where
        N: 'static + Send + NodeStore,
    {
        use crate::{util, MAX_NODES, MAX_SHARDS};
        use std::mem;

        if matches!(&self.inner, Inner::Handle(_) | Inner::Main(_)) {
            err!(InvalidInput, desc: "cluster can be spawned only in init-state ")?;
        }
        if self.num_shards > (MAX_SHARDS as usize) {
            err!(InvalidInput, desc: "num. of shards too large {}", self.num_shards)?;
        } else if self.num_shards == 0 {
            err!(InvalidInput, desc: "num_shards can't be ZERO")?;
        } else if !util::is_power_of_2(self.num_shards) {
            err!(
                InvalidInput,
                desc: "num. of shards must be power of 2 {}",
                self.num_shards
            )?;
        }
        if self.max_nodes > MAX_NODES {
            err!(InvalidInput, desc: "num. of nodes too large {}", self.max_nodes)?;
        }

        let gods = mem::replace(&mut self.gods, Vec::default());
        let shards = BTreeMap::default();
        let listener = Listener::default();

        let cluster = Cluster {
            name: format!("{}-cluster-main", self.config.name),
            max_nodes: self.max_nodes,
            num_shards: self.num_shards,
            port: self.port,
            gods: Vec::default(),
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                gods,
                nodes: Box::new(nodes),
                listener,
                shards,
                app_tx: tx,
            }),
        };
        let thrd = Thread::spawn_sync(&self.name, self.chan_size, cluster);

        let cluster = Cluster {
            name: format!("{}-cluster-handle", self.config.name),
            max_nodes: self.max_nodes,
            num_shards: self.num_shards,
            port: self.port,
            gods: Vec::default(),
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner: Inner::Handle(thrd),
        };
        {
            let mut shards = BTreeMap::default();
            for _ in 0..self.num_shards {
                let (config, clust_tx) = (self.config.clone(), cluster.to_tx());
                let shard = Shard::from_config(config)?.spawn(clust_tx)?;
                shards.insert(shard.uuid, shard);
            }

            let (config, clust_tx) = (self.config.clone(), cluster.to_tx());
            let listener = Listener::from_config(config)?.spawn(clust_tx)?;

            match &cluster.inner {
                Inner::Handle(thrd) => {
                    thrd.request(Request::Set { shards, listener })??;
                }
                _ => unreachable!(),
            }
        }

        Ok(cluster)
    }

    pub fn to_tx(&self) -> Self {
        info!("{} cloning tx ...", self.pp());

        let inner = match &self.inner {
            Inner::Handle(thrd) => Inner::Tx(thrd.to_tx()),
            Inner::Tx(tx) => Inner::Tx(tx.clone()),
            _ => unreachable!(),
        };
        Cluster {
            name: format!("{}-cluster-tx", self.config.name),
            max_nodes: self.max_nodes,
            num_shards: self.num_shards,
            port: self.port,
            gods: Vec::default(),
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner,
        }
    }
}

pub enum Request {
    Set {
        listener: Listener,
        shards: BTreeMap<Uuid, Shard>,
    },
    AddNodes {
        nodes: Vec<Node>,
    },
    RemoveNodes {
        uuids: Vec<Uuid>,
    },
    RestartChild {
        name: &'static str,
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
            Inner::Handle(thrd) => thrd.request(Request::AddNodes { nodes })??,
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn remove_nodes(&self, uuids: Vec<Uuid>) -> Result<()> {
        match &self.inner {
            Inner::Handle(thrd) => thrd.request(Request::RemoveNodes { uuids })??,
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
            Inner::Tx(tx) => tx.request(Request::AddConnection { conn, addr, pkt })??,
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn restart_listener(&self) -> Result<()> {
        match &self.inner {
            Inner::Tx(tx) => tx.post(Request::RestartChild { name: "listener" })?,
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn failed_shard(&self) -> Result<()> {
        match &self.inner {
            Inner::Tx(tx) => tx.post(Request::RestartChild { name: "shard" })?,
            _ => unreachable!(),
        };

        Ok(())
    }

    pub fn close_wait(mut self) -> Result<Cluster> {
        use std::mem;

        let inner = mem::replace(&mut self.inner, Inner::Init);
        match inner {
            Inner::Handle(thrd) => {
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
        use crate::thread::pending_requests;
        use Request::*;

        info!(
            "{} max_nodes:{} num_shards:{} port:{} gods:{} chan_size:{} ...",
            self.pp(),
            self.max_nodes,
            self.num_shards,
            self.port,
            self.gods.len(),
            self.chan_size
        );

        let mut closed = false;
        loop {
            let (mut qs, _empty, disconnected) = pending_requests(&rx, self.chan_size);
            if closed {
                info!("{} skipping {} requests closed:{}", self.pp(), qs.len(), closed);
                qs.drain(..);
            } else {
                debug!("{} process {} requests closed:{}", self.pp(), qs.len(), closed);
            }

            for q in qs.into_iter() {
                let res = match q {
                    (q @ Set { .. }, Some(tx)) => tx.send(self.handle_set(q)),
                    (q @ AddNodes { .. }, Some(tx)) => tx.send(self.handle_add_nodes(q)),
                    (q @ RemoveNodes { .. }, Some(tx)) => {
                        tx.send(self.handle_remove_nodes(q))
                    }
                    (RestartChild { name: "listener" }, None) => todo!(),
                    (RestartChild { name: "shard" }, None) => todo!(),
                    (q @ AddConnection { .. }, Some(tx)) => {
                        tx.send(self.handle_add_connection(q))
                    }
                    (q @ Close, Some(tx)) => {
                        closed = true;
                        tx.send(self.handle_close(q))
                    }

                    (_, _) => unreachable!(),
                };
                match res {
                    Ok(()) if closed => break,
                    Ok(()) => (),
                    Err(err) => {
                        let msg = format!("fatal error, {}", err.to_string());
                        allow_panic!(self.as_app_tx().send(msg));
                        break;
                    }
                }
            }

            if disconnected {
                break;
            }
        }

        info!("{} thread normal exit...", self.pp());
        self
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

    fn handle_add_nodes(&mut self, req: Request) -> Result<Response> {
        let run_loop = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        let mut nodes = match req {
            Request::AddNodes { nodes } => nodes,
            _ => unreachable!(),
        };

        let n = nodes.len() + run_loop.nodes.len();
        if n > self.max_nodes {
            err!(InvalidInput, desc: "num. of nodes too large {}", n)?;
        }
        // validate whether nodes are already present.
        for node in nodes.iter() {
            let uuid = node.uuid;
            match run_loop.nodes.get(&uuid) {
                Some(_) => err!(InvalidInput, desc: "node {} already present", uuid)?,
                None => (),
            }
        }

        for node in nodes.drain(..) {
            run_loop.nodes.insert(node.uuid, node)
        }

        Ok(Response::Ok)
    }

    fn handle_remove_nodes(&mut self, req: Request) -> Result<Response> {
        let run_loop = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        let uuids = match req {
            Request::RemoveNodes { uuids } => uuids,
            _ => unreachable!(),
        };

        if uuids.len() >= run_loop.nodes.len() {
            err!(InvalidInput, desc: "cannot remove all the nodes {}", uuids.len())?;
        }
        // validate whether nodes are already missing.
        for uuid in uuids.iter() {
            match run_loop.nodes.get(uuid) {
                Some(_) => (),
                None => warn!("node {} is missing", uuid),
            }
        }

        for uuid in uuids.iter() {
            run_loop.nodes.remove(uuid);
        }

        Ok(Response::Ok)
    }

    fn handle_add_connection(&mut self, req: Request) -> Result<Response> {
        let shards = match &mut self.inner {
            Inner::Main(RunLoop { shards, .. }) => shards,
            _ => unreachable!(),
        };
        let (conn, addr, pkt) = match req {
            Request::AddConnection { conn, addr, pkt } => (conn, addr, pkt),
            _ => unreachable!(),
        };

        let mut counts: Vec<(Uuid, usize)> =
            shards.iter_mut().map(|(uuid, shard)| (*uuid, shard.n_sessions)).collect();
        counts.sort_by_key(|x| x.1);

        match shards.get_mut(&counts[0].0) {
            Some(shard) => {
                shard.n_sessions += 1;
                shard.add_session(conn, addr, pkt)?;
            }
            None => (),
        }

        Ok(Response::Ok)
    }

    fn handle_close(&mut self, _: Request) -> Result<Response> {
        use std::mem;

        let RunLoop { nodes, listener, shards, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        let (n, m) = (nodes.len(), shards.len());
        info!("Cluster::close, there are {} nodes and {} shards", n, m);

        // TODO: is there any explicit clean up to be done for Node ?

        *listener = mem::replace(listener, Listener::default()).close_wait()?;

        let hshards = mem::replace(shards, BTreeMap::default());
        for (uuid, shard) in hshards.into_iter() {
            let shard = shard.close_wait()?;
            shards.insert(uuid, shard);
        }

        Ok(Response::Ok)
    }
}

impl Cluster {
    fn pp(&self) -> String {
        format!("{}", self.name)
    }

    fn as_app_tx(&self) -> &mpsc::SyncSender<String> {
        match &self.inner {
            Inner::Main(RunLoop { app_tx, .. }) => app_tx,
            _ => unreachable!(),
        }
    }
}

// TODO: we are yet to understand the scope of god-nodes. For now, they will be part
// of a consensus cirlce and decide addition/deletion of nodes, called god-nodes,
// from consensus circle. And also addition/deletion of federated-nodes.
pub struct God {
    pub address: net::SocketAddr,
    pub uuid: Uuid,
}

/// Represents a Node in the cluster. `address` is the socket-address in which the
/// Node is listening for MQTT. Application must provide a valid address, other fields
/// like `weight` and `uuid` shall be assigned a meaningful default.
#[derive(Clone)]
pub struct Node {
    /// Refer to [ConfigNode::mqtt_address].
    pub mqtt_address: net::SocketAddr, // listen address
    /// Refer to [ConfigNode::path]
    pub path: path::PathBuf,
    /// Refer to [ConfigNode::weight]
    pub weight: u16,
    /// Unique id of the node.
    pub uuid: Uuid,
}

impl Default for Node {
    fn default() -> Node {
        let cn = ConfigNode::default();
        Node {
            mqtt_address: cn.mqtt_address.clone(),
            path: cn.path.clone(),
            weight: cn.weight.unwrap(),
            uuid: cn.uuid.unwrap().parse().unwrap(),
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
}
