use log::{debug, error, info};
use uuid::Uuid;

use std::{collections::BTreeMap, net, path, sync::mpsc};

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::Hostable;
use crate::{rebalance, v5, ClientID, Config, ConfigNode, Listener, Shard};
use crate::{Error, ErrorKind, Result};

pub type AppTx = mpsc::SyncSender<String>;

/// Cluster is the global configuration state for multi-node MQTT cluster.
pub struct Cluster {
    /// Refer [Config::name]
    pub name: String,
    /// Refer [Config::max_nodes]
    pub max_nodes: usize,
    /// Refer [Config::num_shards]
    pub num_shards: u32,
    /// Refer [Config::port]
    pub port: u16,
    prefix: String,
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
    state: ClusterState,
    /// Rebalancing algorithm,
    rebalancer: rebalance::Rebalancer,
    /// Listener thread for MQTT connections from remote/local clients.
    listener: Listener,
    /// Total number of shards within this node.
    shards: BTreeMap<u32, Shard>,
    /// Channel to interface with application.
    app_tx: mpsc::SyncSender<String>,
}

enum ClusterState {
    /// Cluster is single-node.
    SingleNode { state: SingleState },
    /// Cluster has its gods&nodes state updated, and in the processin of working out
    /// rebalancing.
    Elastic { state: MultiState },
    /// Cluster is stable.
    Stable { state: MultiState },
}

struct MultiState {
    config: Config,
    nodes: Vec<Node>, // TODO: should we split this into gods and nodes.
    topology: Vec<rebalance::Topology>,
}

struct SingleState {
    config: Config,
    node: Node,
}

impl Default for Cluster {
    fn default() -> Cluster {
        let config = Config::default();
        let mut def = Cluster {
            name: config.name.to_string(),
            max_nodes: config.max_nodes(),
            num_shards: config.num_shards(),
            port: config.port.unwrap(),
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
            Inner::Handle(_) => {
                error!("{} invalid drop ...", self.prefix);
                panic!("{} invalid drop ...", self.prefix);
            }
            Inner::Tx(_tx) => info!("{} drop ...", self.prefix),
            Inner::Main(_run_loop) => info!("{} drop ...", self.prefix),
        }
    }
}

// Handle cluster
impl Cluster {
    /// Create a cluster from configuration. Cluster shall be in `Init` state, to start
    /// the cluster call [Cluster::spawn]
    pub fn from_config(config: Config) -> Result<Cluster> {
        let def = Cluster::default();
        let mut val = Cluster {
            name: format!("{}-cluster-init", config.name),
            max_nodes: config.max_nodes(),
            num_shards: config.num_shards(),
            port: config.port.unwrap_or(def.port),
            prefix: def.prefix.clone(),
            config,
            inner: Inner::Init,
        };
        val.prefix = val.prefix();

        Ok(val)
    }

    pub fn spawn(self, node: Node, app_tx: AppTx) -> Result<Cluster> {
        use crate::util;

        if matches!(&self.inner, Inner::Handle(_) | Inner::Main(_)) {
            err!(InvalidInput, desc: "cluster can be spawned only in init-state ")?;
        }
        if self.num_shards == 0 {
            err!(InvalidInput, desc: "num_shards can't be ZERO")?;
        } else if !util::is_power_of_2(self.num_shards) {
            err!(
                InvalidInput,
                desc: "num. of shards must be power of 2 {}",
                self.num_shards
            )?;
        }

        let rebalancer = rebalance::Rebalancer {
            num_shards: self.num_shards,
            algo: rebalance::Algorithm::SingleNode,
        };
        let listener = Listener::default();
        let shards = BTreeMap::default();

        let state = ClusterState::SingleNode {
            state: SingleState { config: self.config.clone(), node },
        };

        let cluster = Cluster {
            name: format!("{}-cluster-main", self.config.name),
            max_nodes: self.max_nodes,
            num_shards: self.num_shards,
            port: self.port,
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner: Inner::Main(RunLoop { state, rebalancer, listener, shards, app_tx }),
        };
        let thrd = Thread::spawn(&self.prefix, cluster);

        let cluster = Cluster {
            name: format!("{}-cluster-handle", self.config.name),
            max_nodes: self.max_nodes,
            num_shards: self.num_shards,
            port: self.port,
            prefix: self.prefix.clone(),
            config: self.config.clone(),
            inner: Inner::Handle(thrd),
        };
        {
            let mut shards = BTreeMap::default();
            for shard_id in 0..self.num_shards {
                let (config, clust_tx) = (self.config.clone(), cluster.to_tx());
                let shard = Shard::from_config(config, shard_id)?.spawn(clust_tx)?;
                shards.insert(shard_id, shard);
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
        info!("{} cloning tx ...", self.prefix);

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
    RestartChild {
        name: &'static str,
    },
    AddConnection {
        conn: mio::net::TcpStream,
        addr: net::SocketAddr,
        pkt: v5::Connect,
    },
    RemoveConnection {
        client_id: ClientID,
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

    pub fn remove_connection(&self, client_id: ClientID) -> Result<()> {
        match &self.inner {
            Inner::Tx(tx) => tx.request(Request::RemoveConnection { client_id })??,
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
        use crate::{thread::pending_requests, REQ_CHANNEL_SIZE};
        use Request::*;

        info!(
            "{} spawn max_nodes:{} num_shards:{} port:{} ...",
            self.prefix, self.max_nodes, self.num_shards, self.port,
        );

        let mut closed = false;
        loop {
            let (mut qs, _empty, disconnected) = pending_requests(&rx, REQ_CHANNEL_SIZE);
            if closed {
                info!("{} skipping {} requests closed:{}", self.prefix, qs.len(), closed);
                qs.drain(..);
            } else {
                debug!("{} process {} requests closed:{}", self.prefix, qs.len(), closed);
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
                    (q @ RemoveConnection { .. }, Some(tx)) => {
                        tx.send(self.handle_remove_connection(q))
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
                        allow_panic!(self.prefix, self.as_app_tx().send(msg));
                        break;
                    }
                }
            }

            if disconnected {
                break;
            }
        }

        info!("{} thread normal exit...", self.prefix);
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

    fn handle_add_nodes(&mut self, _req: Request) -> Result<Response> {
        todo!()
    }

    fn handle_remove_nodes(&mut self, _req: Request) -> Result<Response> {
        todo!()
    }

    fn handle_add_connection(&mut self, req: Request) -> Result<Response> {
        let (conn, addr, pkt) = match req {
            Request::AddConnection { conn, addr, pkt } => (conn, addr, pkt),
            _ => unreachable!(),
        };

        let RunLoop { rebalancer, shards, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        let client_id = pkt.payload.client_id.clone();
        let (shard_uuid, subscribed_tx) = {
            let shard_num = rebalancer.session_parition(&*client_id);
            let shard = shards.get_mut(&shard_num).unwrap();
            let subscribed_tx = shard.add_session(conn, addr, pkt)?;
            (shard.uuid.clone(), subscribed_tx)
        };

        for (_, shard) in shards.iter().filter(|(_, s)| s.uuid != shard_uuid) {
            shard.book_session(client_id.clone(), subscribed_tx.clone())?;
        }

        Ok(Response::Ok)
    }

    fn handle_remove_connection(&mut self, req: Request) -> Result<Response> {
        let client_id = match req {
            Request::RemoveConnection { client_id } => client_id,
            _ => unreachable!(),
        };
        let RunLoop { shards, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        for (_, shard) in shards.iter() {
            shard.unbook_session(client_id.clone())?;
        }

        Ok(Response::Ok)
    }

    fn handle_close(&mut self, _: Request) -> Result<Response> {
        use std::mem;

        let RunLoop { listener, shards, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        info!("{}, closing {} shards hosted", self.prefix, shards.len());

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
    fn prefix(&self) -> String {
        format!("{}", self.name)
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
