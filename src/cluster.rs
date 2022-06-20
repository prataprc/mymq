use log::{debug, error, info, warn};
use uuid::Uuid;

use std::{collections::BTreeMap, net};

use crate::thread::{Rx, Thread, Threadable, Tx};
use crate::{Config, ConfigNode, ConsistentHash, Hostable, Shard, Shardable};
use crate::{Error, ErrorKind, Result};

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
    /// Refer [Config::nodes]
    pub nodes: Vec<Node>,
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

struct RunLoop {
    rebalancer: Rebalancer,
    nodes: BTreeMap<Uuid, Node>,
    shards: BTreeMap<Uuid, Shard>,
}

impl Default for Cluster {
    fn default() -> Cluster {
        use crate::default_listen_address4;

        let config = Config::default();
        let node = Node {
            address: default_listen_address4(None),
            ..Node::default()
        };
        Cluster {
            name: config.name.to_string(),
            max_nodes: config.max_nodes.unwrap(),
            num_shards: config.num_shards.unwrap(),
            port: config.port.unwrap(),
            nodes: vec![node],
            chan_size: config.cluster_chan_size.unwrap(),
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
            Inner::Init => debug!("Cluster::Init, {:?} drop ...", self.name),
            Inner::Handle(_) => {
                error!("Cluster::Handle, {:?} invalid drop ...", self.name);
                panic!("Cluster::Handle, {:?} invalid drop ...", self.name);
            }
            Inner::Tx(_tx) => info!("Cluster::Tx {:?} drop ...", self.name),
            Inner::Main(_run_loop) => info!("Cluster::Main {:?} drop ...", self.name),
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
            name: config.name.clone(),
            max_nodes: config.max_nodes.unwrap_or(c.max_nodes),
            num_shards: config.num_shards.unwrap_or(c.num_shards),
            port: config.port.unwrap_or(c.port),
            chan_size: config.cluster_chan_size.unwrap_or(c.chan_size),
            nodes: if config.nodes.len() == 0 {
                c.nodes.clone()
            } else {
                let mut nodes = vec![];
                for n in config.nodes.clone().into_iter() {
                    nodes.push(TryFrom::try_from(n)?)
                }
                nodes
            },
            config,
            inner: Inner::Init,
        };

        Ok(val)
    }

    // should supply a restart location from disk.
    pub fn restart() -> Result<Cluster> {
        todo!()
    }

    pub fn spawn(mut self) -> Result<Cluster> {
        use crate::{util, MAX_NODES, MAX_SHARDS};

        info!(
            concat!(
                "starting cluster {:?} max_nodes:{} num_shards:{} ",
                "port:{} nodes:{} chan_size:{} ..."
            ),
            self.name,
            self.max_nodes,
            self.num_shards,
            self.port,
            self.nodes.len(),
            self.chan_size
        );

        if matches!(&self.inner, Inner::Handle(_) | Inner::Main(_)) {
            err!(InvalidInput, desc: "cluster can be spawned only in init-state ")?;
        }
        if self.num_shards > (MAX_SHARDS as usize) {
            err!(InvalidInput, desc: "num. of shards too large {}", self.num_shards)?;
        }
        if !util::is_power_of_2(self.num_shards) {
            err!(
                InvalidInput,
                desc: "num. of shards must be power of 2 {}",
                self.num_shards
            )?;
        }
        if self.max_nodes > MAX_NODES {
            err!(InvalidInput, desc: "num. of nodes too large {}", self.max_nodes)?;
        }

        let nodes: Vec<Node> = self.nodes.drain(..).collect();
        let cluster = Cluster {
            name: self.name.clone(),
            max_nodes: self.max_nodes,
            num_shards: self.num_shards,
            port: self.port,
            nodes: Vec::default(),
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner: Inner::Main(RunLoop {
                rebalancer: ConsistentHash::from_nodes(&nodes)?.into(),
                nodes: BTreeMap::from_iter(nodes.into_iter().map(|n| (n.uuid, n))),
                shards: BTreeMap::default(),
            }),
        };
        let thrd = Thread::spawn_sync(&self.name, self.chan_size, cluster);

        let cluster = Cluster {
            name: self.name.clone(),
            max_nodes: self.max_nodes,
            num_shards: self.num_shards,
            port: self.port,
            nodes: Vec::default(),
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner: Inner::Handle(thrd),
        };
        {
            let mut shards = BTreeMap::default();
            for _ in 0..self.num_shards {
                let shard =
                    Shard::from_config(self.config.clone())?.spawn(cluster.to_tx())?;
                shards.insert(shard.uuid, shard);
            }
            match &cluster.inner {
                Inner::Handle(thrd) => {
                    thrd.request(Request::SetShards(shards))??;
                }
                _ => unreachable!(),
            }
        }

        Ok(cluster)
    }

    pub fn to_tx(&self) -> Self {
        info!("Cluster::to_tx {:?} cloning tx ...", self.name);

        let inner = match &self.inner {
            Inner::Handle(thrd) => Inner::Tx(thrd.to_tx()),
            Inner::Tx(tx) => Inner::Tx(tx.clone()),
            _ => unreachable!(),
        };
        Cluster {
            name: self.name.clone(),
            max_nodes: self.max_nodes,
            num_shards: self.num_shards,
            port: self.port,
            nodes: Vec::default(),
            chan_size: self.chan_size,
            config: self.config.clone(),
            inner,
        }
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

pub enum Request {
    SetShards(BTreeMap<Uuid, Shard>),
    AddNodes { nodes: Vec<Node> },
    RemoveNodes { nodes: Vec<Uuid> },
    ShardMap { uuid: Uuid },
    Close,
}

// Handle Cluster
impl Cluster {
    pub fn add_nodes(&mut self, nodes: Vec<Node>) -> Result<()> {
        match &self.inner {
            Inner::Handle(thrd) => {
                thrd.request(Request::AddNodes { nodes })??;
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    pub fn remove_nodes(&mut self, nodes: Vec<Uuid>) -> Result<()> {
        match &self.inner {
            Inner::Handle(thrd) => {
                thrd.request(Request::RemoveNodes { nodes })??;
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    pub fn shard_to_node<T: Shardable>(&mut self, shard: T) -> Result<Uuid> {
        let uuid = shard.uuid();
        let node_uuid = match &self.inner {
            Inner::Handle(thrd) => thrd.request(Request::ShardMap { uuid })??,
            _ => unreachable!(),
        };
        match node_uuid {
            Response::NodeUuid(uuid) => Ok(uuid),
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

    fn main_loop(mut self, rx: Rx<Self::Req, Self::Resp>) -> Result<Self> {
        use crate::thread::pending_msg;
        use Request::*;

        loop {
            let (qs, disconnected) = pending_msg(&rx, 16);
            for q in qs.into_iter() {
                match q {
                    (SetShards(shards_handle), Some(tx)) => {
                        err!(IPCFail, try: tx.send(self.handle_set_shards(shards_handle)))?
                    }
                    (AddNodes { nodes }, Some(tx)) => {
                        err!(IPCFail, try: tx.send(self.handle_add_nodes(nodes)))?
                    }
                    (RemoveNodes { nodes }, Some(tx)) => err!(
                        IPCFail,
                        try: tx.send(self.handle_remove_nodes(nodes.as_slice()))
                    )?,
                    (ShardMap { uuid }, Some(tx)) => {
                        err!(IPCFail, try: tx.send(self.handle_shard_map(uuid)))?
                    }
                    (Close, Some(tx)) => {
                        err!(IPCFail, try: tx.send(self.handle_close()))?
                    }
                    (_, _) => unreachable!(),
                }
            }

            if disconnected {
                break;
            }
        }

        Ok(self)
    }
}

// Main loop
impl Cluster {
    fn handle_set_shards(&mut self, shards: BTreeMap<Uuid, Shard>) -> Result<Response> {
        let run_loop = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        run_loop.shards = shards;
        Ok(Response::Ok)
    }

    fn handle_add_nodes(&mut self, mut nodes: Vec<Node>) -> Result<Response> {
        let run_loop = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
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

        run_loop.rebalancer.add_nodes(&nodes)?;

        nodes.drain(..).for_each(|n| {
            run_loop.nodes.insert(n.uuid, n);
        });

        Ok(Response::Ok)
    }

    fn handle_remove_nodes(&mut self, uuids: &[Uuid]) -> Result<Response> {
        let run_loop = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
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

        run_loop.rebalancer.remove_nodes(&uuids)?;

        uuids.iter().for_each(|uuid| {
            run_loop.nodes.remove(uuid);
        });

        Ok(Response::Ok)
    }

    fn handle_shard_map(&self, uuid: Uuid) -> Result<Response> {
        let run_loop = match &self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        let node_uuid = run_loop.rebalancer.shard_to_node(uuid);
        match run_loop.nodes.get(&node_uuid) {
            Some(_) => Ok(Response::NodeUuid(node_uuid)),
            None => err!(InvalidInput, desc: "node {} not in cluster", node_uuid),
        }
    }

    fn handle_close(&mut self) -> Result<Response> {
        use std::mem;

        let RunLoop { nodes, shards, .. } = match &mut self.inner {
            Inner::Main(run_loop) => run_loop,
            _ => unreachable!(),
        };

        // TODO: is there any explicit clean up to be done for Node ?
        let hshards = mem::replace(shards, BTreeMap::default());
        let (n, m) = (nodes.len(), hshards.len());
        info!("Cluster::close, there are {} nodes and {} shards", n, m);

        for (uuid, shard) in hshards.into_iter() {
            let shard = shard.close_wait()?;
            shards.insert(uuid, shard);
        }

        Ok(Response::Ok)
    }
}

/// Represents a Node in the cluster. `address` is the socket-address in which the
/// Node is listening for MQTT. Application must provide a valid address, other fields
/// like `weight` and `uuid` shall be assigned a meaningful default.
#[derive(Clone)]
pub struct Node {
    pub address: net::SocketAddr, // listen address
    pub weight: u16,
    pub uuid: Uuid,
}

impl Default for Node {
    fn default() -> Node {
        use crate::default_listen_address4;

        Node {
            weight: u16::try_from(num_cpus::get()).unwrap(),
            address: default_listen_address4(None),
            uuid: Uuid::new_v4(),
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
            address: c.address,
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

/// The premises for rebalancer is that:
///
/// * A Cluster shall be made up of nodes, to host one or more shards.
/// * The complete set of all sessions shall be divided into shards.
/// * Number of shards are fixed when creating a cluster, and cannot change there after.
/// * Number of shards must be power of 2.
/// * Clients are mapped to shards using ClientID, so that a client will always map to
///   the same shard no matter when it is computed.
///  * One or more shards shall be mapped to a node.
///  * There should atleast be as many shards as the CPU cores in a node.
///  * Shard mapping to node is open-ended, Rebalancer can experiment with several
///    algorithsm
///  * Rebalancer shall account for replicas, for fault-tolerance and use consensus
///    protocol between master and replicas for "lossless" implementation of MQTT.
///  * Every master in the cluster shall be wired-up together via consensus protocol
///    for exchanging configuration information.
pub enum Rebalancer {
    CHash(ConsistentHash),
}

impl From<ConsistentHash> for Rebalancer {
    fn from(val: ConsistentHash) -> Rebalancer {
        Rebalancer::CHash(val)
    }
}

impl Rebalancer {
    pub fn add_nodes(&mut self, nodes: &[Node]) -> Result<()> {
        match self {
            Rebalancer::CHash(val) => val.add_nodes(nodes)?,
        };

        Ok(())
    }

    pub fn remove_nodes(&mut self, uuids: &[Uuid]) -> Result<()> {
        let nodes: Vec<Node> =
            uuids.iter().map(|uuid| Node { uuid: *uuid, ..Node::default() }).collect();
        match self {
            Rebalancer::CHash(val) => val.remove_nodes(&nodes)?,
        };

        Ok(())
    }

    pub fn shard_to_node(&self, uuid: Uuid) -> Uuid {
        match self {
            Rebalancer::CHash(val) => val.shard_to_node(uuid),
        }
    }
}
