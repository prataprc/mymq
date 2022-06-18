use log::{error, info, warn};
use uuid::Uuid;

use std::{collections::BTreeMap, net};

use crate::thread::{Rx, Thread, Threadable};
use crate::{ConsistentHash, Hostable, Shard, Shardable, MAX_NODES};
use crate::{Error, ErrorKind, Result};

/// Cluster is the global configuration state for multi-node MQTT cluster.
///
/// TODO: at some point in time this shall be integrated with consensus protocol for
/// lossless replication and fault-tolerance.
pub struct Cluster {
    /// human readable name of the cluster.
    pub name: String,
    /// Maximum nodes that can exist in this cluster. If not provided [MAX_NODES]
    /// shall be used. Immutable once created.
    pub max_nodes: usize,
    /// Fixed number of shards, of session/connections, that can exist in this cluster.
    /// Shards are assigned to nodes. If not provided [DEFAULT_SHARDS] shards shall be
    /// used. Immutable once created.
    pub num_shards: usize,
    /// Network listen address for `this` node. If not provided
    /// [default_listen_address4] shall be used. Immutable once created.
    pub address: net::SocketAddr, // listen address
    /// Initial set of nodes that are going to be part of this cluster. If not provided
    /// a default node shall be created from `address` and created as single-node
    /// cluster. Can mutate via [Cluster::add_nodes] and [Cluster::remove_nodes]
    /// methods.
    pub nodes: Vec<Node>,
    inner: Inner,
}

enum Inner {
    Init,
    Handle(Thread<Cluster, Request, Result<Response>>),
    Main(Main),
}

struct Main {
    rebalancer: Rebalancer,
    nodes: BTreeMap<Uuid, Node>,
    shards: BTreeMap<Uuid, Shard>,
}

impl Default for Cluster {
    fn default() -> Cluster {
        use crate::{default_listen_address4, DEFAULT_SHARDS};

        let node = Node {
            address: default_listen_address4(None),
            ..Node::default()
        };
        Cluster {
            name: "my-cluster".to_string(), // TODO: no magic
            max_nodes: MAX_NODES,
            num_shards: DEFAULT_SHARDS,
            address: default_listen_address4(None),
            nodes: vec![node],
            inner: Inner::Init,
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if self.nodes.len() > 0 {
            error!("Cluster::drop, unexpected cluster state {{nodes}}");
        }

        match &mut self.inner {
            Inner::Init => error!("Cluster::drop, invalid state Inner::Init"),
            Inner::Handle(handle) => match handle.close_wait() {
                Ok(_cluster) => (),
                Err(err) => error!("Cluster::drop, thread result: {}", err),
            },
            Inner::Main(run_loop) => {
                info!(
                    "Cluster::drop, there are {} nodes and {} shards",
                    run_loop.nodes.len(),
                    run_loop.shards.len()
                );
            }
        }
    }
}

// Handle cluster
impl Cluster {
    pub const CHANNEL_SIZE: usize = 16; // TODO: is this enough ?

    pub fn spawn(mut self) -> Result<Cluster> {
        use crate::{util, MAX_SHARDS};

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
        let shards = {
            let mut shards = BTreeMap::new();
            for i in 0..self.num_shards {
                let mut shard = Shard::default();
                shard.name = format!("{}-shard-{}", self.name, i);
                shards.insert(shard.uuid, shard.spawn()?);
            }
            shards
        };
        let run_loop = Cluster {
            name: self.name.clone(),
            max_nodes: self.max_nodes,
            num_shards: self.num_shards,
            address: self.address,
            nodes: Vec::default(),
            inner: Inner::Main(Main {
                rebalancer: ConsistentHash::from_nodes(&nodes)?.into(),
                nodes: BTreeMap::from_iter(nodes.into_iter().map(|n| (n.uuid, n))),
                shards,
            }),
        };

        let handle = Thread::spawn_sync(&self.name, Self::CHANNEL_SIZE, run_loop);

        let val = Cluster {
            name: self.name.clone(),
            max_nodes: self.max_nodes,
            num_shards: self.num_shards,
            address: self.address,
            nodes: Vec::default(),
            inner: Inner::Handle(handle),
        };

        Ok(val)
    }
}

pub enum Request {
    AddNodes { nodes: Vec<Node> },
    RemoveNodes { nodes: Vec<Uuid> },
    ShardMap { uuid: Uuid },
}

// Handle Cluster
impl Cluster {
    pub fn add_nodes(&mut self, nodes: Vec<Node>) -> Result<()> {
        match &self.inner {
            Inner::Handle(handle) => {
                handle.request(Request::AddNodes { nodes })??;
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    pub fn remove_nodes(&mut self, nodes: Vec<Uuid>) -> Result<()> {
        match &self.inner {
            Inner::Handle(handle) => {
                handle.request(Request::RemoveNodes { nodes })??;
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    pub fn shard_to_node<T: Shardable>(&mut self, shard: T) -> Result<Uuid> {
        let uuid = shard.uuid();
        let node_uuid = match &self.inner {
            Inner::Handle(handle) => handle.request(Request::ShardMap { uuid })??,
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
    pub fn handle_add_nodes(&mut self, mut nodes: Vec<Node>) -> Result<Response> {
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

    pub fn handle_remove_nodes(&mut self, uuids: &[Uuid]) -> Result<Response> {
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

    pub fn handle_shard_map(&self, uuid: Uuid) -> Result<Response> {
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
}

/// Represents a Node in the cluster. `address` is the socket-address in which the
/// Node is listening for MQTT. Application must provide a valid address, other fields
/// like `weight` and `uuid` shall be assigned a meaningful default.
pub struct Node {
    pub weight: u16,
    pub uuid: Uuid,
    pub address: net::SocketAddr, // listen address
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
