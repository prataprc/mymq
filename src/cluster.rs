use log::warn;
use uuid::Uuid;

use std::{collections::BTreeMap, net};

use crate::thread::{Rx, Thread, Threadable};
use crate::{ConsistentHash, Hostable, Shard, MAX_NODES};
use crate::{Error, ErrorKind, Result};

pub struct Cluster {
    pub max_nodes: usize,

    rebalancer: Rebalancer,
    nodes: BTreeMap<Uuid, Node>,
    shards: BTreeMap<Uuid, Shard>,
}

impl Cluster {
    pub fn new(max_nodes: usize, shards: Vec<Shard>, node: Node) -> Result<Cluster> {
        use crate::{util, MAX_SHARDS};

        let n = shards.len();
        if n > (MAX_SHARDS as usize) {
            err!(InvalidInput, desc: "num. of shards too large {}", n)?;
        }
        if max_nodes > MAX_NODES {
            err!(InvalidInput, desc: "num. of nodes too large {}", max_nodes)?;
        }
        if !util::is_power_of_2(n) {
            err!(InvalidInput, desc: "num. of shards must be power of 2 {}", n)?;
        }

        let nodes = vec![node];
        let val = Cluster {
            max_nodes,

            rebalancer: ConsistentHash::from_nodes(&nodes)?.into(),
            nodes: BTreeMap::from_iter(nodes.into_iter().map(|n| (n.uuid(), n))),
            shards: BTreeMap::from_iter(shards.into_iter().map(|s| (s.uuid(), s))),
        };

        Ok(val)
    }

    pub fn add_nodes(&mut self, mut nodes: Vec<Node>) -> Result<&mut Self> {
        let n = nodes.len() + self.nodes.len();
        if n > self.max_nodes {
            err!(InvalidInput, desc: "num. of nodes too large {}", n)?;
        }
        // validate whether nodes are already present.
        for node in nodes.iter() {
            let uuid = node.uuid();
            match self.nodes.get(&uuid) {
                Some(_) => err!(InvalidInput, desc: "node {} already present", uuid)?,
                None => (),
            }
        }

        self.rebalancer.add_nodes(&nodes)?;

        nodes.drain(..).for_each(|n| {
            self.nodes.insert(n.uuid(), n);
        });

        Ok(self)
    }

    pub fn remove_nodes(&mut self, nodes: &[Node]) -> Result<&mut Self> {
        if nodes.len() >= self.nodes.len() {
            err!(InvalidInput, desc: "cannot remove all the nodes {}", nodes.len())?;
        }
        // validate whether nodes are already missing.
        for node in nodes.iter() {
            let uuid = node.uuid();
            match self.nodes.get(&uuid) {
                Some(_) => (),
                None => warn!("node {} is missing", uuid),
            }
        }

        self.rebalancer.remove_nodes(nodes)?;

        nodes.iter().for_each(|n| {
            self.nodes.remove(&n.uuid());
        });

        Ok(self)
    }
}

impl Cluster {
    pub fn shard_to_node(&self, shard: &Shard) -> &Node {
        let node_uuid = self.rebalancer.shard_to_node(shard);
        self.get_node(node_uuid).unwrap()
    }

    pub fn get_node(&self, uuid: Uuid) -> Option<&Node> {
        self.nodes.get(&uuid)
    }

    pub fn get_shard(&self, uuid: Uuid) -> Option<&Shard> {
        self.shards.get(&uuid)
    }
}

pub enum Request {
    Todo,
}

pub enum Response {
    Todo,
}

impl Threadable for Cluster {
    type Req = Request;
    type Resp = Response;

    fn main_loop(self, rx: Rx<Self::Req, Self::Resp>) -> Self {
        todo!()
    }
}

pub struct Node {
    pub weight: u16,

    uuid: Uuid,
    address: net::SocketAddr,
}

impl Node {
    pub fn new(address: net::SocketAddr, weight: u16) -> Result<Node> {
        let val = Node { weight, uuid: Uuid::new_v4(), address };

        Ok(val)
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
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

    pub fn remove_nodes(&mut self, nodes: &[Node]) -> Result<()> {
        match self {
            Rebalancer::CHash(val) => val.remove_nodes(nodes)?,
        };

        Ok(())
    }

    pub fn shard_to_node(&self, shard: &Shard) -> Uuid {
        match self {
            Rebalancer::CHash(val) => val.shard_to_node(shard),
        }
    }
}
