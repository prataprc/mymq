use uuid::Uuid;

use std::{fmt, result};

use crate::Hostable;
use crate::{Error, ErrorKind, Result};

// Challenges in having consistent hashing.
// TODO: How random is the uuid ? Because we intend to use uuid to generate hash.
// TODO: how random can the hash value be ?
// TODO: Do we have a requirement that client can compute the client->shard and
//       shard->node mapping without consulting the cluster ?

pub struct ConsistentHash {
    conodes: Vec<(Uuid, u32)>, // (uuid, weighed-hash)
}

impl fmt::Debug for ConsistentHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        for n in self.conodes.iter() {
            write!(f, "node-hash-{}: 0x{:8x}", n.0, n.1)?;
        }
        Ok(())
    }
}

impl ConsistentHash {
    pub fn from_nodes<T: Hostable>(nodes: &[T]) -> Result<ConsistentHash> {
        match nodes.first() {
            Some(node) => {
                let (uuid, weight) = (node.uuid(), node.weight());
                let conodes = circle_of_hash(uuid, hash(uuid), weight);
                let mut ch = ConsistentHash { conodes };
                ch.add_nodes(&nodes[1..])?;

                Ok(ch)
            }
            None => err!(InvalidInput, desc: "Atleast one node must be a in cluster"),
        }
    }

    pub fn add_nodes<T: Hostable>(&mut self, nodes: &[T]) -> Result<&mut Self> {
        for node in nodes.iter() {
            let (uuid, weight) = (node.uuid(), node.weight());

            self.conodes.extend_from_slice(&circle_of_hash(uuid, hash(uuid), weight));
            self.conodes.sort_by(|a, b| a.1.cmp(&b.1));
        }

        Ok(self)
    }

    pub fn remove_nodes<T: Hostable>(&mut self, nodes: &[T]) -> Result<&mut Self> {
        for node in nodes.iter() {
            let uuid = node.uuid();
            let mut offs: Vec<usize> = self
                .conodes
                .iter()
                .enumerate()
                .filter(|(_, (uu, _))| &uuid == uu)
                .map(|(i, _)| i)
                .collect();
            offs.reverse();

            for i in offs.into_iter() {
                self.conodes.remove(i);
            }
        }

        Ok(self)
    }
}

impl ConsistentHash {
    /// take shard's uuid and return the node's uuid in which the shard is hosted.
    pub fn shard_to_node(&self, uuid: Uuid) -> Uuid {
        let hash = hash(uuid);
        let off = match self.conodes.binary_search_by_key(&hash, |a| a.1) {
            Ok(0) | Err(0) => self.conodes.len() - 1,
            Ok(off) | Err(off) => off - 1,
        };
        self.conodes[off].0
    }
}

// return (weight, weighed-hash)
fn circle_of_hash(uuid: Uuid, hash: u32, weight: u16) -> Vec<(Uuid, u32)> {
    use rand::{rngs::StdRng, Rng, SeedableRng};

    let mut rng = StdRng::seed_from_u64(((hash as u64) << 32) | ((hash as u64) << 32));

    let mut conodes: Vec<(Uuid, u32)> = (1..weight).map(|_| (uuid, rng.gen())).collect();
    conodes.insert(0, (uuid, hash));
    conodes.sort_by_key(|a| a.1);
    conodes
}

fn hash(uuid: Uuid) -> u32 {
    let bytes: &[u8] = uuid.as_ref();
    let mut hash = 0_u32;
    for (start, end) in (0..4).zip(1..5) {
        hash ^= u32::from_be_bytes(bytes[(start * 4)..(end * 4)].try_into().unwrap());
    }
    hash
}

#[cfg(test)]
#[path = "chash_test.rs"]
mod chash_test;
