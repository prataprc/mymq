//! Broker implementation.

use std::{mem, net, path, sync::mpsc};

use crate::{v5, ClientID};

/// Used with [mio] library while polling for events.
pub const POLL_EVENTS_SIZE: usize = 1024;

/// Control Queue is processed in batches of this constant.
pub const CONTROL_CHAN_SIZE: usize = 1024;

/// Type alias for back-channel to application.
///
/// While creating the Cluster, application can pass an mpsc channel to [Cluster] that
/// the application will be listening on.
pub type AppTx = mpsc::SyncSender<String>;

/// Seqno counted for every outgoing publish packet for each session.
type OutSeqno = u64;

/// Seqno counted for every incoming publish packet for each shard.
type InpSeqno = u64;

/// Timestamp list managed at incoming publish, used to track lossless publish to all
/// subscribed-clients.
pub struct Timestamp {
    dst_shard_id: u32,
    last_routed: InpSeqno,
    last_acked: InpSeqno,
}

impl Timestamp {
    fn new(dst_shard_id: u32, last_routed: InpSeqno) -> Timestamp {
        Timestamp { dst_shard_id, last_routed, last_acked: 0 }
    }
}

/// Return type from methods used to send or receive messages/packets/commands.
///
/// This type is associated with methods that communicates with:
/// * Message-Queue, communication between shards
/// * Packet-Queue, communication between shard and socket
/// * Command-Queue, communication with thread.
#[derive(Clone)]
pub enum QueueStatus<T> {
    Ok(Vec<T>),           // holds remaining (for tx) or received (for rx) values
    Block(Vec<T>),        // holds remaining (for tx) or received (for rx) values
    Disconnected(Vec<T>), // holds remaining (for tx) or received (for rx) values
}

impl<T> QueueStatus<T> {
    pub fn take_values(&mut self) -> Vec<T> {
        let val = match self {
            QueueStatus::Ok(val) => val,
            QueueStatus::Block(val) => val,
            QueueStatus::Disconnected(val) => val,
        };
        mem::replace(val, Vec::new())
    }

    pub fn set_values(&mut self, values: Vec<T>) {
        let old_values = match self {
            QueueStatus::Ok(old_values) => old_values,
            QueueStatus::Block(old_values) => old_values,
            QueueStatus::Disconnected(old_values) => old_values,
        };
        assert!(old_values.len() == 0);
        let _empty = mem::replace(old_values, values);
    }

    pub fn map<U>(self, values: Vec<U>) -> QueueStatus<U> {
        use QueueStatus::*;

        let (old_values, val) = match self {
            Ok(old_values) => (old_values, Ok(values)),
            Block(old_values) => (old_values, Block(values)),
            Disconnected(old_values) => (old_values, Disconnected(values)),
        };
        assert!(old_values.len() == 0);
        val
    }

    pub fn is_disconnected(&self) -> bool {
        match self {
            QueueStatus::Disconnected(_) => true,
            _ => false,
        }
    }
}

/// Trait to be implemented by nodes that can host [Cluster] and one or more [Shard].
pub trait Hostable {
    /// Return universally unique id for this node.
    fn uuid(&self) -> uuid::Uuid;

    /// Return the weight of the node. Weight of the node is, typically, computed based
    /// on the hardware capabilities of the node.
    fn weight(&self) -> u16;

    /// Return the path of the node. Typically this maps to the location of the node.
    fn path(&self) -> path::PathBuf;
}

/// Trait implemented by [Shard].
pub trait Shardable {
    fn uuid(&self) -> uuid::Uuid;
}

/// Default listen address for MQTT packets: `0.0.0.0:1883`
pub fn mqtt_listen_address4(port: Option<u16>) -> net::SocketAddr {
    use std::net::{IpAddr, Ipv4Addr};

    let port = port.unwrap_or(Config::DEF_MQTT_PORT);
    net::SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port)
}

pub struct SessionArgsActive {
    pub raddr: net::SocketAddr,
    pub config: Config,
    pub client_id: ClientID,
    pub shard_id: u32,
    pub miot_tx: PktTx,
    pub session_rx: PktRx,
    pub connect: v5::Connect,
}

pub struct SessionArgsReplica {
    pub shard_id: u32,
    pub client_id: ClientID,
    pub raddr: net::SocketAddr,
    pub config: Config,
}

impl From<SessionArgsActive> for SessionArgsReplica {
    fn from(args: SessionArgsActive) -> SessionArgsReplica {
        SessionArgsReplica {
            shard_id: args.shard_id,
            client_id: args.shard_id.clone(),
            raddr: args.raddr,
            config: args.config.clone(),
        }
    }
}

mod config;
mod flush;
mod keep_alive;
mod message;
mod session;
mod socket;
mod spinlock;
mod thread;
mod ttrie;

mod cluster;
mod handshake;
mod listener;
mod miot;
mod rebalance;
mod shard;
mod ticker;

// TODO: mod consensus;
// TODO: mod rr;

pub use cluster::{Cluster, Node};
pub use config::{Config, ConfigNode};
pub use flush::Flusher;
pub use handshake::Handshake;
pub use keep_alive::KeepAlive;
pub use listener::Listener;
pub use message::{msg_channel, ConsensIO, Message, MsgRx, MsgTx, RouteIO};
pub use miot::Miot;
pub use session::Session;
pub use shard::Shard;
pub use socket::{pkt_channel, PktRx, PktTx, Socket};
pub use spinlock::Spinlock;
pub use thread::{Rx, Thread, Threadable, Tx};
pub use ticker::Ticker;
pub use ttrie::{route_match, RetainedTrie, SubscribedTrie};
