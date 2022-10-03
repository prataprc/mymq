//! Broker implementation. Overview of broker [design].

use std::{net, path, sync::mpsc};

use crate::v5;

/// Used with [mio] library while polling for events.
pub const POLL_EVENTS_SIZE: usize = 1024;

/// Control Queue is processed in batches of this constant.
pub const CONTROL_CHAN_SIZE: usize = 1024;

/// Used by threads to sleep wait for an event to accur..
pub const SLEEP_10MS: std::time::Duration = std::time::Duration::from_millis(10);

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

pub struct SessionArgsActive {
    pub raddr: net::SocketAddr,
    pub config: Config,
    pub client_id: ClientID,
    pub shard_id: u32,
    pub miot_tx: PacketTx,
    pub session_rx: PacketRx,
    pub connect: v5::Connect,
}

pub struct SessionArgsReplica {
    pub raddr: net::SocketAddr,
    pub config: Config,
    pub client_id: ClientID,
    pub shard_id: u32,
    pub connect: v5::Connect,
}

pub mod design;

mod config;
mod flush;
mod keep_alive;
mod message;
mod session;
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
pub use spinlock::Spinlock;
pub use thread::{Rx, Thread, Threadable, Tx};
pub use ticker::Ticker;
pub use ttrie::{route_match, RetainedTrie, SubscribedTrie};

pub use crate::{util, IterTopicPath};
pub use crate::{ClientID, PacketID, Packetize, Timer, ToJson, TopicFilter, TopicName};
pub use crate::{Error, ErrorKind, ReasonCode, Result};
pub use crate::{PacketRx, PacketTx, Protocol, QPacket, QueueStatus, Socket};
