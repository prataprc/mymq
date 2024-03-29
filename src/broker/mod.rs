//! Broker implementation. Overview of broker [design].

use std::{path, sync::mpsc};

use crate::{ClientID, QPacket, Socket, TopicName};
use crate::{Error, Result};

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
    pub fn new(dst_shard_id: u32, last_routed: InpSeqno) -> Timestamp {
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

/// Trait abstraction for [Cluster].
///
/// Used by other components of broker.
pub trait ClusterAPI {
    /// Wake up the cluster
    fn wake(&self) -> Result<()>;

    /// Add a incoming connection to cluster
    fn add_connection(&self, sock: Socket);

    /// Set a publish message for topic-name
    fn set_retain_topic(&self, publish: QPacket);

    /// Remove publish message for given topic-name.
    fn reset_retain_topic(&self, topic_name: TopicName);

    /// Close this cluster
    fn close_wait(self) -> Self;

    /// Return a transmission channel to Cluster.
    fn to_tx(&self, who: &str) -> Self;
}

/// Trait abstraction for [Shard].
///
/// Used by other components of broker.
pub trait ShardAPI {
    type Clstr: ClusterAPI;

    fn to_shard_id(&self) -> u32;

    fn incr_inp_seqno(&mut self) -> InpSeqno;

    /// Wake up this shard
    fn wake(&self) -> Result<()>;

    /// Flush session's socket packet queue.
    fn flush_session(&self, pq: PQueue, err: Option<Error>);

    /// Delete will message for `client_id`.
    fn delete_will_message(&mut self, client_id: &ClientID) -> Result<()>;

    fn as_topic_filters(&self) -> &SubscribedTrie;

    fn as_retained_topics(&self) -> &RetainedTrie;

    fn as_cluster(&self) -> &Self::Clstr;
}

fn generate_client_id(config: &Config, _sock: &Socket) -> ClientID {
    match config.client_id_generator.as_str() {
        "uuid_v5" => ClientID::new_uuid_v4(),
        _ => unreachable!(), // NOTE: must have been already validated
    }
}

pub mod design;
// TODO: mod consensus;
pub mod rebalance;

mod config;
mod spinlock;
mod thread;
mod ttrie;
pub use config::{Config, ConfigNode};
pub use spinlock::Spinlock;
pub use thread::{Rx, Thread, Threadable, Tx};
pub use ttrie::{route_match, RetainedTrie, SubscribedTrie};

mod keep_alive;
mod message;
pub use keep_alive::KeepAlive;
pub use message::{msg_channel, ConsensIO, Message, MsgRx, MsgTx, RouteIO};

mod cluster;
mod flush;
mod handshake;
mod listener;
mod miot;
mod session;
mod shard;
mod ticker;

pub use cluster::{Cluster, Node};
pub use flush::Flusher;
pub use handshake::Handshake;
pub use listener::Listener;
pub use miot::{Miot, PQueue, PQueueArgs};
pub use session::{Session, SessionArgsMaster, SessionArgsReplica};
pub use shard::Shard;
pub use ticker::Ticker;
