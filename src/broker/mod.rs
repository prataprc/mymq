//! Broker implementation.

use std::{net, path, sync::mpsc, time};

use crate::Config;

/// Used by threads to sleep wait for an event to accur..
pub const SLEEP_10MS: time::Duration = time::Duration::from_millis(10);

/// Used with [mio] library while polling for events.
pub const POLL_EVENTS_SIZE: usize = 1024;

/// Control Queue is processed in batches of this constant.
pub const CONTROL_CHAN_SIZE: usize = 1024;

/// Type alias for back-channel to application.
///
/// While creating the Cluster, application can pass an mpsc channel to [Cluster] that
/// the application will be listening on.
pub type AppTx = mpsc::SyncSender<String>;

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
    fn take_values(&mut self) -> Vec<T> {
        use std::mem;

        let val = match self {
            QueueStatus::Ok(val) => val,
            QueueStatus::Block(val) => val,
            QueueStatus::Disconnected(val) => val,
        };
        mem::replace(val, Vec::new())
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

mod cluster;
mod flush;
mod handshake;
mod keep_alive;
mod listener;
mod message;
mod miot;
mod packet;
mod rebalance;
mod rr;
mod session;
mod shard;
mod socket;
mod spinlock;
mod thread;
mod ticker;
mod ttrie;

pub use cluster::{Cluster, Node};
pub use flush::Flusher;
pub use handshake::Handshake;
pub use keep_alive::KeepAlive;
pub use listener::Listener;
pub use message::{msg_channel, ClientInp, ClientOut, Message, MsgRx, MsgTx};
pub use miot::Miot;
pub use packet::{MQTTRead, MQTTWrite};
pub use session::Session;
pub use shard::Shard;
pub use socket::{pkt_channel, PktRx, PktTx, Socket};
pub use spinlock::Spinlock;
pub use thread::{Rx, Thread, Threadable, Tx};
pub use ticker::Ticker;
pub use ttrie::{RetainedTrie, SubscribedTrie};
