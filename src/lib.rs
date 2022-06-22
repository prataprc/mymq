//! Package implement MQTT protocol-framing for both client and server.

// TODO: review all err!() calls and tally them with MQTT spec.

#![feature(backtrace)]
#![feature(error_iter)]

#[macro_use]
mod error;
mod chash;
mod cluster;
mod config;
mod listener;
mod miot;
mod session;
mod shard;
mod thread;
mod types;
mod util;

#[macro_use]
pub mod v5;

#[cfg(any(feature = "fuzzy", test))]
pub mod fuzzy;

pub use chash::ConsistentHash;
pub use cluster::{Cluster, Node};
pub use config::{Config, ConfigNode};
pub use error::{Error, ErrorKind, ReasonCode};
pub use listener::Listener;
pub use miot::Miot;
pub use session::Session;
pub use shard::Shard;
pub use thread::{Thread, Threadable};
pub use types::{Blob, MqttProtocol, UserProperty, VarU32};
pub use types::{ClientID, TopicFilter, TopicName};

use uuid::Uuid;

use std::net::SocketAddr;

pub const MAX_NODES: usize = 1024;
pub const MAX_SHARDS: u32 = 0x8000;
pub const MAX_SESSIONS: usize = 1024 * 8;
pub const MQTT_PORT: u16 = 1883;
pub const CHANNEL_SIZE: usize = 1024;
pub const MAX_SOCKET_RETRY: usize = 128;

// TODO: restrict packet size to maximum allowed for each session or use
//       protocol-limitation

/// Result returned by this methods and functions defined in this package.
pub type Result<T> = std::result::Result<T, Error>;

/// Trait for protocol framing, data-encoding and decoding.
pub trait Packetize: Sized {
    /// Deserialize bytes and construct a packet or packet's field. Upon error, it is
    /// expected that the stream is left at meaningful boundry to re-detect the error.
    ///
    /// Also, the stream should alteast be as long as `remaining_len` in fixed-header
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)>;

    /// Serialize value into bytes, for small frames.
    fn encode(&self) -> Result<Blob>;
}

pub trait Hostable {
    fn uuid(&self) -> uuid::Uuid;

    fn weight(&self) -> u16;
}

pub trait Shardable {
    fn uuid(&self) -> uuid::Uuid;
}

pub trait NodeStore {
    fn len(&self) -> usize;

    fn get(&self, uuid: &Uuid) -> Option<&Node>;

    fn insert(&self, uuid: Uuid, node: Node);

    fn remove(&self, uuid: &Uuid);
}

/// Default listen address for MQTT packets: `0.0.0.0:1883`
pub fn mqtt_listen_address4(port: Option<u16>) -> SocketAddr {
    use std::net::{IpAddr, Ipv4Addr};

    let port = port.unwrap_or(MQTT_PORT);
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port)
}

#[cfg(test)]
#[path = "lib_test.rs"]
mod lib_test;
