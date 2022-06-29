//! Package implement MQTT protocol-framing for both client and server.

// TODO: review all err!() calls and tally them with MQTT spec.

#![feature(backtrace)]
#![feature(error_iter)]

#[macro_use]
mod error;
// mod chash; TODO
mod cluster;
mod config;
mod flush;
mod handshake;
mod listener;
mod miot;
mod packet;
mod queue;
mod rebalance;
mod session;
mod shard;
mod thread;
mod timer;
mod ttrie;
mod types;
mod util;

#[macro_use]
pub mod v5;

#[cfg(any(feature = "fuzzy", test))]
pub mod fuzzy;

// pub use chash::ConsistentHash; TODO
pub use cluster::{Cluster, Node};
pub use config::{Config, ConfigNode};
pub use error::{Error, ErrorKind, ReasonCode};
pub use flush::Flush;
pub use handshake::Handshake;
pub use listener::Listener;
pub use miot::Miot;
pub use session::Session;
pub use shard::Shard;
pub use thread::{Thread, Threadable};
pub use timer::Timer;
pub use ttrie::TopicTrie;
pub use types::{Blob, MqttProtocol, UserProperty, VarU32};
pub use types::{ClientID, TopicFilter, TopicName};

use std::{net, path, time};

pub const SLEEP_10MS: time::Duration = time::Duration::from_millis(10);

pub const MAX_SESSIONS: usize = 1024 * 8;
pub const MQTT_PORT: u16 = 1883;
pub const MAX_SOCKET_RETRY: usize = 128;
pub const MAX_FLUSH_RETRY: usize = 16;
pub const FIRST_TOKEN: mio::Token = mio::Token(2);

pub const REQ_CHANNEL_SIZE: usize = 1024;
pub const MSG_CHANNEL_SIZE: usize = 1024;
pub const MAX_PACKET_SIZE: usize = 1024 * 1024; // default is 1MB.

// TODO: restrict packet size to maximum allowed for each session or use
//       protocol-limitation

/// Result returned by this methods and functions defined in this package.
pub type Result<T> = std::result::Result<T, Error>;

/// Type alias for PacketID.
pub type PacketID = u16;

/// Trait for protocol framing, data-encoding and decoding. Shall return one of the
/// following error-kind: `ProtocolError`, `PacketEncode`, `PacketDecode`,
/// `MalformedPacket`.
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

    fn path(&self) -> path::PathBuf;
}

pub trait Shardable {
    fn uuid(&self) -> uuid::Uuid;
}

/// Default listen address for MQTT packets: `0.0.0.0:1883`
pub fn mqtt_listen_address4(port: Option<u16>) -> net::SocketAddr {
    use std::net::{IpAddr, Ipv4Addr};

    let port = port.unwrap_or(MQTT_PORT);
    net::SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port)
}

#[cfg(test)]
#[path = "lib_test.rs"]
mod lib_test;
