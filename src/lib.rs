//! Package implement MQTT protocol-framing for both client and server.

// TODO: review all err!() calls and tally them with MQTT spec.

#![feature(backtrace)]
#![feature(error_iter)]

#[cfg(any(feature = "fuzzy", test))]
use arbitrary::Arbitrary;

#[macro_use]
mod error;
mod types;
mod util;

#[macro_use]
pub mod v5;

#[cfg(any(feature = "fuzzy", test))]
pub mod fuzzy;

pub use error::{Error, ErrorKind, ReasonCode};
pub use types::{Blob, VarU32};
pub use types::{ClientID, MqttProtocol, UserProperty};
pub use types::{TopicFilter, TopicName};

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

#[cfg(test)]
#[path = "lib_test.rs"]
mod lib_test;
