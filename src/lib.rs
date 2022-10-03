//! Package implement message queue _broker_ based on MQTT protocol.
//!
//! Currently implements MQTT-v5 _broker_ and a corresponding _client_. Based [on
//! popular demand][issue-4] other message-queue protocols shall be integrated into
//! this broker.
//!
//! #### Features
//!
//! _*broker*_, enabled by default, provides all the necessary items needed to build
//! a MQTT broker application. Enabling broker, will automatically enable client.
//!
//! _*client*_, enabled by default, provides all the necessary items needed to build
//! an MQTT client. Application that doesn't require a broker can disable default
//! features via `--no-default-features` in cmd-line or via `default-features = false`
//! in [dependency declaration][dep].
//!
//! _*backtrace*_, is library feature that captures backtrace at the point where error
//! is detected by this libarary. Additionally if `logging` is enabled backtace is
//! logged as per the configured log-backend.
//!
//! _*fuzzy*_, is used only by the test infrastructure. Typical application won't have
//! a need for this. Enabling this will provide `arbitrary::Arbitrary` implementation
//! for several types defined in this library.
//!
//! By default `broker` and `client` features are enabled.
//!
//! #### Rust unstable features
//!
//! * [backtrace_frames][us1]
//! * [backtrace][us2]
//! * [error_iter][us3]
//! * [map_first_last][us4]
//! * [result_flattening][us5]
//!
//! #### Binary artifacts
//!
//! * _*mymqd*_, daemon program to start the server and manage the mymq deployment.
//!
//! [dep]: https://doc.rust-lang.org/cargo/reference/features.html#dependency-features
//! [us1]: https://doc.rust-lang.org/beta/unstable-book/library-features/backtrace.html
//! [us2]: https://doc.rust-lang.org/beta/unstable-book/library-features/backtrace-frames.html
//! [us3]: https://doc.rust-lang.org/beta/unstable-book/library-features/error-iter.html
//! [us4]: https://doc.rust-lang.org/beta/unstable-book/library-features/map-first-last.htm
//! [us5]: https://doc.rust-lang.org/beta/unstable-book/library-features/result-flattening.html
//! [issue-4]: https://github.com/prataprc/mymq/issues/4

// TODO: review all err!() calls and tally them with MQTT spec.
// TODO: validate()? calls must be wired into all Packetize::{encode, decode}
//       implementation

#![feature(backtrace_frames)]
#![feature(backtrace)]
#![feature(error_iter)]
#![feature(map_first_last)]
#![feature(result_flattening)]

/// Type alias for Result returned by functions and methods defined in this package.
pub type Result<T> = std::result::Result<T, Error>;

/// Trait, to serialize data, implemented by types that participate in protocol framing.
///
/// Shall return one of the following [ErrorKind] `ProtocolError`, `MalformedPacket`.
pub trait Packetize: Sized {
    /// Deserialize bytes and construct a packet or packet's field. Upon error, it is
    /// expected that the stream is left at meaningful boundry to re-detect the error.
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)>;

    /// Serialize value into bytes.
    fn encode(&self) -> Result<Blob>;
}

/// Trait implemented by [TopicName] and [TopicFilter].
///
/// We take the inspiration from MQTT specification where TopicName and TopicFilter are
/// defined in path-like format.
pub trait IterTopicPath<'a> {
    type Iter: Iterator<Item = &'a str> + Clone;

    fn iter_topic_path(&'a self) -> Self::Iter;

    fn is_dollar_topic(&self) -> bool;

    fn is_begin_wild_card(&self) -> bool;
}

/// Trait that returns Jsonified strings.
pub trait ToJson {
    /// Implementing type shall return configuration as JSON string.
    fn to_config_json(&self) -> String;

    /// Implementing type shall return statistics as JSON string.
    fn to_stats_json(&self) -> String;
}

#[macro_use]
mod error;
#[macro_use]
mod config;
mod protocol;
mod queue;
mod timer;
mod types;

pub use error::{Error, ErrorKind, ReasonCode};
pub use protocol::{Protocol, QPacket, Socket};
pub use queue::{new_packet_queue, PacketRx, PacketTx, QueueStatus};
pub use timer::Timer;
pub use types::{Blob, ClientID, PacketID, TopicFilter, TopicName, VarU32};

#[macro_use]
pub mod v5;
pub mod util;

#[cfg(feature = "broker")]
pub mod broker;

#[cfg(feature = "netw")]
pub mod netw;
