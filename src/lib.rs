//! Package implement MQTT protocol-framing for both client and server.

#![feature(backtrace)]
#![feature(error_iter)]

#[cfg(any(feature = "fuzzy", test))]
use arbitrary::Arbitrary;

#[macro_use]
mod error;
mod clientid;
mod topic;

#[macro_use]
pub mod v5;

#[cfg(any(feature = "fuzzy", test))]
pub mod fuzzy;

pub use clientid::ClientID;
pub use error::{Error, ErrorKind, ReasonCode};
pub use topic::{TopicFilter, TopicName};

// TODO: restrict packet size to maximum allowed for each session or use
//       protocol-limitation

/// Result returned by this methods and functions defined in this package.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Blob {
    Small { data: [u8; 32], size: usize },
    Large { data: Vec<u8> },
}

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        match self {
            Blob::Small { data, size } => &data[..*size],
            Blob::Large { data } => &data,
        }
    }
}

impl Blob {
    #[cfg(test)]
    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
}

/// Trait for protocol framing, data-encoding and decoding.
pub trait Packetize: Sized {
    /// Deserialize bytes and construct a packet or packet's field.
    /// Upon error, it is expected that the stream is left at meaningful
    /// boundry to re-detect the error.
    fn decode(stream: &[u8]) -> Result<(Self, usize)>;

    /// Serialize value into bytes, for small frames.
    fn encode(&self) -> Result<Blob>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
pub enum MqttProtocol {
    V4 = 4,
    V5 = 5,
}

impl Packetize for u8 {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        match stream.len() {
            n if n >= 1 => Ok((stream[0], 1)),
            _ => err!(InsufficientBytes, code: MalformedPacket, "u8::decode()"),
        }
    }

    fn encode(&self) -> Result<Blob> {
        let mut blob = Blob::Small { data: Default::default(), size: 1 };
        match &mut blob {
            Blob::Small { data, .. } => data[0] = *self,
            _ => (),
        }

        Ok(blob)
    }
}

impl Packetize for u16 {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        match stream.len() {
            n if n >= 2 => Ok((u16::from_be_bytes(stream[..2].try_into().unwrap()), 2)),
            _ => err!(InsufficientBytes, code: MalformedPacket, "u16::decode()"),
        }
    }

    fn encode(&self) -> Result<Blob> {
        let mut blob = Blob::Small { data: Default::default(), size: 2 };
        match &mut blob {
            Blob::Small { data, .. } => data[..2].copy_from_slice(&self.to_be_bytes()),
            _ => (),
        }

        Ok(blob)
    }
}

impl Packetize for u32 {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        match stream.len() {
            n if n >= 4 => Ok((u32::from_be_bytes(stream[..4].try_into().unwrap()), 4)),
            _ => err!(InsufficientBytes, code: MalformedPacket, "u32::decode()"),
        }
    }

    fn encode(&self) -> Result<Blob> {
        let mut blob = Blob::Small { data: Default::default(), size: 4 };
        match &mut blob {
            Blob::Small { data, .. } => data[..4].copy_from_slice(&self.to_be_bytes()),
            _ => (),
        }

        Ok(blob)
    }
}

impl Packetize for String {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        use std::str::from_utf8;

        let (len, _) = u16::decode(stream)?;
        let len = usize::from(len);
        if len + 2 > stream.len() {
            return err!(InsufficientBytes, code: MalformedPacket, "String::decode");
        }

        match from_utf8(&stream[2..2 + len]) {
            Ok(s) if s.chars().any(is_invalid_utf8_code_point) => {
                err!(MalformedPacket, code: MalformedPacket, "invalid utf8 string")
            }
            Ok(s) => Ok((s.to_string(), 2 + len)),
            Err(err) => {
                err!(MalformedPacket, code: MalformedPacket, cause: err, "String::decode")
            }
        }
    }

    fn encode(&self) -> Result<Blob> {
        if self.chars().any(is_invalid_utf8_code_point) {
            return err!(InvalidInput, desc: "invalid utf8 string");
        }

        match self.len() {
            n if n > (u16::MAX as usize) => {
                err!(InvalidInput, desc: "String too large {:?}", n)
            }
            n if n < 30 => {
                let mut data = [0_u8; 32];
                data[0..2].copy_from_slice(&(n as u16).to_be_bytes());
                data[2..2 + n].copy_from_slice(self.as_bytes());
                Ok(Blob::Small { data, size: 2 + n })
            }
            n => {
                let mut data = Vec::with_capacity(2 + n);
                data.extend_from_slice(&(n as u16).to_be_bytes());
                data.extend_from_slice(self.as_bytes());
                Ok(Blob::Large { data })
            }
        }
    }
}

impl Packetize for Vec<u8> {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        let (len, _) = u16::decode(stream)?;
        let len = usize::from(len);
        if len + 2 > stream.len() {
            return err!(InsufficientBytes, code: MalformedPacket, "Vector::read");
        }
        Ok((stream[2..2 + len].to_vec(), 2 + len))
    }

    fn encode(&self) -> Result<Blob> {
        match self.len() {
            n if n > (u16::MAX as usize) => {
                err!(InvalidInput, desc: "Vector::write({})", n)
            }
            n => {
                let mut data = Vec::with_capacity(2 + n);
                data.extend_from_slice(&(n as u16).to_be_bytes());
                data.extend_from_slice(self.as_ref());
                Ok(Blob::Large { data })
            }
        }
    }
}

pub(crate) fn is_invalid_utf8_code_point(ch: char) -> bool {
    let c = ch as u32;
    (c >= 0xD800 && c <= 0xDFFF) || c <= 0x1F || (c >= 0x7F && c <= 0x9F)
}

#[cfg(test)]
#[path = "lib_test.rs"]
mod lib_test;
