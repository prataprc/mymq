//! Package implement MQTT protocol-framing for both client and server.

#![feature(backtrace)]

#[macro_use]
mod error;
pub mod v5;

pub use error::{Error, ErrorKind, ReasonCode};

/// Result returned by this methods and functions defined in this package.
pub type Result<T> = std::result::Result<T, Error>;

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
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
}

/// Trait for protocol framing, data-encoding and decoding.
pub trait Packetize: Sized {
    /// Deserialize bytes and construct a packet or packet's field.
    /// Upon error, it is expected that the stream is left at meaningful
    /// boundry to re-detect the error.
    fn decode(stream: &[u8]) -> Result<(Self, usize)>;

    /// Same as read, but no checks are done, assumes that stream is well formed.
    fn decode_unchecked(stream: &[u8]) -> (Self, usize);

    /// Serialize value into bytes, for small frames.
    fn encode(&self) -> Result<Blob>;

    /// Serialize value into bytes, for large payloads.
    fn into_blob(self) -> Result<Blob>;
}

impl Packetize for u8 {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        match stream.len() {
            n if n >= 1 => Ok((stream[0], 1)),
            _ => err!(InsufficientBytes, code: MalformedPacket, "u8::decode()"),
        }
    }

    fn decode_unchecked(stream: &[u8]) -> (Self, usize) {
        (stream[0], 1)
    }

    fn encode(&self) -> Result<Blob> {
        let mut blob = Blob::Small { data: Default::default(), size: 1 };
        match &mut blob {
            Blob::Small { data, .. } => data[0] = *self,
            _ => (),
        }

        Ok(blob)
    }

    fn into_blob(self) -> Result<Blob> {
        self.encode()
    }
}

impl Packetize for u16 {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        match stream.len() {
            n if n >= 2 => Ok((u16::from_be_bytes(stream[..2].try_into().unwrap()), 2)),
            _ => err!(InsufficientBytes, code: MalformedPacket, "u16::decode()"),
        }
    }

    fn decode_unchecked(stream: &[u8]) -> (Self, usize) {
        (u16::from_be_bytes(stream[..2].try_into().unwrap()), 2)
    }

    fn encode(&self) -> Result<Blob> {
        let mut blob = Blob::Small { data: Default::default(), size: 2 };
        match &mut blob {
            Blob::Small { data, .. } => data[..2].copy_from_slice(&self.to_be_bytes()),
            _ => (),
        }

        Ok(blob)
    }

    fn into_blob(self) -> Result<Blob> {
        self.encode()
    }
}

impl Packetize for u32 {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        match stream.len() {
            n if n >= 4 => Ok((u32::from_be_bytes(stream[..4].try_into().unwrap()), 4)),
            _ => err!(InsufficientBytes, code: MalformedPacket, "u32::decode()"),
        }
    }

    fn decode_unchecked(stream: &[u8]) -> (Self, usize) {
        (u32::from_be_bytes(stream[..4].try_into().unwrap()), 4)
    }

    fn encode(&self) -> Result<Blob> {
        let mut blob = Blob::Small { data: Default::default(), size: 4 };
        match &mut blob {
            Blob::Small { data, .. } => data[..4].copy_from_slice(&self.to_be_bytes()),
            _ => (),
        }

        Ok(blob)
    }

    fn into_blob(self) -> Result<Blob> {
        self.encode()
    }
}

impl Packetize for String {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        use std::str::from_utf8;

        let (len, _) = u16::decode(stream)?;
        let len = usize::from(len);
        if len + 2 > stream.len() {
            return err!(InsufficientBytes, code: MalformedPacket, "String::read");
        }

        match from_utf8(&stream[2..2 + len]) {
            Ok(s) => Ok((s.to_string(), 2 + len)),
            Err(err) => {
                err!(MalformedPacket, code: MalformedPacket, cause: err, "String::read")
            }
        }
    }

    fn decode_unchecked(stream: &[u8]) -> (Self, usize) {
        use std::str::from_utf8_unchecked;

        let (len, _) = u16::decode_unchecked(stream);
        let len = usize::from(len);
        let s = unsafe { from_utf8_unchecked(&stream[2..2 + len]).to_string() };
        (s, 2 + len)
    }

    fn encode(&self) -> Result<Blob> {
        match self.len() {
            n if n > (u16::MAX as usize) => {
                err!(InvalidInput, desc: "String::write({})", n)
            }
            n => {
                let mut data = Vec::with_capacity(2 + n);
                data.extend_from_slice(&(n as u16).to_be_bytes());
                data.extend_from_slice(self.as_bytes());
                Ok(Blob::Large { data })
            }
        }
    }

    fn into_blob(self) -> Result<Blob> {
        self.encode()
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

    fn decode_unchecked(stream: &[u8]) -> (Self, usize) {
        let (len, _) = u16::decode_unchecked(stream);
        let len = usize::from(len);
        (stream[2..2 + len].to_vec(), 2 + len)
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

    fn into_blob(self) -> Result<Blob> {
        self.encode()
    }
}

#[cfg(test)]
#[path = "lib_test.rs"]
mod lib_test;
