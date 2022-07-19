#[cfg(any(feature = "fuzzy", test))]
use arbitrary::Arbitrary;

use std::ops::{Deref, DerefMut};

use crate::util::{self, advance};
use crate::{Error, ErrorKind, ReasonCode, Result};
use crate::{IterTopicPath, Packetize};

/// Enumeration of different MQTT Protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
pub enum MqttProtocol {
    V4 = 4,
    V5 = 5,
}

impl TryFrom<u8> for MqttProtocol {
    type Error = Error;

    fn try_from(val: u8) -> Result<MqttProtocol> {
        match val {
            4 => Ok(MqttProtocol::V4),
            5 => Ok(MqttProtocol::V5),
            val => err!(
                MalformedPacket,
                code: UnsupportedProtocolVersion,
                "found: {:?}",
                val
            )?,
        }
    }
}

impl From<MqttProtocol> for u8 {
    fn from(val: MqttProtocol) -> u8 {
        match val {
            MqttProtocol::V4 => 4,
            MqttProtocol::V5 => 5,
        }
    }
}

/// Type is associated with [Packetize] trait and optimizes on the returned byte-blob.
///
/// Small variant stores the bytes in stack.
/// Large variant stores the bytes in heap.
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

/// Type client-id implements a unique ID defined by MQTT specification.
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct ClientID(pub String);

impl Deref for ClientID {
    type Target = String;

    fn deref(&self) -> &String {
        &self.0
    }
}

impl DerefMut for ClientID {
    fn deref_mut(&mut self) -> &mut String {
        &mut self.0
    }
}

impl ClientID {
    pub fn new_uuid_v4() -> ClientID {
        ClientID(uuid::Uuid::new_v4().to_string())
    }

    pub fn from_connect(client_id: &ClientID) -> ClientID {
        match client_id.len() {
            0 => Self::new_uuid_v4(),
            _ => client_id.clone(),
        }
    }
}

/// Type implement topic-name defined by MQTT specification.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopicName(String);

impl Deref for TopicName {
    type Target = String;

    fn deref(&self) -> &String {
        &self.0
    }
}

impl DerefMut for TopicName {
    fn deref_mut(&mut self) -> &mut String {
        &mut self.0
    }
}

impl From<String> for TopicName {
    fn from(val: String) -> TopicName {
        TopicName(val)
    }
}

impl Packetize for TopicName {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (val, n) = String::decode(stream)?;
        let val = TopicName::from(val);

        val.validate()?;
        Ok((val, n))
    }

    fn encode(&self) -> Result<Blob> {
        self.validate()?;
        self.0.encode()
    }
}

impl<'a> IterTopicPath<'a> for TopicName {
    type Iter = std::str::Split<'a, char>;

    fn iter_topic_path(&'a self) -> Self::Iter {
        self.split('/')
    }
}

impl TopicName {
    fn validate(&self) -> Result<()> {
        todo!()
    }
}

/// Type implement topic-filter defined by MQTT specification.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopicFilter(String);

impl Deref for TopicFilter {
    type Target = String;

    fn deref(&self) -> &String {
        &self.0
    }
}

impl DerefMut for TopicFilter {
    fn deref_mut(&mut self) -> &mut String {
        &mut self.0
    }
}

impl From<String> for TopicFilter {
    fn from(val: String) -> TopicFilter {
        TopicFilter(val)
    }
}

impl Packetize for TopicFilter {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (val, n) = String::decode(stream)?;
        let val = TopicFilter::from(val);

        val.validate()?;
        Ok((val, n))
    }

    fn encode(&self) -> Result<Blob> {
        self.validate()?;
        self.0.encode()
    }
}

impl<'a> IterTopicPath<'a> for TopicFilter {
    type Iter = std::str::Split<'a, char>;

    fn iter_topic_path(&'a self) -> Self::Iter {
        self.split('/')
    }
}

impl TopicFilter {
    fn validate(&self) -> Result<()> {
        todo!()
    }
}

/// Type implement variable-length unsigned 32-bit integer.
///
/// Uses continuation bit at position 7 to continue reading next byte to frame 'u32'.
///
/// ```txt
/// i/p stream: 0b0www_wwww 0b1zzz_zzzz 0b1yyy_yyyy 0b1xxx_xxxx, low-byte to high-byte
/// o/p u32   : 0bwww_wwww_zzz_zzzz_yyy_yyyy_xxx_xxxx
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
pub struct VarU32(pub u32);

impl Deref for VarU32 {
    type Target = u32;

    fn deref(&self) -> &u32 {
        &self.0
    }
}

impl Packetize for VarU32 {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        use std::{cmp, mem};

        let stream: &[u8] = stream.as_ref();

        let n = cmp::min(stream.len(), mem::size_of::<u32>());
        let mut out = 0_u32;
        for i in 0..n {
            let val = ((stream[i] as u32) & 0x7f) << (7 * (i as u32));
            out += val;
            if stream[i] < 0x80 {
                return Ok((VarU32(out), i + 1));
            }
        }

        err!(MalformedPacket, code: MalformedPacket, "VarU32::decode")
    }

    fn encode(&self) -> Result<Blob> {
        let mut data = [0_u8; 32];
        let size = match *(&self.0) {
            val if val < 128 => {
                data[0] = (val & 0x7f_u32) as u8;
                1
            }
            val if val < 16_384 => {
                data[0] = ((val & 0x7f_u32) as u8) | 0x80;
                data[1] = ((val >> 7) & 0x7f_u32) as u8;
                2
            }
            val if val < 2_097_152 => {
                data[0] = ((val & 0x7f_u32) as u8) | 0x80;
                data[1] = (((val >> 7) & 0x7f_u32) as u8) | 0x80;
                data[2] = ((val >> 14) & 0x7f_u32) as u8;
                3
            }
            val if val <= *VarU32::MAX => {
                data[0] = ((val & 0x7f_u32) as u8) | 0x80;
                data[1] = (((val >> 7) & 0x7f_u32) as u8) | 0x80;
                data[2] = (((val >> 14) & 0x7f_u32) as u8) | 0x80;
                data[3] = ((val >> 21) & 0x7f_u32) as u8;
                4
            }
            val => err!(ProtocolError, desc: "VarU32::encode({})", val)?,
        };

        Ok(Blob::Small { data, size })
    }
}

impl VarU32 {
    pub const MAX: VarU32 = VarU32(268_435_455);
}

/// Type alias for MQTT User-Property.
pub type UserProperty = (String, String);

impl Packetize for UserProperty {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (key, m) = String::decode(stream)?;
        let (val, n) = String::decode(advance(stream, m)?)?;
        Ok(((key, val), (m + n)))
    }

    fn encode(&self) -> Result<Blob> {
        let key_blob = self.0.encode()?;
        let val_blob = self.1.encode()?;
        let m = key_blob.as_ref().len();
        let n = val_blob.as_ref().len();

        if (m + n) < 32 {
            let mut data = [0_u8; 32];
            data[..m].copy_from_slice(key_blob.as_ref());
            data[m..m + n].copy_from_slice(val_blob.as_ref());
            Ok(Blob::Small { data, size: m + n })
        } else {
            let mut data = Vec::with_capacity(64);
            data.extend_from_slice(key_blob.as_ref());
            data.extend_from_slice(val_blob.as_ref());
            Ok(Blob::Large { data })
        }
    }
}

impl Packetize for u8 {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

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
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

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
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

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
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (len, _) = u16::decode(stream)?;
        let len = usize::from(len);
        if len + 2 > stream.len() {
            return err!(InsufficientBytes, code: MalformedPacket, "String::decode");
        }

        match std::str::from_utf8(&stream[2..2 + len]) {
            Ok(s) if s.chars().any(util::is_invalid_utf8_code_point) => {
                err!(
                    MalformedPacket,
                    code: MalformedPacket,
                    "String::encode invalid utf8 string"
                )
            }
            Ok(s) => Ok((s.to_string(), 2 + len)),
            Err(err) => {
                err!(MalformedPacket, code: MalformedPacket, cause: err, "String::decode")
            }
        }
    }

    fn encode(&self) -> Result<Blob> {
        if self.chars().any(util::is_invalid_utf8_code_point) {
            return err!(ProtocolError, desc: "String::encode invalid utf8 string");
        }

        match self.len() {
            n if n > (u16::MAX as usize) => {
                err!(ProtocolError, desc: "String::encode too large {:?}", n)
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
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (len, _) = u16::decode(stream)?;
        let len = usize::from(len);
        if len + 2 > stream.len() {
            return err!(InsufficientBytes, code: MalformedPacket, "Vector::decode");
        }
        Ok((stream[2..2 + len].to_vec(), 2 + len))
    }

    fn encode(&self) -> Result<Blob> {
        match self.len() {
            n if n > (u16::MAX as usize) => {
                err!(ProtocolError, desc: "Vector::encode({})", n)
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
