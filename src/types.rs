use std::ops::{Deref, DerefMut};

use crate::util::{self, advance};
use crate::Packetize;
use crate::{Error, ErrorKind, ReasonCode, Result};

// TODO: there is a protocol limit for packet size, provide a way for applications
//       to set a limit for packet size.

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

#[derive(Debug, Clone, PartialEq)]
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

impl TryFrom<String> for TopicName {
    type Error = Error;

    fn try_from(val: String) -> Result<TopicName> {
        if val.len() == 0 {
            err!(ProtocolError, code: InvalidTopicName, "empty topic-name {:?}", val)
        } else if val.chars().any(|c| c == '#' || c == '+') {
            err!(ProtocolError, code: InvalidTopicName, "has wildcards {:?}", val)
        } else {
            Ok(TopicName(val))
        }
    }
}

impl Packetize for TopicName {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        let (val, n) = String::decode(stream)?;
        let val: TopicName = val.try_into()?;
        Ok((val, n))
    }

    fn encode(&self) -> Result<Blob> {
        let val: TopicName = self.0.clone().try_into()?;
        val.encode()
    }
}

impl TopicName {
    pub fn as_levels(&self) -> Vec<&str> {
        self.split('/').collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
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

impl TryFrom<String> for TopicFilter {
    type Error = Error;

    fn try_from(val: String) -> Result<TopicFilter> {
        if val.len() == 0 {
            err!(ProtocolError, code: InvalidTopicName, "empty topic-name {:?}", val)
        } else {
            Ok(TopicFilter(val))
        }
    }
}

impl TopicFilter {
    pub fn as_levels(&self) -> Vec<&str> {
        self.split('/').collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
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

/// Uses continuation bit at position 7 to continue reading next byte to frame 'u32'.
/// i/p stream: 0b0www_wwww 0b1zzz_zzzz 0b1yyy_yyyy 0b1xxx_xxxx, low-byte to high-byte
/// o/p u32:    0bwww_wwww_zzz_zzzz_yyy_yyyy_xxx_xxxx
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
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        use std::{cmp, mem};

        let n = cmp::min(stream.len(), mem::size_of::<u32>());
        let mut out = 0_u32;
        for i in 0..n {
            let val = ((stream[i] as u32) & 0x7f) << (7 * (i as u32));
            out += val;
            if stream[i] < 0x80 {
                return Ok((VarU32(out), i + 1));
            }
        }

        err!(MalformedPacket, code: MalformedPacket, "VarU32::read")
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
            val => err!(InvalidInput, desc: "VarU32:write({})", val)?,
        };

        Ok(Blob::Small { data, size })
    }
}

impl VarU32 {
    pub const MAX: VarU32 = VarU32(268_435_455);
}

pub type UserProperty = (String, String);

impl Packetize for UserProperty {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
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
            data.extend_from_slice(self.0.encode()?.as_ref());
            data.extend_from_slice(self.1.encode()?.as_ref());
            Ok(Blob::Large { data })
        }
    }
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
            Ok(s) if s.chars().any(util::is_invalid_utf8_code_point) => {
                err!(MalformedPacket, code: MalformedPacket, "invalid utf8 string")
            }
            Ok(s) => Ok((s.to_string(), 2 + len)),
            Err(err) => {
                err!(MalformedPacket, code: MalformedPacket, cause: err, "String::decode")
            }
        }
    }

    fn encode(&self) -> Result<Blob> {
        if self.chars().any(util::is_invalid_utf8_code_point) {
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
                UnsupportedProtocolVersion,
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
