#[cfg(any(feature = "fuzzy", test))]
use arbitrary::{Arbitrary, Error as ArbitraryError, Unstructured};
#[cfg(any(feature = "fuzzy", test))]
use std::result;

use std::ops::{Deref, DerefMut};

use crate::util::{self, advance};
use crate::{Error, ErrorKind, ReasonCode, Result};
use crate::{IterTopicPath, Packetize};

// TODO: Section.4.7
//       An Application Message is sent to each Client Subscription whose Topic
//       Filter matches the Topic Name attached to an Application Message. The topic
//       resource MAY be either predefined in the Server by an administrator or it MAY
//       be dynamically created by the Server when it receives the first subscription
//       or an Application Message with that Topic Name. The Server MAY also use a
//       security component to authorize particular actions on the topic resource for
//       a given Client.

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

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for ClientID {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let client_id = match uns.arbitrary::<u8>()? % 2 {
            0 => ClientID::new_uuid_v4(),
            1 => ClientID("".to_string()),
            _ => unreachable!(),
        };

        Ok(client_id)
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

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for TopicName {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let level_choice: Vec<String> =
            vec!["", "$", "$SYS"].into_iter().map(|s| s.to_string()).collect();
        let string_choice: Vec<String> =
            vec!["", "a", "ab", "abc"].into_iter().map(|s| s.to_string()).collect();

        let c = uns.arbitrary::<u8>()?;
        let levels = match c {
            00..=09 => vec![uns.choose(&string_choice)?.to_string()],
            _ => {
                let mut levels = vec![];
                for _ in 0..((c % 10) + 1) {
                    let level = match uns.arbitrary::<u8>()? {
                        000..=200 => uns.choose(&string_choice)?.to_string(),
                        201..=255 => uns.choose(&level_choice)?.clone(),
                    };
                    levels.push(level);
                }
                levels
            }
        };

        let mut s = String::from_iter(
            levels.join("/").chars().filter(|ch| !matches!(ch, '#' | '+' | '\u{0}')),
        );

        s = loop {
            if s.len() > 0 {
                break s;
            }
            s = uns.choose(&string_choice)?.to_string();
        };

        Ok(s.into())
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
    pub fn validate(&self) -> Result<()> {
        // All Topic Names and Topic Filters MUST be at least one character long.
        if self.0.len() == 0 {
            err!(MalformedPacket, code: MalformedPacket, "ZERO length TopicName")?;
        }

        if self.0.chars().any(|ch| matches!(ch, '#' | '+' | '\u{0}')) {
            err!(MalformedPacket, code: MalformedPacket, "")?;
        }

        Ok(())
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

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for TopicFilter {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let level_choice: Vec<String> =
            vec!["", "$", "$SYS", "#", "+"].into_iter().map(|s| s.to_string()).collect();
        let string_choice: Vec<String> =
            vec!["", "a", "ab", "abc"].into_iter().map(|s| s.to_string()).collect();

        let c = uns.arbitrary::<u8>()?;
        let levels = match c {
            00..=09 => vec![uns.choose(&level_choice)?.to_string()],
            10..=20 => vec![uns.choose(&string_choice)?.to_string()],
            _ => {
                let mut levels = vec![];
                for _ in 0..((c % 10) + 1) {
                    let level = match uns.arbitrary::<u8>()? {
                        000..=200 => uns.choose(&string_choice)?.to_string(),
                        201..=255 => uns.choose(&level_choice)?.clone(),
                    };
                    levels.push(level);
                }
                match levels.iter().enumerate().skip_while(|(_, s)| s != &"#").next() {
                    Some((i, _)) => levels[..i + 1].to_vec(),
                    None => levels,
                }
            }
        };

        let mut s = String::from_iter(
            levels.join("/").chars().filter(|ch| !matches!(ch, '\u{0}')),
        );

        s = loop {
            if s.len() > 0 {
                break s;
            }
            s = uns.choose(&string_choice)?.to_string();
        };

        Ok(s.into())
    }
}

impl TopicFilter {
    pub fn validate(&self) -> Result<()> {
        // All Topic Names and Topic Filters MUST be at least one character long.
        if self.0.len() == 0 {
            err!(MalformedPacket, code: MalformedPacket, "ZERO length TopicFilter")?;
        } else if self.0.chars().any(|ch| matches!(ch, '\u{0}')) {
            err!(MalformedPacket, code: MalformedPacket, "")?;
        }

        let levels = self.iter_topic_path();

        let mut iter = levels.clone().filter(|l| l.len() > 1);
        if iter.any(|l| l.chars().any(|c| matches!(c, '#' | '+'))) {
            err!(MalformedPacket, code: MalformedPacket, "wildcard mixed with chars")?;
        }

        let mut iter = levels.clone().skip_while(|l| l != &"#");
        iter.next();
        if let Some(_) = iter.next() {
            err!(MalformedPacket, code: MalformedPacket, "chars after # wildcard")?;
        }

        Ok(())
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
pub struct VarU32(pub u32);

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for VarU32 {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let val: u32 = uns.arbitrary()?;
        Ok(VarU32(val % *VarU32::MAX))
    }
}

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

#[cfg(any(feature = "fuzzy", test))]
pub fn valid_user_props<'a>(
    uns: &mut Unstructured<'a>,
    n: usize,
) -> result::Result<Vec<UserProperty>, ArbitraryError> {
    let mut props = vec![];
    for _ in 0..n {
        let keys: Vec<String> =
            vec!["", "key"].into_iter().map(|s| s.to_string()).collect();
        let vals: Vec<String> =
            vec!["", "val"].into_iter().map(|s| s.to_string()).collect();

        let key: String = uns.choose(&keys)?.to_string();
        let val: String = uns.choose(&vals)?.to_string();
        props.push((key, val))
    }

    Ok(props)
}

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
            err!(InsufficientBytes, code: MalformedPacket, "String::decode")?;
        }

        match std::str::from_utf8(&stream[2..2 + len]) {
            Ok(s) if !s.chars().all(util::is_valid_utf8_code_point) => {
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
        if !self.chars().all(util::is_valid_utf8_code_point) {
            err!(ProtocolError, desc: "String::encode invalid utf8 string")?;
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
            err!(InsufficientBytes, code: MalformedPacket, "Vector::decode")?;
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
