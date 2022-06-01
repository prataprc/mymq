#[cfg(any(feature = "fuzzy", test))]
use arbitrary::Arbitrary;

use std::ops::Deref;

use crate::{Blob, Error, ErrorKind, Packetize, ReasonCode, Result};

#[cfg(any(feature = "fuzzy", test))]
pub mod fuzzy;

// TODO: FixedHeader.remaining_len must be validated with
//       ConnectProperties.maximum_pkt_size.

/// All that is MQTT
#[derive(Debug, Clone, PartialEq)]
pub enum Packet {
    //Connect(Connect),
//ConnAck(ConnAck),
//Publish(Publish),
//PubAck(PubAck),
//PubRec(PubRec),
//PubRel(PubRel),
//PubComp(PubComp),
//Subscribe(Subscribe),
//SubAck(SubAck),
//Unsubscribe(Unsubscribe),
//UnsubAck(UnsubAck),
//PingReq,
//PingResp,
//Disconnect(Disconnect),
}

/// MQTT packet type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
pub enum PacketType {
    Connect = 1,
    ConnAck = 2,
    Publish = 3,
    PubAck = 4,
    PubRec = 5,
    PubRel = 6,
    PubComp = 7,
    Subscribe = 8,
    SubAck = 9,
    Unsubscribe = 10,
    UnsubAck = 11,
    PingReq = 12,
    PingResp = 13,
    Disconnect = 14,
    Auth = 15,
}

impl TryFrom<u8> for PacketType {
    type Error = Error;

    fn try_from(val: u8) -> Result<PacketType> {
        let val = match val {
            1 => PacketType::Connect,
            2 => PacketType::ConnAck,
            3 => PacketType::Publish,
            4 => PacketType::PubAck,
            5 => PacketType::PubRec,
            6 => PacketType::PubRel,
            7 => PacketType::PubComp,
            8 => PacketType::Subscribe,
            9 => PacketType::SubAck,
            10 => PacketType::Unsubscribe,
            11 => PacketType::UnsubAck,
            12 => PacketType::PingReq,
            13 => PacketType::PingResp,
            14 => PacketType::Disconnect,
            15 => PacketType::Auth,
            _ => err!(MalformedPacket, code: MalformedPacket, "forbidden packet")?,
        };

        Ok(val)
    }
}

impl From<PacketType> for u8 {
    fn from(val: PacketType) -> u8 {
        match val {
            PacketType::Connect => 1,
            PacketType::ConnAck => 2,
            PacketType::Publish => 3,
            PacketType::PubAck => 4,
            PacketType::PubRec => 5,
            PacketType::PubRel => 6,
            PacketType::PubComp => 7,
            PacketType::Subscribe => 8,
            PacketType::SubAck => 9,
            PacketType::Unsubscribe => 10,
            PacketType::UnsubAck => 11,
            PacketType::PingReq => 12,
            PacketType::PingResp => 13,
            PacketType::Disconnect => 14,
            PacketType::Auth => 15,
        }
    }
}

/// Protocol type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
pub enum Protocol {
    V4 = 4,
    V5 = 5,
}

/// Quality of service
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl TryFrom<u8> for QoS {
    type Error = Error;

    fn try_from(val: u8) -> Result<QoS> {
        let val = match val {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => err!(MalformedPacket, code: InvalidQoS, "forbidden packet")?,
        };

        Ok(val)
    }
}

impl From<QoS> for u8 {
    fn from(val: QoS) -> u8 {
        match val {
            QoS::AtMostOnce => 0,
            QoS::AtLeastOnce => 1,
            QoS::ExactlyOnce => 2,
        }
    }
}

/// Uses continuation bit at position 7 to continue reading next byte to frame 'u32'.
/// i/p stream: 0b0www_wwww 0b1zzz_zzzz 0b1yyy_yyyy 0b1xxx_xxxx, low-byte to high-byte
/// o/p u32:    0bwww_wwww_zzz_zzzz_yyy_yyyy_xxx_xxxx
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
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

    fn decode_unchecked(stream: &[u8]) -> (Self, usize) {
        Self::decode(stream).unwrap()
    }

    fn encode(&self) -> Result<Blob> {
        let mut data = [0_u8; 32];
        let size = match *(&self.0) {
            val if val < 128 => {
                data[0] = (val & 0x7f_u32) as u8;
                1
            }
            val if val < 16_384 => {
                data[0] = (val & 0x7f_u32) as u8;
                data[1] = ((val >> 7) & 0x7f_u32) as u8;
                2
            }
            val if val < 2_097_152 => {
                data[0] = (val & 0x7f_u32) as u8;
                data[1] = ((val >> 7) & 0x7f_u32) as u8;
                data[2] = ((val >> 14) & 0x7f_u32) as u8;
                3
            }
            val if val <= *VarU32::MAX => {
                data[0] = (val & 0x7f_u32) as u8;
                data[0] = ((val >> 7) & 0x7f_u32) as u8;
                data[0] = ((val >> 14) & 0x7f_u32) as u8;
                data[0] = ((val >> 21) & 0x7f_u32) as u8;
                4
            }
            val => err!(InvalidInput, desc: "VarU32:write({})", val)?,
        };

        Ok(Blob::Small { data, size })
    }

    fn into_blob(self) -> Result<Blob> {
        self.encode()
    }
}

impl VarU32 {
    pub const MAX: VarU32 = VarU32(268_435_455);
}

/// Packet type from a byte
///
/// ```ignore
///          7                          3                          0
///          +--------------------------+--------------------------+
/// byte 1   | MQTT Control Packet Type | Flags for each type      |
///          +--------------------------+--------------------------+
///          |         Remaining Bytes Len  (1/2/3/4 bytes)        |
///          +-----------------------------------------------------+
///
/// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Figure_2.2_-
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub struct FixedHeader {
    /// First byte of the stream. Used to identify packet types and several flags
    byte1: u8,
    /// Remaining length of the packet. Doesn't include fixed header bytes
    /// Represents variable header + payload size
    remaining_len: VarU32,
}

impl Packetize for FixedHeader {
    fn decode(stream: &[u8]) -> Result<(FixedHeader, usize)> {
        let (byte1, m) = u8::decode(stream)?;
        let (remaining_len, n) = VarU32::decode(stream)?;
        Ok((FixedHeader { byte1, remaining_len }, m + n))
    }

    /// Same as read, but no checks are done, assumes that stream is well formed.
    fn decode_unchecked(stream: &[u8]) -> (FixedHeader, usize) {
        let (byte1, m) = u8::decode_unchecked(stream);
        let (remaining_len, n) = VarU32::decode_unchecked(stream);
        (FixedHeader { byte1, remaining_len }, m + n)
    }

    /// Serialize value into bytes, for small frames.
    fn encode(&self) -> Result<Blob> {
        let byte1 = self.byte1.encode()?;
        let remaining_len = self.remaining_len.encode()?;
        let (m, n) = (byte1.as_ref().len(), remaining_len.as_ref().len());

        let mut data = [0_u8; 32];
        data[..m].copy_from_slice(byte1.as_ref());
        data[m..m + n].copy_from_slice(remaining_len.as_ref());

        Ok(Blob::Small { data, size: m + n })
    }

    /// Serialize value into bytes, for large payloads.
    fn into_blob(self) -> Result<Blob> {
        self.encode()
    }
}

impl FixedHeader {
    pub fn new(pkt_type: PacketType, remaining_len: VarU32) -> Result<FixedHeader> {
        if remaining_len > VarU32::MAX {
            err!(PayloadTooLong, desc: "payload too long for MQTT packets")?
        }
        let byte1 = u8::from(pkt_type) << 4;
        Ok(FixedHeader { byte1, remaining_len })
    }

    pub fn new_pubish(
        retain: bool,
        qos: QoS,
        dup: bool,
        remaining_len: VarU32,
    ) -> Result<FixedHeader> {
        if remaining_len > VarU32::MAX {
            err!(PayloadTooLong, desc: "payload too long for MQTT packets")?
        }

        let retain: u8 = if retain { 0b0001 } else { 0b0000 };
        let qos: u8 = u8::from(qos) << 1;
        let dup: u8 = if dup { 0b1000 } else { 0b0000 };
        let pkt_type = u8::from(PacketType::Publish) << 4;

        let val = FixedHeader {
            byte1: pkt_type | retain | qos | dup,
            remaining_len,
        };

        Ok(val)
    }

    /// Unwrap the fixed header into (packet-type, retain, qos, dup).
    pub fn unwrap(self) -> Result<(PacketType, bool, QoS, bool)> {
        let pkt_type: PacketType = (self.byte1 >> 4).try_into()?;
        let retain = (self.byte1 & 0b0001) > 0;
        let qos: QoS = match (self.byte1 & 0b0110) >> 1 {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            qos => err!(InvalidInput, code: InvalidQoS, "qos:{} not supported", qos)?,
        };
        let dup = (self.byte1 & 0b1000) > 0;

        Ok((pkt_type, retain, qos, dup))
    }

    /// Length of fixed header. Byte 1 + (1..4) bytes. So fixed header
    /// len can vary from 2 bytes to 5 bytes 1..4 bytes are variable length encoded
    /// to represent remaining length
    pub fn len(&self) -> usize {
        1 + match *self.remaining_len {
            n if n < 128 => 1,
            n if n < 16_384 => 2,
            n if n < 2_097_152 => 3,
            n if n < *VarU32::MAX => 4,
            _ => unreachable!(),
        }
    }
}

pub type UserPair = (String, String);

impl Packetize for UserPair {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        let (key, m) = String::decode(stream)?;
        let (val, n) = String::decode(&stream[..m])?;
        Ok(((key, val), (m + n)))
    }

    fn decode_unchecked(stream: &[u8]) -> (Self, usize) {
        let (key, m) = String::decode_unchecked(stream);
        let (val, n) = String::decode_unchecked(&stream[..m]);
        ((key, val), (m + n))
    }

    fn encode(&self) -> Result<Blob> {
        let mut data = Vec::with_capacity(64);
        data.extend_from_slice(self.0.encode()?.as_ref());
        data.extend_from_slice(self.1.encode()?.as_ref());
        Ok(Blob::Large { data })
    }

    fn into_blob(self) -> Result<Blob> {
        self.encode()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyType {
    PayloadFormatIndicator = 1,
    MessageExpiryInterval = 2,
    ContentType = 3,
    ResponseTopic = 8,
    CorrelationData = 9,
    SubscriptionIdentifier = 11,
    SessionExpiryInterval = 17,
    AssignedClientIdentifier = 18,
    ServerKeepAlive = 19,
    AuthenticationMethod = 21,
    AuthenticationData = 22,
    RequestProblemInformation = 23,
    WillDelayInterval = 24,
    RequestResponseInformation = 25,
    ResponseInformation = 26,
    ServerReference = 28,
    ReasonString = 31,
    ReceiveMaximum = 33,
    TopicAliasMaximum = 34,
    TopicAlias = 35,
    MaximumQoS = 36,
    RetainAvailable = 37,
    UserProperty = 38,
    MaximumPacketSize = 39,
    WildcardSubscriptionAvailable = 40,
    SubscriptionIdentifierAvailable = 41,
    SharedSubscriptionAvailable = 42,
}

impl TryFrom<u32> for PropertyType {
    type Error = Error;

    fn try_from(val: u32) -> Result<PropertyType> {
        use PropertyType::*;

        let val = match val {
            1 => PayloadFormatIndicator,
            2 => MessageExpiryInterval,
            3 => ContentType,
            8 => ResponseTopic,
            9 => CorrelationData,
            11 => SubscriptionIdentifier,
            17 => SessionExpiryInterval,
            18 => AssignedClientIdentifier,
            19 => ServerKeepAlive,
            21 => AuthenticationMethod,
            22 => AuthenticationData,
            23 => RequestProblemInformation,
            24 => WillDelayInterval,
            25 => RequestResponseInformation,
            26 => ResponseInformation,
            28 => ServerReference,
            31 => ReasonString,
            33 => ReceiveMaximum,
            34 => TopicAliasMaximum,
            35 => TopicAlias,
            36 => MaximumQoS,
            37 => RetainAvailable,
            38 => UserProperty,
            39 => MaximumPacketSize,
            40 => WildcardSubscriptionAvailable,
            41 => SubscriptionIdentifierAvailable,
            42 => SharedSubscriptionAvailable,
            _ => err!(MalformedPacket, code: MalformedPacket, "u32->PropertyType")?,
        };

        Ok(val)
    }
}

pub enum Property {
    PayloadFormatIndicator(u8),
    MessageExpiryInterval(u32),
    ContentType(String),
    ResponseTopic(String),
    CorrelationData(Vec<u8>),
    SubscriptionIdentifier(VarU32),
    SessionExpiryInterval(u32),
    AssignedClientIdentifier(String),
    ServerKeepAlive(u16),
    AuthenticationMethod(String),
    AuthenticationData(Vec<u8>),
    RequestProblemInformation(u8),
    WillDelayInterval(u32),
    RequestResponseInformation(u8),
    ResponseInformation(String),
    ServerReference(String),
    ReasonString(String),
    ReceiveMaximum(u16),
    TopicAliasMaximum(u16),
    TopicAlias(u16),
    MaximumQoS(QoS),
    RetainAvailable(u8),
    UserProperty(UserPair),
    MaximumPacketSize(u32),
    WildcardSubscriptionAvailable(u8),
    SubscriptionIdentifierAvailable(u8),
    SharedSubscriptionAvailable(u8),
}

impl Packetize for Property {
    fn decode(mut stream: &[u8]) -> Result<(Self, usize)> {
        use Property::*;

        let (t, m) = VarU32::decode(stream)?;
        stream = &stream[m..];
        let (property, n) = match PropertyType::try_from(*t)? {
            PropertyType::PayloadFormatIndicator => {
                let (val, n) = u8::decode(stream)?;
                (PayloadFormatIndicator(val), n)
            }
            PropertyType::MessageExpiryInterval => {
                let (val, n) = u32::decode(stream)?;
                (MessageExpiryInterval(val), n)
            }
            PropertyType::ContentType => {
                let (val, n) = String::decode(stream)?;
                (ContentType(val), n)
            }
            PropertyType::ResponseTopic => {
                let (val, n) = String::decode(stream)?;
                (ResponseTopic(val), n)
            }
            PropertyType::CorrelationData => {
                let (val, n) = Vec::<u8>::decode(stream)?;
                (CorrelationData(val), n)
            }
            PropertyType::SubscriptionIdentifier => {
                let (val, n) = VarU32::decode(stream)?;
                (SubscriptionIdentifier(val), n)
            }
            PropertyType::SessionExpiryInterval => {
                let (val, n) = u32::decode(stream)?;
                (SessionExpiryInterval(val), n)
            }
            PropertyType::AssignedClientIdentifier => {
                let (val, n) = String::decode(stream)?;
                (AssignedClientIdentifier(val), n)
            }
            PropertyType::ServerKeepAlive => {
                let (val, n) = u16::decode(stream)?;
                (ServerKeepAlive(val), n)
            }
            PropertyType::AuthenticationMethod => {
                let (val, n) = String::decode(stream)?;
                (AuthenticationMethod(val), n)
            }
            PropertyType::AuthenticationData => {
                let (val, n) = Vec::<u8>::decode(stream)?;
                (AuthenticationData(val), n)
            }
            PropertyType::RequestProblemInformation => {
                let (val, n) = u8::decode(stream)?;
                (RequestProblemInformation(val), n)
            }
            PropertyType::WillDelayInterval => {
                let (val, n) = u32::decode(stream)?;
                (WillDelayInterval(val), n)
            }
            PropertyType::RequestResponseInformation => {
                let (val, n) = u8::decode(stream)?;
                (RequestResponseInformation(val), n)
            }
            PropertyType::ResponseInformation => {
                let (val, n) = String::decode(stream)?;
                (ResponseInformation(val), n)
            }
            PropertyType::ServerReference => {
                let (val, n) = String::decode(stream)?;
                (ServerReference(val), n)
            }
            PropertyType::ReasonString => {
                let (val, n) = String::decode(stream)?;
                (ReasonString(val), n)
            }
            PropertyType::ReceiveMaximum => {
                let (val, n) = u16::decode(stream)?;
                (ReceiveMaximum(val), n)
            }
            PropertyType::TopicAliasMaximum => {
                let (val, n) = u16::decode(stream)?;
                (TopicAliasMaximum(val), n)
            }
            PropertyType::TopicAlias => {
                let (val, n) = u16::decode(stream)?;
                (TopicAlias(val), n)
            }
            PropertyType::MaximumQoS => {
                let (val, n) = u8::decode(stream)?;
                let qos = QoS::try_from(val)?;
                (MaximumQoS(qos), n)
            }
            PropertyType::RetainAvailable => {
                let (val, n) = u8::decode(stream)?;
                (RetainAvailable(val), n)
            }
            PropertyType::UserProperty => {
                let (val, n) = UserPair::decode(stream)?;
                (UserProperty(val), n)
            }
            PropertyType::MaximumPacketSize => {
                let (val, n) = u32::decode(stream)?;
                (MaximumPacketSize(val), n)
            }
            PropertyType::WildcardSubscriptionAvailable => {
                let (val, n) = u8::decode(stream)?;
                (WildcardSubscriptionAvailable(val), n)
            }
            PropertyType::SubscriptionIdentifierAvailable => {
                let (val, n) = u8::decode(stream)?;
                (SubscriptionIdentifierAvailable(val), n)
            }
            PropertyType::SharedSubscriptionAvailable => {
                let (val, n) = u8::decode(stream)?;
                (SharedSubscriptionAvailable(val), n)
            }
        };

        Ok((property, m + n))
    }

    fn decode_unchecked(stream: &[u8]) -> (Self, usize) {
        Self::decode(stream).unwrap()
    }

    fn encode(&self) -> Result<Blob> {
        use Property::*;

        let mut data = Vec::with_capacity(64);
        match self {
            PayloadFormatIndicator(val) => {
                data.extend_from_slice(
                    &(PropertyType::PayloadFormatIndicator as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.to_be_bytes());
            }
            MessageExpiryInterval(val) => {
                data.extend_from_slice(
                    &(PropertyType::MessageExpiryInterval as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.to_be_bytes());
            }
            ContentType(val) => {
                data.extend_from_slice(&(PropertyType::ContentType as u8).to_be_bytes());
                data.extend_from_slice(val.encode()?.as_ref());
            }
            ResponseTopic(val) => {
                data.extend_from_slice(
                    &(PropertyType::ResponseTopic as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.encode()?.as_ref());
            }
            CorrelationData(val) => {
                data.extend_from_slice(
                    &(PropertyType::CorrelationData as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.encode()?.as_ref());
            }
            SubscriptionIdentifier(val) => {
                data.extend_from_slice(
                    &(PropertyType::SubscriptionIdentifier as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.encode()?.as_ref());
            }
            SessionExpiryInterval(val) => {
                data.extend_from_slice(
                    &(PropertyType::SessionExpiryInterval as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.to_be_bytes());
            }
            AssignedClientIdentifier(val) => {
                data.extend_from_slice(
                    &(PropertyType::AssignedClientIdentifier as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.encode()?.as_ref());
            }
            ServerKeepAlive(val) => {
                data.extend_from_slice(
                    &(PropertyType::ServerKeepAlive as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.to_be_bytes());
            }
            AuthenticationMethod(val) => {
                data.extend_from_slice(
                    &(PropertyType::AuthenticationMethod as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.encode()?.as_ref());
            }
            AuthenticationData(val) => {
                data.extend_from_slice(
                    &(PropertyType::AuthenticationData as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.encode()?.as_ref());
            }
            RequestProblemInformation(val) => {
                data.extend_from_slice(
                    &(PropertyType::RequestProblemInformation as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.to_be_bytes());
            }
            WillDelayInterval(val) => {
                data.extend_from_slice(
                    &(PropertyType::WillDelayInterval as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.to_be_bytes());
            }
            RequestResponseInformation(val) => {
                data.extend_from_slice(
                    &(PropertyType::RequestResponseInformation as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.to_be_bytes());
            }
            ResponseInformation(val) => {
                data.extend_from_slice(
                    &(PropertyType::ResponseInformation as u8).to_be_bytes(),
                );
                data.extend_from_slice(val.encode()?.as_ref());
            }
            ServerReference(val) => {
                data.extend_from_slice(
                    &(PropertyType::ServerReference as u8).to_be_bytes(),
                );
                data.extend_from_slice(val.encode()?.as_ref());
            }
            ReasonString(val) => {
                data.extend_from_slice(&(PropertyType::ReasonString as u8).to_be_bytes());
                data.extend_from_slice(&val.encode()?.as_ref());
            }
            ReceiveMaximum(val) => {
                data.extend_from_slice(
                    &(PropertyType::ReceiveMaximum as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.to_be_bytes());
            }
            TopicAliasMaximum(val) => {
                data.extend_from_slice(
                    &(PropertyType::TopicAliasMaximum as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.to_be_bytes());
            }
            TopicAlias(val) => {
                data.extend_from_slice(&(PropertyType::TopicAlias as u8).to_be_bytes());
                data.extend_from_slice(&val.to_be_bytes());
            }
            MaximumQoS(val) => {
                data.extend_from_slice(&(PropertyType::MaximumQoS as u8).to_be_bytes());
                data.extend_from_slice(&u8::from(*val).to_be_bytes());
            }
            RetainAvailable(val) => {
                data.extend_from_slice(
                    &(PropertyType::RetainAvailable as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.to_be_bytes());
            }
            UserProperty(val) => {
                data.extend_from_slice(&(PropertyType::UserProperty as u8).to_be_bytes());
                data.extend_from_slice(&val.encode()?.as_ref());
            }
            MaximumPacketSize(val) => {
                data.extend_from_slice(
                    &(PropertyType::MaximumPacketSize as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.to_be_bytes());
            }
            WildcardSubscriptionAvailable(val) => {
                data.extend_from_slice(
                    &(PropertyType::WildcardSubscriptionAvailable as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.to_be_bytes());
            }
            SubscriptionIdentifierAvailable(val) => {
                data.extend_from_slice(
                    &(PropertyType::SubscriptionIdentifierAvailable as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.to_be_bytes());
            }
            SharedSubscriptionAvailable(val) => {
                data.extend_from_slice(
                    &(PropertyType::SharedSubscriptionAvailable as u8).to_be_bytes(),
                );
                data.extend_from_slice(&val.to_be_bytes());
            }
        };

        Ok(Blob::Large { data })
    }

    fn into_blob(self) -> Result<Blob> {
        self.encode()
    }
}

//#[cfg(any(feature = "fuzzy", test))]
//#[path = "types_arbitrary.rs"]
//mod types_arbitrary;

#[cfg(test)]
#[path = "mod_test.rs"]
mod mod_test;
