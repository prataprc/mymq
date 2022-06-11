#[cfg(any(feature = "fuzzy", test))]
use arbitrary::Arbitrary;

use crate::util::advance;
use crate::{Blob, Packetize, Result, UserProperty, VarU32};
use crate::{Error, ErrorKind, ReasonCode};

mod connect;

pub use connect::{ConnectFlags, ConnectProperties, WillProperties};

// TODO: FixedHeader.remaining_len must be validated with
//       ConnectProperties.maximum_pkt_size.
// TODO: first socket read for fixed-header can wait indefinitely, but the next read
//       for remaining_len must timeout within a stipulated period.

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

#[macro_export]
macro_rules! fixed_byte {
    ($pkt_type:expr, $retain:ident, $qos:ident, $dup:ident) => {{
        let retain: u8 = if $retain { 0b0001 } else { 0b0000 };
        let qos: u8 = u8::from($qos) << 1;
        let dup: u8 = if $dup { 0b1000 } else { 0b0000 };
        let pkt_type = $pkt_type << 4;

        pkt_type | retain | qos | dup
    }};
}

impl Packetize for FixedHeader {
    fn decode(stream: &[u8]) -> Result<(FixedHeader, usize)> {
        let (byte1, m) = u8::decode(stream)?;
        let (remaining_len, n) = VarU32::decode(advance(stream, m)?)?;

        let fh = FixedHeader { byte1, remaining_len };
        fh.validate()?;

        Ok((fh, m + n))
    }

    fn encode(&self) -> Result<Blob> {
        let byte1 = self.byte1.encode()?;
        let remaining_len = self.remaining_len.encode()?;
        let (m, n) = (byte1.as_ref().len(), remaining_len.as_ref().len());

        let mut data = [0_u8; 32];
        data[..m].copy_from_slice(byte1.as_ref());
        data[m..m + n].copy_from_slice(remaining_len.as_ref());

        Ok(Blob::Small { data, size: m + n })
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

    pub fn new_publish(
        retain: bool,
        qos: QoS,
        dup: bool,
        remaining_len: VarU32,
    ) -> Result<FixedHeader> {
        if remaining_len > VarU32::MAX {
            err!(PayloadTooLong, desc: "payload too long for MQTT packets")?
        }

        let val = FixedHeader {
            byte1: fixed_byte!(u8::from(PacketType::Publish), retain, qos, dup),
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
    pub fn len(&self) -> Result<usize> {
        let val = 1 + match *self.remaining_len {
            n if n < 128 => 1,
            n if n < 16_384 => 2,
            n if n < 2_097_152 => 3,
            n if n < *VarU32::MAX => 4,
            n => err!(InvalidInput, code: MalformedPacket, "remaining-len {}", n)?,
        };

        Ok(val)
    }

    fn validate(&self) -> Result<()> {
        let (pkt_type, retain, qos, dup) = self.unwrap()?;
        match pkt_type {
            PacketType::Publish => Ok(()),
            _ if retain || dup || qos != QoS::AtMostOnce => err!(
                MalformedPacket,
                code: MalformedPacket,
                "flags found for {:?}",
                pkt_type
            ),
            _ => Ok(()),
        }
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
    UserProp = 38,
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
            38 => UserProp,
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
    UserProp(UserProperty),
    MaximumPacketSize(u32),
    WildcardSubscriptionAvailable(u8),
    SubscriptionIdentifierAvailable(u8),
    SharedSubscriptionAvailable(u8),
}

macro_rules! dec_prop {
    ($varn:ident, $valtype:ty, $stream:expr) => {{
        let (val, n) = <$valtype>::decode($stream)?;
        (Property::$varn(val), n)
    }};
}

macro_rules! enc_prop {
    ($data:ident, $varn:ident, $($action:tt)*) => {{
        $data.extend_from_slice(&(PropertyType::$varn as u8).to_be_bytes());
        $data.extend_from_slice($($action)*);
    }};
}

impl Packetize for Property {
    fn decode(mut stream: &[u8]) -> Result<(Self, usize)> {
        use PropertyType::*;

        let (t, m) = VarU32::decode(stream)?;
        stream = advance(stream, m)?;
        let (property, n) = match PropertyType::try_from(*t)? {
            PayloadFormatIndicator => dec_prop!(PayloadFormatIndicator, u8, stream),
            MessageExpiryInterval => dec_prop!(MessageExpiryInterval, u32, stream),
            ContentType => dec_prop!(ContentType, String, stream),
            ResponseTopic => dec_prop!(ResponseTopic, String, stream),
            CorrelationData => {
                let (val, n) = Vec::<u8>::decode(stream)?;
                (Property::CorrelationData(val), n)
            }
            SubscriptionIdentifier => dec_prop!(SubscriptionIdentifier, VarU32, stream),
            SessionExpiryInterval => dec_prop!(SessionExpiryInterval, u32, stream),
            AssignedClientIdentifier => {
                dec_prop!(AssignedClientIdentifier, String, stream)
            }
            ServerKeepAlive => dec_prop!(ServerKeepAlive, u16, stream),
            AuthenticationMethod => dec_prop!(AuthenticationMethod, String, stream),
            AuthenticationData => {
                let (val, n) = Vec::<u8>::decode(stream)?;
                (Property::AuthenticationData(val), n)
            }
            RequestProblemInformation => dec_prop!(RequestProblemInformation, u8, stream),
            WillDelayInterval => dec_prop!(WillDelayInterval, u32, stream),
            RequestResponseInformation => {
                dec_prop!(RequestResponseInformation, u8, stream)
            }
            ResponseInformation => dec_prop!(ResponseInformation, String, stream),
            ServerReference => dec_prop!(ServerReference, String, stream),
            ReasonString => dec_prop!(ReasonString, String, stream),
            ReceiveMaximum => dec_prop!(ReceiveMaximum, u16, stream),
            TopicAliasMaximum => dec_prop!(TopicAliasMaximum, u16, stream),
            TopicAlias => dec_prop!(TopicAlias, u16, stream),
            MaximumQoS => {
                let (val, n) = u8::decode(stream)?;
                let qos = QoS::try_from(val)?;
                (Property::MaximumQoS(qos), n)
            }
            RetainAvailable => dec_prop!(RetainAvailable, u8, stream),
            UserProp => dec_prop!(UserProp, UserProperty, stream),
            MaximumPacketSize => dec_prop!(MaximumPacketSize, u32, stream),
            WildcardSubscriptionAvailable => {
                dec_prop!(WildcardSubscriptionAvailable, u8, stream)
            }
            SubscriptionIdentifierAvailable => {
                dec_prop!(SubscriptionIdentifierAvailable, u8, stream)
            }
            SharedSubscriptionAvailable => {
                dec_prop!(SharedSubscriptionAvailable, u8, stream)
            }
        };

        Ok((property, m + n))
    }

    fn encode(&self) -> Result<Blob> {
        use Property::*;

        let mut data = Vec::with_capacity(64);
        match self {
            PayloadFormatIndicator(val) => {
                enc_prop!(data, PayloadFormatIndicator, &val.to_be_bytes());
            }
            MessageExpiryInterval(val) => {
                enc_prop!(data, MessageExpiryInterval, &val.to_be_bytes());
            }
            ContentType(val) => {
                enc_prop!(data, ContentType, val.encode()?.as_ref());
            }
            ResponseTopic(val) => {
                enc_prop!(data, ResponseTopic, &val.encode()?.as_ref());
            }
            CorrelationData(val) => {
                enc_prop!(data, CorrelationData, &val.encode()?.as_ref());
            }
            SubscriptionIdentifier(val) => {
                enc_prop!(data, SubscriptionIdentifier, &val.encode()?.as_ref());
            }
            SessionExpiryInterval(val) => {
                enc_prop!(data, SessionExpiryInterval, &val.to_be_bytes());
            }
            AssignedClientIdentifier(val) => {
                enc_prop!(data, AssignedClientIdentifier, &val.encode()?.as_ref());
            }
            ServerKeepAlive(val) => {
                enc_prop!(data, ServerKeepAlive, &val.to_be_bytes());
            }
            AuthenticationMethod(val) => {
                enc_prop!(data, AuthenticationMethod, &val.encode()?.as_ref());
            }
            AuthenticationData(val) => {
                enc_prop!(data, AuthenticationData, &val.encode()?.as_ref());
            }
            RequestProblemInformation(val) => {
                enc_prop!(data, RequestProblemInformation, &val.to_be_bytes());
            }
            WillDelayInterval(val) => {
                enc_prop!(data, WillDelayInterval, &val.to_be_bytes());
            }
            RequestResponseInformation(val) => {
                enc_prop!(data, RequestResponseInformation, &val.to_be_bytes());
            }
            ResponseInformation(val) => {
                enc_prop!(data, ResponseInformation, val.encode()?.as_ref());
            }
            ServerReference(val) => {
                enc_prop!(data, ServerReference, val.encode()?.as_ref());
            }
            ReasonString(val) => {
                enc_prop!(data, ReasonString, &val.encode()?.as_ref());
            }
            ReceiveMaximum(val) => {
                enc_prop!(data, ReceiveMaximum, &val.to_be_bytes());
            }
            TopicAliasMaximum(val) => {
                enc_prop!(data, TopicAliasMaximum, &val.to_be_bytes());
            }
            TopicAlias(val) => {
                enc_prop!(data, TopicAlias, &val.to_be_bytes());
            }
            MaximumQoS(val) => {
                enc_prop!(data, MaximumQoS, &u8::from(*val).to_be_bytes());
            }
            RetainAvailable(val) => {
                enc_prop!(data, RetainAvailable, &val.to_be_bytes());
            }
            UserProp(val) => {
                enc_prop!(data, UserProp, &val.encode()?.as_ref());
            }
            MaximumPacketSize(val) => {
                enc_prop!(data, MaximumPacketSize, &val.to_be_bytes());
            }
            WildcardSubscriptionAvailable(val) => {
                enc_prop!(data, WildcardSubscriptionAvailable, &val.to_be_bytes());
            }
            SubscriptionIdentifierAvailable(val) => {
                enc_prop!(data, SubscriptionIdentifierAvailable, &val.to_be_bytes());
            }
            SharedSubscriptionAvailable(val) => {
                enc_prop!(data, SharedSubscriptionAvailable, &val.to_be_bytes());
            }
        };

        Ok(Blob::Large { data })
    }
}

impl Property {
    fn to_property_type(&self) -> PropertyType {
        use PropertyType::*;

        match self {
            Property::PayloadFormatIndicator(_) => PayloadFormatIndicator,
            Property::MessageExpiryInterval(_) => MessageExpiryInterval,
            Property::ContentType(_) => ContentType,
            Property::ResponseTopic(_) => ResponseTopic,
            Property::CorrelationData(_) => CorrelationData,
            Property::SubscriptionIdentifier(_) => SubscriptionIdentifier,
            Property::SessionExpiryInterval(_) => SessionExpiryInterval,
            Property::AssignedClientIdentifier(_) => AssignedClientIdentifier,
            Property::ServerKeepAlive(_) => ServerKeepAlive,
            Property::AuthenticationMethod(_) => AuthenticationMethod,
            Property::AuthenticationData(_) => AuthenticationData,
            Property::RequestProblemInformation(_) => RequestProblemInformation,
            Property::WillDelayInterval(_) => WillDelayInterval,
            Property::RequestResponseInformation(_) => RequestResponseInformation,
            Property::ResponseInformation(_) => ResponseInformation,
            Property::ServerReference(_) => ServerReference,
            Property::ReasonString(_) => ReasonString,
            Property::ReceiveMaximum(_) => ReceiveMaximum,
            Property::TopicAliasMaximum(_) => TopicAliasMaximum,
            Property::TopicAlias(_) => TopicAlias,
            Property::MaximumQoS(_) => MaximumQoS,
            Property::RetainAvailable(_) => RetainAvailable,
            Property::UserProp(_) => UserProp,
            Property::MaximumPacketSize(_) => MaximumPacketSize,
            Property::WildcardSubscriptionAvailable(_) => WildcardSubscriptionAvailable,
            Property::SubscriptionIdentifierAvailable(_) => {
                SubscriptionIdentifierAvailable
            }
            Property::SharedSubscriptionAvailable(_) => SharedSubscriptionAvailable,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadFormat {
    Binary = 0,
    Utf8 = 1,
}

impl Default for PayloadFormat {
    fn default() -> PayloadFormat {
        PayloadFormat::Binary
    }
}

impl TryFrom<u8> for PayloadFormat {
    type Error = Error;

    fn try_from(val: u8) -> Result<PayloadFormat> {
        match val {
            0 => Ok(PayloadFormat::Binary),
            1 => Ok(PayloadFormat::Utf8),
            _ => {
                err!(ProtocolError, code: ProtocolError, "invalid payload format {}", val)
            }
        }
    }
}

impl From<PayloadFormat> for u8 {
    fn from(val: PayloadFormat) -> u8 {
        match val {
            PayloadFormat::Binary => 0,
            PayloadFormat::Utf8 => 1,
        }
    }
}
#[cfg(test)]
#[path = "mod_test.rs"]
mod mod_test;

#[cfg(any(feature = "fuzzy", test))]
#[path = "mod_fuzzy.rs"]
mod mod_fuzzy;
