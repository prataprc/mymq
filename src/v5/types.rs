use std::{fmt, result};

use crate::util::advance;
use crate::v5::{Auth, Disconnect, SubAck, Subscribe, UnSubscribe, UnsubAck};
use crate::v5::{ConnAck, Connect, PingReq, PingResp, Pub, Publish};
use crate::ClientID;
use crate::{Blob, PacketType, Packetize, QoS, Subscription, TopicName, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

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

/// Enumeration of all possible MQTT packets, its header, fields, properties, payload.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Packet {
    Connect(Connect),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck(Pub),
    PubRec(Pub),
    PubRel(Pub),
    PubComp(Pub),
    Subscribe(Subscribe),
    SubAck(SubAck),
    UnSubscribe(UnSubscribe),
    UnsubAck(UnsubAck),
    PingReq,
    PingResp,
    Disconnect(Disconnect),
    Auth(Auth),
}

impl fmt::Display for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            Packet::Connect(connect) => <Connect as fmt::Display>::fmt(connect, f),
            Packet::ConnAck(connack) => <ConnAck as fmt::Display>::fmt(connack, f),
            Packet::Publish(publish) => <Publish as fmt::Display>::fmt(publish, f),
            Packet::PubAck(val) => <Pub as fmt::Display>::fmt(val, f),
            Packet::PubRec(val) => <Pub as fmt::Display>::fmt(val, f),
            Packet::PubRel(val) => <Pub as fmt::Display>::fmt(val, f),
            Packet::PubComp(val) => <Pub as fmt::Display>::fmt(val, f),
            Packet::Subscribe(sub) => <Subscribe as fmt::Display>::fmt(sub, f),
            Packet::SubAck(suback) => <SubAck as fmt::Display>::fmt(suback, f),
            Packet::UnSubscribe(unsub) => <UnSubscribe as fmt::Display>::fmt(unsub, f),
            Packet::UnsubAck(unsub_ack) => <UnsubAck as fmt::Display>::fmt(unsub_ack, f),
            Packet::PingReq => write!(f, "PINGREQ"),
            Packet::PingResp => write!(f, "PINGRESP"),
            Packet::Disconnect(disconn) => <Disconnect as fmt::Display>::fmt(disconn, f),
            Packet::Auth(auth) => <Auth as fmt::Display>::fmt(auth, f),
        }
    }
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for Packet {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let pkt_type: PacketType = uns.arbitrary()?;
        let pkt = match pkt_type {
            PacketType::Connect => Packet::Connect(uns.arbitrary()?),
            PacketType::ConnAck => Packet::ConnAck(uns.arbitrary()?),
            PacketType::Publish => Packet::Publish(uns.arbitrary()?),
            PacketType::PubAck
            | PacketType::PubRec
            | PacketType::PubRel
            | PacketType::PubComp => {
                let pkt: Pub = uns.arbitrary()?;
                match pkt.packet_type {
                    PacketType::PubAck => Packet::PubAck(pkt),
                    PacketType::PubRec => Packet::PubRec(pkt),
                    PacketType::PubRel => Packet::PubRel(pkt),
                    PacketType::PubComp => Packet::PubComp(pkt),
                    _ => unreachable!(),
                }
            }
            PacketType::Subscribe => Packet::Subscribe(uns.arbitrary()?),
            PacketType::SubAck => Packet::SubAck(uns.arbitrary()?),
            PacketType::UnSubscribe => Packet::UnSubscribe(uns.arbitrary()?),
            PacketType::UnsubAck => Packet::UnsubAck(uns.arbitrary()?),
            PacketType::PingReq => Packet::PingReq,
            PacketType::PingResp => Packet::PingResp,
            PacketType::Disconnect => Packet::Disconnect(uns.arbitrary()?),
            PacketType::Auth => Packet::Auth(uns.arbitrary()?),
        };

        Ok(pkt)
    }
}

impl Packetize for Packet {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();
        let (fh, _) = FixedHeader::decode(stream)?;

        match fh.unwrap().0 {
            PacketType::Connect => {
                let (pkt, n) = Connect::decode(stream)?;
                Ok((Packet::Connect(pkt), n))
            }
            PacketType::ConnAck => {
                let (pkt, n) = ConnAck::decode(stream)?;
                Ok((Packet::ConnAck(pkt), n))
            }
            PacketType::Publish => {
                let (pkt, n) = Publish::decode(stream)?;
                Ok((Packet::Publish(pkt), n))
            }
            PacketType::PubAck => {
                let (pkt, n) = Pub::decode(stream)?;
                Ok((Packet::PubAck(pkt), n))
            }
            PacketType::PubRec => {
                let (pkt, n) = Pub::decode(stream)?;
                Ok((Packet::PubRec(pkt), n))
            }
            PacketType::PubRel => {
                let (pkt, n) = Pub::decode(stream)?;
                Ok((Packet::PubRel(pkt), n))
            }
            PacketType::PubComp => {
                let (pkt, n) = Pub::decode(stream)?;
                Ok((Packet::PubComp(pkt), n))
            }
            PacketType::Subscribe => {
                let (pkt, n) = Subscribe::decode(stream)?;
                Ok((Packet::Subscribe(pkt), n))
            }
            PacketType::SubAck => {
                let (pkt, n) = SubAck::decode(stream)?;
                Ok((Packet::SubAck(pkt), n))
            }
            PacketType::UnSubscribe => {
                let (pkt, n) = UnSubscribe::decode(stream)?;
                Ok((Packet::UnSubscribe(pkt), n))
            }
            PacketType::UnsubAck => {
                let (pkt, n) = UnsubAck::decode(stream)?;
                Ok((Packet::UnsubAck(pkt), n))
            }
            PacketType::PingReq => {
                let (_pkt, n) = PingReq::decode(stream)?;
                Ok((Packet::PingReq, n))
            }
            PacketType::PingResp => {
                let (_pkt, n) = PingResp::decode(stream)?;
                Ok((Packet::PingResp, n))
            }
            PacketType::Disconnect => {
                let (pkt, n) = Disconnect::decode(stream)?;
                Ok((Packet::Disconnect(pkt), n))
            }
            PacketType::Auth => {
                let (pkt, n) = Auth::decode(stream)?;
                Ok((Packet::Auth(pkt), n))
            }
        }
    }

    fn encode(&self) -> Result<Blob> {
        match self {
            Packet::Connect(pkt) => pkt.encode(),
            Packet::ConnAck(pkt) => pkt.encode(),
            Packet::Publish(pkt) => pkt.encode(),
            Packet::PubAck(pkt) => pkt.encode(),
            Packet::PubRec(pkt) => pkt.encode(),
            Packet::PubRel(pkt) => pkt.encode(),
            Packet::PubComp(pkt) => pkt.encode(),
            Packet::Subscribe(pkt) => pkt.encode(),
            Packet::SubAck(pkt) => pkt.encode(),
            Packet::UnSubscribe(pkt) => pkt.encode(),
            Packet::UnsubAck(pkt) => pkt.encode(),
            Packet::PingReq => PingReq.encode(),
            Packet::PingResp => PingResp.encode(),
            Packet::Disconnect(pkt) => pkt.encode(),
            Packet::Auth(pkt) => pkt.encode(),
        }
    }
}

impl Packet {
    pub fn to_packet_type(&self) -> PacketType {
        match self {
            Packet::Connect(_) => PacketType::Connect,
            Packet::ConnAck(_) => PacketType::ConnAck,
            Packet::Publish(_) => PacketType::Publish,
            Packet::PubAck(_) => PacketType::PubAck,
            Packet::PubRec(_) => PacketType::PubRec,
            Packet::PubRel(_) => PacketType::PubRel,
            Packet::PubComp(_) => PacketType::PubComp,
            Packet::Subscribe(_) => PacketType::Subscribe,
            Packet::SubAck(_) => PacketType::SubAck,
            Packet::UnSubscribe(_) => PacketType::UnSubscribe,
            Packet::UnsubAck(_) => PacketType::UnsubAck,
            Packet::PingReq => PacketType::PingReq,
            Packet::PingResp => PacketType::PingResp,
            Packet::Disconnect(_) => PacketType::Disconnect,
            Packet::Auth(_) => PacketType::Auth,
        }
    }

    pub fn to_packet_id(&self) -> Option<u16> {
        match self {
            Packet::Publish(publish) => publish.packet_id,
            Packet::Subscribe(sub) => Some(sub.packet_id),
            Packet::UnSubscribe(unsub) => Some(unsub.packet_id),
            Packet::PubAck(puback) => Some(puback.packet_id),
            Packet::SubAck(suback) => Some(suback.packet_id),
            Packet::UnsubAck(unsuback) => Some(unsuback.packet_id),
            pkt => unreachable!("{}", pkt),
        }
    }

    pub fn to_qos(&self) -> QoS {
        match self {
            Packet::Publish(publish) => publish.qos,
            pkt => unreachable!("{}", pkt),
        }
    }

    pub fn to_subscription_id(&self) -> Option<u32> {
        match self {
            Packet::Subscribe(sub) => sub.to_subscription_id(),
            pkt => unreachable!("{}", pkt),
        }
    }

    pub fn to_subscriptions(&self) -> Vec<Subscription> {
        match self {
            Packet::Subscribe(sub) => sub.to_subscriptions(),
            pkt => unreachable!("{}", pkt),
        }
    }

    pub fn to_unsubscriptions(&self, client_id: ClientID) -> Vec<Subscription> {
        match self {
            Packet::UnSubscribe(unsub) => unsub.to_unsubscriptions(client_id),
            pkt => unreachable!("{}", pkt),
        }
    }

    pub fn to_topic_alias(&self) -> Option<u16> {
        match self {
            Packet::Publish(publish) => publish.to_topic_alias(),
            pkt => unreachable!("{}", pkt),
        }
    }

    pub fn to_disconnect_code(&self) -> ReasonCode {
        match self {
            Packet::Disconnect(disconnect) => disconnect.code,
            pkt => unreachable!("{}", pkt),
        }
    }

    pub fn to_reason_string(&self) -> Option<String> {
        match self {
            Packet::Disconnect(disconnect) => disconnect.reason_string(),
            pkt => unreachable!("{}", pkt),
        }
    }

    pub fn as_topic_name(&self) -> &TopicName {
        match self {
            Packet::Publish(publish) => &publish.as_topic_name(),
            pkt => unreachable!("{}", pkt),
        }
    }

    pub fn is_qos0(&self) -> bool {
        match self {
            Packet::Publish(publish) => publish.is_qos0(),
            pkt => unreachable!("{}", pkt),
        }
    }

    pub fn is_qos12(&self) -> bool {
        match self {
            Packet::Publish(publish) => publish.is_qos12(),
            pkt => unreachable!("{}", pkt),
        }
    }

    #[cfg(any(feature = "fuzzy", test))]
    pub fn normalize(&mut self) {
        match self {
            Packet::Connect(val) => val.normalize(),
            Packet::ConnAck(val) => val.normalize(),
            Packet::Publish(val) => val.normalize(),
            Packet::PubAck(val) => val.normalize(),
            Packet::PubRec(val) => val.normalize(),
            Packet::PubRel(val) => val.normalize(),
            Packet::PubComp(val) => val.normalize(),
            Packet::Subscribe(val) => val.normalize(),
            Packet::SubAck(val) => val.normalize(),
            Packet::UnSubscribe(val) => val.normalize(),
            Packet::UnsubAck(val) => val.normalize(),
            Packet::PingReq => (),
            Packet::PingResp => (),
            Packet::Disconnect(val) => val.normalize(),
            Packet::Auth(val) => val.normalize(),
        }
    }
}

impl Packet {
    pub fn set_packet_id(&mut self, packet_id: u16) {
        match self {
            Packet::Publish(publish) => {
                publish.set_packet_id(packet_id);
            }
            pkt => unreachable!("{}", pkt),
        }
    }

    pub fn set_retain(&mut self, retain: bool) {
        match self {
            Packet::Publish(publish) => {
                publish.retain = retain;
            }
            pkt => unreachable!("{}", pkt),
        }
    }

    pub fn set_fixed_header(&mut self, retain: bool, qos: QoS, dup: bool) {
        match self {
            Packet::Publish(publish) => {
                publish.set_fixed_header(retain, qos, dup);
            }
            pkt => unreachable!("{}", pkt),
        }
    }

    pub fn set_subscription_ids(&mut self, ids: Vec<u32>) {
        match self {
            Packet::Publish(publish) => {
                publish.set_subscription_ids(ids);
            }
            pkt => unreachable!("{}", pkt),
        }
    }

    #[inline]
    pub fn is_retain(&self) -> bool {
        match self {
            Packet::Publish(publish) => publish.retain,
            pkt => unreachable!("{}", pkt),
        }
    }

    #[inline]
    pub fn message_expiry_interval(&self) -> Option<u32> {
        match self {
            Packet::Publish(publish) => publish.message_expiry_interval(),
            pkt => unreachable!("{}", pkt),
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
    pub byte1: u8,
    /// Remaining length of the packet. Doesn't include fixed header bytes
    /// Represents variable header + payload size
    pub remaining_len: VarU32,
}

/// MQTT packetization, to create the 1 byte fixed-header.
macro_rules! fixed_byte {
    ($pkt_type:expr, $retain:ident, $qos:ident, $dup:ident) => {{
        let retain: u8 = if $retain { 0b0001 } else { 0b0000 };
        let qos: u8 = u8::from($qos) << 1;
        let dup: u8 = if $dup { 0b1000 } else { 0b0000 };
        let pkt_type = $pkt_type << 4;

        pkt_type | retain | qos | dup
    }};
}
pub(crate) use fixed_byte;

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for FixedHeader {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let pkt_type: PacketType = uns.arbitrary()?;
        let rem_len: VarU32 = loop {
            let rem_len = uns.arbitrary::<VarU32>()?;
            if rem_len < VarU32::MAX {
                break rem_len;
            }
        };

        let fh = match pkt_type {
            PacketType::Connect => FixedHeader::new(pkt_type, rem_len)?,
            PacketType::ConnAck => FixedHeader::new(pkt_type, rem_len)?,
            PacketType::Publish => {
                let retain: bool = uns.arbitrary()?;
                let qos: QoS = uns.arbitrary()?;
                let dup: bool = uns.arbitrary()?;
                FixedHeader::new_publish(retain, qos, dup, rem_len)?
            }
            PacketType::PubAck => FixedHeader::new(pkt_type, rem_len)?,
            PacketType::PubRec => FixedHeader::new(pkt_type, rem_len)?,
            PacketType::PubRel => FixedHeader::new_pubrel(rem_len)?,
            PacketType::PubComp => FixedHeader::new(pkt_type, rem_len)?,
            PacketType::Subscribe => FixedHeader::new_subscribe(rem_len)?,
            PacketType::SubAck => FixedHeader::new(pkt_type, rem_len)?,
            PacketType::UnSubscribe => FixedHeader::new_unsubscribe(rem_len)?,
            PacketType::UnsubAck => FixedHeader::new(pkt_type, rem_len)?,
            PacketType::PingReq => FixedHeader::new(pkt_type, rem_len)?,
            PacketType::PingResp => FixedHeader::new(pkt_type, rem_len)?,
            PacketType::Disconnect => FixedHeader::new(pkt_type, rem_len)?,
            PacketType::Auth => FixedHeader::new(pkt_type, rem_len)?,
        };

        Ok(fh)
    }
}

impl Packetize for FixedHeader {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(FixedHeader, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (byte1, n) = dec_field!(u8, stream, 0);
        let (remaining_len, n) = dec_field!(VarU32, stream, n);

        let fh = FixedHeader { byte1, remaining_len };

        fh.validate()?;
        Ok((fh, n))
    }

    fn encode(&self) -> Result<Blob> {
        self.validate()?;

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
    pub const HDR_RETAIN: u8 = 0b_0000_0001;
    pub const HDR_QOS: u8 = 0b_0000_0110;
    pub const HDR_DUP: u8 = 0b_0000_1000;
    pub const HDR_PKT_TYPE: u8 = 0b_1111_0000;

    /// Construct fixed-header for CONNECT, CONNACK, PUBACK, PUBREC, PUBCOMP, SUBACK,
    /// UNSUBACK, PINGREQ, PINGRESP, DISCONNECT, AUTH.
    pub fn new(pkt_type: PacketType, remaining_len: VarU32) -> Result<FixedHeader> {
        if remaining_len > VarU32::MAX {
            err!(ProtocolError, desc: "FixedHeader remain-len {}", *remaining_len)?
        }
        let byte1 = u8::from(pkt_type) << 4;
        Ok(FixedHeader { byte1, remaining_len })
    }

    /// Construct fixed-header for PUBLISH
    pub fn new_publish(
        retain: bool,
        qos: QoS,
        dup: bool,
        remaining_len: VarU32,
    ) -> Result<FixedHeader> {
        if remaining_len > VarU32::MAX {
            err!(ProtocolError, desc: "FixedHeader remain-len {}", *remaining_len)?
        }

        let val = FixedHeader {
            byte1: fixed_byte!(u8::from(PacketType::Publish), retain, qos, dup),
            remaining_len,
        };

        Ok(val)
    }

    /// Construct fixed-header for PUBREL
    pub fn new_pubrel(remaining_len: VarU32) -> Result<FixedHeader> {
        if remaining_len > VarU32::MAX {
            err!(ProtocolError, desc: "FixedHeader remain-len {}", *remaining_len)?
        }

        let (packet_type, qos) = (u8::from(PacketType::PubRel), QoS::AtLeastOnce);
        let val = FixedHeader {
            byte1: fixed_byte!(packet_type, false, qos, false),
            remaining_len,
        };

        Ok(val)
    }

    /// Construct fixed-header for SUBSCRIBE
    pub fn new_subscribe(remaining_len: VarU32) -> Result<FixedHeader> {
        if remaining_len > VarU32::MAX {
            err!(ProtocolError, desc: "FixedHeader remain-len {}", *remaining_len)?
        }

        let (packet_type, qos) = (u8::from(PacketType::Subscribe), QoS::AtLeastOnce);
        let val = FixedHeader {
            byte1: fixed_byte!(packet_type, false, qos, false),
            remaining_len,
        };

        Ok(val)
    }

    /// Construct fixed-header for UNSUBSCRIBE
    pub fn new_unsubscribe(remaining_len: VarU32) -> Result<FixedHeader> {
        if remaining_len > VarU32::MAX {
            err!(ProtocolError, desc: "FixedHeader remain-len {}", *remaining_len)?;
        }

        let (packet_type, qos) = (u8::from(PacketType::UnSubscribe), QoS::AtLeastOnce);
        let val = FixedHeader {
            byte1: fixed_byte!(packet_type, false, qos, false),
            remaining_len,
        };

        Ok(val)
    }

    /// Unwrap the fixed header into (packet-type, retain, qos, dup).
    pub fn unwrap(self) -> (PacketType, bool, QoS, bool) {
        let pkt_type = PacketType::try_from(self.byte1 >> 4).unwrap();
        let retain = (self.byte1 & Self::HDR_RETAIN) > 0;
        let qos = QoS::try_from((self.byte1 & Self::HDR_QOS) >> 1).unwrap();
        let dup = (self.byte1 & Self::HDR_DUP) > 0;

        (pkt_type, retain, qos, dup)
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
            n => err!(
                MalformedPacket,
                code: MalformedPacket,
                "FixedHeader, remaining-len {}",
                n
            )?,
        };

        Ok(val)
    }

    pub fn validate(&self) -> Result<()> {
        use crate::QoS::{AtLeastOnce, AtMostOnce};
        use PacketType::*;

        let _qos = QoS::try_from((self.byte1 & Self::HDR_QOS) >> 1)?;
        let _pkt_type = PacketType::try_from((self.byte1 & Self::HDR_PKT_TYPE) >> 4)?;

        let (pkt_type, retain, qos, dup) = self.unwrap();
        match pkt_type {
            Connect if qos == AtMostOnce && !retain && !dup => Ok(()),
            ConnAck if qos == AtMostOnce && !retain && !dup => Ok(()),
            Publish => Ok(()),
            PubAck if qos == AtMostOnce && !retain && !dup => Ok(()),
            PubRec if qos == AtMostOnce && !retain && !dup => Ok(()),
            PubRel if qos == AtLeastOnce && !retain && !dup => Ok(()),
            PubComp if qos == AtMostOnce && !retain && !dup => Ok(()),
            Subscribe if qos == AtLeastOnce && !retain && !dup => Ok(()),
            SubAck if qos == AtMostOnce && !retain && !dup => Ok(()),
            UnSubscribe if qos == AtLeastOnce && !retain && !dup => Ok(()),
            UnsubAck if qos == AtMostOnce && !retain && !dup => Ok(()),
            Disconnect if qos == AtMostOnce && !retain && !dup => Ok(()),
            Auth if qos == AtMostOnce && !retain && !dup => Ok(()),
            _ if retain || dup || qos != QoS::AtMostOnce => err!(
                MalformedPacket,
                code: MalformedPacket,
                "FixedHeader invalid flags byte1:0x{:x}",
                self.byte1
            ),
            _ => Ok(()),
        }
    }
}

/// Enumerated list of all property types defined in MQTT spec.
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
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
            val => err!(
                MalformedPacket,
                code: MalformedPacket,
                "invalid PropertyType {}",
                val
            )?,
        };

        Ok(val)
    }
}

/// Enumeration of property and its value that are allowed in a MQTT packet.
#[derive(Debug, Eq, PartialEq)]
pub enum Property {
    PayloadFormatIndicator(u8),
    MessageExpiryInterval(u32),
    ContentType(String),
    ResponseTopic(TopicName),
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

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for Property {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        use crate::ClientID;
        use PropertyType::*;

        let content_types: Vec<String> =
            vec!["img/png"].into_iter().map(|s| s.to_string()).collect();
        let auth_methods: Vec<String> =
            vec!["userpass"].into_iter().map(|s| s.to_string()).collect();
        let server_references: Vec<String> =
            vec!["a.b.com:1883"].into_iter().map(|s| s.to_string()).collect();

        let prop = match uns.arbitrary::<PropertyType>()? {
            PayloadFormatIndicator => {
                let val: u8 = uns.arbitrary()?;
                Property::PayloadFormatIndicator(val)
            }
            MessageExpiryInterval => {
                let val: u32 = uns.arbitrary()?;
                Property::MessageExpiryInterval(val)
            }
            ContentType => {
                let val: String = uns.choose(&content_types)?.to_string();
                Property::ContentType(val)
            }
            ResponseTopic => {
                let val: TopicName = uns.arbitrary()?;
                Property::ResponseTopic(val)
            }
            CorrelationData => {
                let val: Vec<u8> = uns.arbitrary()?;
                Property::CorrelationData(val)
            }
            SubscriptionIdentifier => {
                let val: VarU32 = uns.arbitrary()?;
                Property::SubscriptionIdentifier(val)
            }
            SessionExpiryInterval => {
                let val: u32 = uns.arbitrary()?;
                Property::SessionExpiryInterval(val)
            }
            AssignedClientIdentifier => {
                let val: String = uns.arbitrary::<ClientID>()?.to_string();
                Property::AssignedClientIdentifier(val)
            }
            ServerKeepAlive => {
                let val: u16 = uns.arbitrary()?;
                Property::ServerKeepAlive(val)
            }
            AuthenticationMethod => {
                let val: String = uns.choose(&auth_methods)?.to_string();
                Property::AuthenticationMethod(val)
            }
            AuthenticationData => {
                let val: Vec<u8> = uns.arbitrary()?;
                Property::AuthenticationData(val)
            }
            RequestProblemInformation => {
                let val: u8 = uns.arbitrary()?;
                Property::RequestProblemInformation(val)
            }
            WillDelayInterval => {
                let val: u32 = uns.arbitrary()?;
                Property::WillDelayInterval(val)
            }
            RequestResponseInformation => {
                let val: u8 = uns.arbitrary()?;
                Property::RequestResponseInformation(val)
            }
            ResponseInformation => {
                let val: String = uns.choose(&auth_methods)?.to_string();
                Property::ResponseInformation(val)
            }
            ServerReference => {
                let val: String = uns.choose(&server_references)?.to_string();
                Property::ServerReference(val)
            }
            ReasonString => {
                let val: String = "failed".to_string();
                Property::ReasonString(val)
            }
            ReceiveMaximum => {
                let val: u16 = uns.arbitrary()?;
                Property::ReceiveMaximum(val)
            }
            TopicAliasMaximum => {
                let val: u16 = uns.arbitrary()?;
                Property::TopicAliasMaximum(val)
            }
            TopicAlias => {
                let val: u16 = uns.arbitrary()?;
                Property::TopicAlias(val)
            }
            MaximumQoS => {
                let val: QoS = uns.arbitrary()?;
                Property::MaximumQoS(val)
            }
            RetainAvailable => {
                let val: u8 = uns.arbitrary()?;
                Property::RetainAvailable(val)
            }
            UserProp => {
                let val: UserProperty = valid_user_props(uns, 1)?.pop().unwrap();
                Property::UserProp(val)
            }
            MaximumPacketSize => {
                let val: u32 = uns.arbitrary()?;
                Property::MaximumPacketSize(val)
            }
            WildcardSubscriptionAvailable => {
                let val: u8 = uns.arbitrary()?;
                Property::WildcardSubscriptionAvailable(val)
            }
            SubscriptionIdentifierAvailable => {
                let val: u8 = uns.arbitrary()?;
                Property::SubscriptionIdentifierAvailable(val)
            }
            SharedSubscriptionAvailable => {
                let val: u8 = uns.arbitrary()?;
                Property::SharedSubscriptionAvailable(val)
            }
        };

        Ok(prop)
    }
}

impl Packetize for Property {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        use PropertyType::*;

        let mut stream: &[u8] = stream.as_ref();

        let (prop_type, n) = dec_field!(VarU32, stream, 0);
        stream = advance(stream, n)?;

        let (property, m) = match PropertyType::try_from(*prop_type)? {
            PayloadFormatIndicator => dec_prop!(PayloadFormatIndicator, u8, stream),
            MessageExpiryInterval => dec_prop!(MessageExpiryInterval, u32, stream),
            ContentType => dec_prop!(ContentType, String, stream),
            ResponseTopic => {
                let (val, n) = TopicName::decode(stream)?;
                (Property::ResponseTopic(val), n)
            }
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

        // println!("Property::decode {} {}", n, m);
        Ok((property, n + m))
    }

    fn encode(&self) -> Result<Blob> {
        use Property::*;

        let mut data = Vec::with_capacity(64);
        match self {
            PayloadFormatIndicator(val) => enc_prop!(data, PayloadFormatIndicator, val),
            MessageExpiryInterval(val) => enc_prop!(data, MessageExpiryInterval, val),
            ContentType(val) => enc_prop!(data, ContentType, val),
            ResponseTopic(val) => enc_prop!(data, ResponseTopic, val),
            CorrelationData(val) => enc_prop!(data, CorrelationData, val),
            SubscriptionIdentifier(val) => enc_prop!(data, SubscriptionIdentifier, val),
            SessionExpiryInterval(val) => enc_prop!(data, SessionExpiryInterval, val),
            AssignedClientIdentifier(val) => {
                enc_prop!(data, AssignedClientIdentifier, val)
            }
            ServerKeepAlive(val) => enc_prop!(data, ServerKeepAlive, val),
            AuthenticationMethod(val) => enc_prop!(data, AuthenticationMethod, val),
            AuthenticationData(val) => enc_prop!(data, AuthenticationData, val),
            RequestProblemInformation(val) => {
                enc_prop!(data, RequestProblemInformation, val)
            }
            WillDelayInterval(val) => enc_prop!(data, WillDelayInterval, val),
            RequestResponseInformation(val) => {
                enc_prop!(data, RequestResponseInformation, val)
            }
            ResponseInformation(val) => enc_prop!(data, ResponseInformation, val),
            ServerReference(val) => enc_prop!(data, ServerReference, val),
            ReasonString(val) => enc_prop!(data, ReasonString, val),
            ReceiveMaximum(val) => enc_prop!(data, ReceiveMaximum, val),
            TopicAliasMaximum(val) => enc_prop!(data, TopicAliasMaximum, val),
            TopicAlias(val) => enc_prop!(data, TopicAlias, val),
            MaximumQoS(val) => enc_prop!(data, MaximumQoS, u8::from(*val)),
            RetainAvailable(val) => enc_prop!(data, RetainAvailable, val),
            UserProp(val) => enc_prop!(data, UserProp, val),
            MaximumPacketSize(val) => enc_prop!(data, MaximumPacketSize, val),
            WildcardSubscriptionAvailable(val) => {
                enc_prop!(data, WildcardSubscriptionAvailable, val)
            }
            SubscriptionIdentifierAvailable(val) => {
                enc_prop!(data, SubscriptionIdentifierAvailable, val)
            }
            SharedSubscriptionAvailable(val) => {
                enc_prop!(data, SharedSubscriptionAvailable, val)
            }
        };

        Ok(Blob::Large { data })
    }
}

impl Property {
    pub fn to_property_type(&self) -> PropertyType {
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

/// Possible payload values for PayloadFormatIndicator property.
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadFormat {
    Binary = 0,
    Utf8 = 1,
}

impl fmt::Display for PayloadFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self {
            PayloadFormat::Binary => write!(f, "binary"),
            PayloadFormat::Utf8 => write!(f, "utf8"),
        }
    }
}

impl Default for PayloadFormat {
    fn default() -> PayloadFormat {
        PayloadFormat::Binary // default, when missing in CONNECT/PUBLISH messages
    }
}

impl TryFrom<u8> for PayloadFormat {
    type Error = Error;

    fn try_from(val: u8) -> Result<PayloadFormat> {
        match val {
            0 => Ok(PayloadFormat::Binary),
            1 => Ok(PayloadFormat::Utf8),
            _ => {
                err!(
                    MalformedPacket,
                    code: MalformedPacket,
                    "invalid payload format {}",
                    val
                )
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

impl PayloadFormat {
    pub fn is_binary(&self) -> bool {
        self == &PayloadFormat::Binary
    }

    pub fn is_utf8(&self) -> bool {
        self == &PayloadFormat::Utf8
    }
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
