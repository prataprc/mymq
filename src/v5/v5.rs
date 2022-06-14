// we are allowing dead code as we may not use both protocol at once
#![allow(dead_code)]

use std::{fmt, slice::Iter, str::Utf8Error};

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub(crate) mod suback {
    use std::convert::{TryFrom, TryInto};

    use super::*;
    use bytes::{Buf, Bytes};

    /// Acknowledgement to subscribe
    #[derive(Debug, Clone, PartialEq)]
    pub struct SubAck {
        pub pkid: u16,
        pub return_codes: Vec<SubscribeReasonCode>,
        pub properties: Option<SubAckProperties>,
    }

    impl SubAck {
        pub fn new(pkid: u16, return_codes: Vec<SubscribeReasonCode>) -> SubAck {
            SubAck { pkid, return_codes, properties: None }
        }

        pub fn len(&self) -> usize {
            let mut len = 2 + self.return_codes.len();

            match &self.properties {
                Some(properties) => {
                    let properties_len = properties.len();
                    let properties_len_len = len_len(properties_len);
                    len += properties_len_len + properties_len;
                }
                None => {
                    // just 1 byte representing 0 len
                    len += 1;
                }
            }

            len
        }

        pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Self, Error> {
            let variable_header_index = fixed_header.fixed_header_len;
            bytes.advance(variable_header_index);

            let pkid = read_u16(&mut bytes)?;
            let properties = SubAckProperties::extract(&mut bytes)?;

            if !bytes.has_remaining() {
                return Err(Error::MalformedPacket);
            }

            let mut return_codes = Vec::new();
            while bytes.has_remaining() {
                let return_code = read_u8(&mut bytes)?;
                return_codes.push(return_code.try_into()?);
            }

            let suback = SubAck { pkid, return_codes, properties };

            Ok(suback)
        }
    }

    pub fn write(
        return_codes: Vec<SubscribeReasonCode>,
        pkid: u16,
        properties: Option<SubAckProperties>,
        buffer: &mut BytesMut,
    ) -> Result<usize, Error> {
        buffer.put_u8(0x90);

        let mut len = 2 + return_codes.len();

        match &properties {
            Some(properties) => {
                let properties_len = properties.len();
                let properties_len_len = len_len(properties_len);
                len += properties_len_len + properties_len;
            }
            None => {
                // just 1 byte representing 0 len
                len += 1;
            }
        }

        let remaining_len = len;
        let remaining_len_bytes = write_remaining_length(buffer, remaining_len)?;

        buffer.put_u16(pkid);

        match &properties {
            Some(properties) => properties.write(buffer)?,
            None => {
                write_remaining_length(buffer, 0)?;
            }
        };

        let p: Vec<u8> = return_codes.iter().map(|code| *code as u8).collect();
        buffer.extend_from_slice(&p);
        Ok(1 + remaining_len_bytes + remaining_len)
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct SubAckProperties {
        pub reason_string: Option<String>,
        pub user_properties: Vec<(String, String)>,
    }

    impl SubAckProperties {
        pub fn len(&self) -> usize {
            let mut len = 0;

            if let Some(reason) = &self.reason_string {
                len += 1 + 2 + reason.len();
            }

            for (key, value) in self.user_properties.iter() {
                len += 1 + 2 + key.len() + 2 + value.len();
            }

            len
        }

        pub fn extract(mut bytes: &mut Bytes) -> Result<Option<SubAckProperties>, Error> {
            let mut reason_string = None;
            let mut user_properties = Vec::new();

            let (properties_len_len, properties_len) = length(bytes.iter())?;
            bytes.advance(properties_len_len);
            if properties_len == 0 {
                return Ok(None);
            }

            let mut cursor = 0;
            // read until cursor reaches property length. properties_len = 0 will skip this loop
            while cursor < properties_len {
                let prop = read_u8(&mut bytes)?;
                cursor += 1;

                match property(prop)? {
                    PropertyType::ReasonString => {
                        let bytes = read_mqtt_bytes(&mut bytes)?;
                        let reason = std::str::from_utf8(&bytes)?.to_owned();
                        cursor += 2 + reason.len();
                        reason_string = Some(reason);
                    }
                    PropertyType::UserProperty => {
                        let key = read_mqtt_bytes(&mut bytes)?;
                        let key = std::str::from_utf8(&key)?.to_owned();
                        let value = read_mqtt_bytes(&mut bytes)?;
                        let value = std::str::from_utf8(&value)?.to_owned();
                        cursor += 2 + key.len() + 2 + value.len();
                        user_properties.push((key, value));
                    }
                    _ => return Err(Error::InvalidPropertyType(prop)),
                }
            }

            Ok(Some(SubAckProperties { reason_string, user_properties }))
        }

        fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
            let len = self.len();
            write_remaining_length(buffer, len)?;

            if let Some(reason) = &self.reason_string {
                buffer.put_u8(PropertyType::ReasonString as u8);
                write_mqtt_string(buffer, reason);
            }

            for (key, value) in self.user_properties.iter() {
                buffer.put_u8(PropertyType::UserProperty as u8);
                write_mqtt_string(buffer, key);
                write_mqtt_string(buffer, value);
            }

            Ok(())
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum SubscribeReasonCode {
        QoS0 = 0,
        QoS1 = 1,
        QoS2 = 2,
        Unspecified = 128,
        ImplementationSpecific = 131,
        NotAuthorized = 135,
        TopicFilterInvalid = 143,
        PkidInUse = 145,
        QuotaExceeded = 151,
        SharedSubscriptionsNotSupported = 158,
        SubscriptionIdNotSupported = 161,
        WildcardSubscriptionsNotSupported = 162,
    }

    impl TryFrom<u8> for SubscribeReasonCode {
        type Error = Error;

        fn try_from(value: u8) -> Result<Self, Self::Error> {
            let v = match value {
                0 => SubscribeReasonCode::QoS0,
                1 => SubscribeReasonCode::QoS1,
                2 => SubscribeReasonCode::QoS2,
                128 => SubscribeReasonCode::Unspecified,
                131 => SubscribeReasonCode::ImplementationSpecific,
                135 => SubscribeReasonCode::NotAuthorized,
                143 => SubscribeReasonCode::TopicFilterInvalid,
                145 => SubscribeReasonCode::PkidInUse,
                151 => SubscribeReasonCode::QuotaExceeded,
                158 => SubscribeReasonCode::SharedSubscriptionsNotSupported,
                161 => SubscribeReasonCode::SubscriptionIdNotSupported,
                162 => SubscribeReasonCode::WildcardSubscriptionsNotSupported,
                v => return Err(Error::InvalidSubscribeReasonCode(v)),
            };

            Ok(v)
        }
    }

    pub fn codes(c: Vec<u8>) -> Vec<SubscribeReasonCode> {
        c.into_iter()
            .map(|v| match qos(v).unwrap() {
                QoS::AtMostOnce => SubscribeReasonCode::QoS0,
                QoS::AtLeastOnce => SubscribeReasonCode::QoS1,
            })
            .collect()
    }
}

pub(crate) mod pingresp {
    use super::*;

    pub fn write(payload: &mut BytesMut) -> Result<usize, Error> {
        payload.put_slice(&[0xD0, 0x00]);
        Ok(2)
    }
}

/// Reads a stream of bytes and extracts next MQTT packet out of it
pub fn read_mut(stream: &mut BytesMut, max_size: usize) -> Result<Packet, Error> {
    let fixed_header = check(stream.iter(), max_size)?;

    // Test with a stream with exactly the size to check border panics
    let packet = stream.split_to(fixed_header.frame_length());
    let packet_type = fixed_header.packet_type()?;

    if fixed_header.remaining_len == 0 {
        // no payload packets
        return match packet_type {
            PacketType::PingReq => Ok(Packet::PingReq),
            PacketType::PingResp => Ok(Packet::PingResp),
            PacketType::Disconnect => Ok(Packet::Disconnect),
            _ => Err(Error::PayloadRequired),
        };
    }

    let packet = packet.freeze();
    let packet = match packet_type {
        PacketType::Connect => {
            Packet::Connect(connect::Connect::read(fixed_header, packet)?)
        }
        PacketType::ConnAck => {
            Packet::ConnAck(connack::ConnAck::read(fixed_header, packet)?)
        }
        PacketType::Publish => {
            Packet::Publish(publish::Publish::read(fixed_header, packet)?)
        }
        PacketType::PubAck => Packet::PubAck(puback::PubAck::read(fixed_header, packet)?),
        PacketType::Subscribe => {
            Packet::Subscribe(subscribe::Subscribe::read(fixed_header, packet)?)
        }
        PacketType::SubAck => Packet::SubAck(suback::SubAck::read(fixed_header, packet)?),
        PacketType::PingReq => Packet::PingReq,
        PacketType::PingResp => Packet::PingResp,
        PacketType::Disconnect => Packet::Disconnect,
        v => return Err(Error::UnsupportedPacket(v)),
    };

    Ok(packet)
}

/// Reads a stream of bytes and extracts next MQTT packet out of it
pub fn read(stream: &mut Bytes, max_size: usize) -> Result<Packet, Error> {
    let fixed_header = check(stream.iter(), max_size)?;

    // Test with a stream with exactly the size to check border panics
    let packet = stream.split_to(fixed_header.frame_length());
    let packet_type = fixed_header.packet_type()?;

    if fixed_header.remaining_len == 0 {
        // no payload packets
        return match packet_type {
            PacketType::PingReq => Ok(Packet::PingReq),
            PacketType::PingResp => Ok(Packet::PingResp),
            PacketType::Disconnect => Ok(Packet::Disconnect),
            _ => Err(Error::PayloadRequired),
        };
    }

    let packet = match packet_type {
        PacketType::Connect => {
            Packet::Connect(connect::Connect::read(fixed_header, packet)?)
        }
        PacketType::ConnAck => {
            Packet::ConnAck(connack::ConnAck::read(fixed_header, packet)?)
        }
        PacketType::Publish => {
            Packet::Publish(publish::Publish::read(fixed_header, packet)?)
        }
        PacketType::PubAck => Packet::PubAck(puback::PubAck::read(fixed_header, packet)?),
        PacketType::Subscribe => {
            Packet::Subscribe(subscribe::Subscribe::read(fixed_header, packet)?)
        }
        PacketType::SubAck => Packet::SubAck(suback::SubAck::read(fixed_header, packet)?),
        PacketType::PingReq => Packet::PingReq,
        PacketType::PingResp => Packet::PingResp,
        PacketType::Disconnect => Packet::Disconnect,
        v => return Err(Error::UnsupportedPacket(v)),
    };

    Ok(packet)
}

/// MQTT packet type
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    Connect = 1,
    ConnAck,
    Publish,
    PubAck,
    PubRec,
    PubRel,
    PubComp,
    Subscribe,
    SubAck,
    Unsubscribe,
    UnsubAck,
    PingReq,
    PingResp,
    Disconnect,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Packet {
    Connect(connect::Connect),
    Publish(publish::Publish),
    ConnAck(connack::ConnAck),
    PubAck(puback::PubAck),
    PingReq,
    PingResp,
    Subscribe(subscribe::Subscribe),
    SubAck(suback::SubAck),
    Disconnect,
}

/// Quality of service
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
}

/// Maps a number to QoS
pub fn qos(num: u8) -> Result<QoS, Error> {
    match num {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        qos => Err(Error::InvalidQoS(qos)),
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
    /// First byte of the stream. Used to identify packet types and
    /// several flags
    pub byte1: u8,
    /// Length of fixed header. Byte 1 + (1..4) bytes. So fixed header
    /// len can vary from 2 bytes to 5 bytes
    /// 1..4 bytes are variable length encoded to represent remaining length
    pub fixed_header_len: usize,
    /// Remaining length of the packet. Doesn't include fixed header bytes
    /// Represents variable header + payload size
    pub remaining_len: usize,
}

impl FixedHeader {
    pub fn new(byte1: u8, remaining_len_len: usize, remaining_len: usize) -> FixedHeader {
        FixedHeader {
            byte1,
            fixed_header_len: remaining_len_len + 1,
            remaining_len,
        }
    }

    pub fn packet_type(&self) -> Result<PacketType, Error> {
        let num = self.byte1 >> 4;
        match num {
            1 => Ok(PacketType::Connect),
            2 => Ok(PacketType::ConnAck),
            3 => Ok(PacketType::Publish),
            4 => Ok(PacketType::PubAck),
            5 => Ok(PacketType::PubRec),
            6 => Ok(PacketType::PubRel),
            7 => Ok(PacketType::PubComp),
            8 => Ok(PacketType::Subscribe),
            9 => Ok(PacketType::SubAck),
            10 => Ok(PacketType::Unsubscribe),
            11 => Ok(PacketType::UnsubAck),
            12 => Ok(PacketType::PingReq),
            13 => Ok(PacketType::PingResp),
            14 => Ok(PacketType::Disconnect),
            _ => Err(Error::InvalidPacketType(num)),
        }
    }

    /// Returns the size of full packet (fixed header + variable header + payload)
    /// Fixed header is enough to get the size of a frame in the stream
    pub fn frame_length(&self) -> usize {
        self.fixed_header_len + self.remaining_len
    }
}

/// Checks if the stream has enough bytes to frame a packet and returns fixed header
/// only if a packet can be framed with existing bytes in the `stream`.
/// The passed stream doesn't modify parent stream's cursor. If this function
/// returned an error, next `check` on the same parent stream is forced start
/// with cursor at 0 again (Iter is owned. Only Iter's cursor is changed internally)
pub fn check(stream: Iter<u8>, max_packet_size: usize) -> Result<FixedHeader, Error> {
    // Create fixed header if there are enough bytes in the stream
    // to frame full packet
    let stream_len = stream.len();
    let fixed_header = parse_fixed_header(stream)?;

    // Don't let rogue connections attack with huge payloads.
    // Disconnect them before reading all that data
    if fixed_header.remaining_len > max_packet_size {
        return Err(Error::PayloadSizeLimitExceeded(fixed_header.remaining_len));
    }

    // If the current call fails due to insufficient bytes in the stream,
    // after calculating remaining length, we extend the stream
    let frame_length = fixed_header.frame_length();
    if stream_len < frame_length {
        return Err(Error::InsufficientBytes(frame_length - stream_len));
    }

    Ok(fixed_header)
}

/// Parses fixed header
fn parse_fixed_header(mut stream: Iter<u8>) -> Result<FixedHeader, Error> {
    // At least 2 bytes are necessary to frame a packet
    let stream_len = stream.len();
    if stream_len < 2 {
        return Err(Error::InsufficientBytes(2 - stream_len));
    }

    let byte1 = stream.next().unwrap();
    let (len_len, len) = length(stream)?;

    Ok(FixedHeader::new(*byte1, len_len, len))
}

/// Parses variable byte integer in the stream and returns the length
/// and number of bytes that make it. Used for remaining length calculation
/// as well as for calculating property lengths
pub fn length(stream: Iter<u8>) -> Result<(usize, usize), Error> {
    let mut len: usize = 0;
    let mut len_len = 0;
    let mut done = false;
    let mut shift = 0;

    // Use continuation bit at position 7 to continue reading next
    // byte to frame 'length'.
    // Stream 0b1xxx_xxxx 0b1yyy_yyyy 0b1zzz_zzzz 0b0www_wwww will
    // be framed as number 0bwww_wwww_zzz_zzzz_yyy_yyyy_xxx_xxxx
    for byte in stream {
        len_len += 1;
        let byte = *byte as usize;
        len += (byte & 0x7F) << shift;

        // stop when continue bit is 0
        done = (byte & 0x80) == 0;
        if done {
            break;
        }

        shift += 7;

        // Only a max of 4 bytes allowed for remaining length
        // more than 4 shifts (0, 7, 14, 21) implies bad length
        if shift > 21 {
            return Err(Error::MalformedRemainingLength);
        }
    }

    // Not enough bytes to frame remaining length. wait for
    // one more byte
    if !done {
        return Err(Error::InsufficientBytes(1));
    }

    Ok((len_len, len))
}

/// Returns big endian u16 view from next 2 bytes
pub fn view_u16(stream: &[u8]) -> Result<u16, Error> {
    let v = match stream.get(0..2) {
        Some(v) => (v[0] as u16) << 8 | (v[1] as u16),
        None => return Err(Error::MalformedPacket),
    };

    Ok(v)
}

/// Returns big endian u16 view from next 2 bytes
pub fn view_str(stream: &[u8], end: usize) -> Result<&str, Error> {
    let v = match stream.get(0..end) {
        Some(v) => v,
        None => return Err(Error::BoundaryCrossed(stream.len())),
    };

    let v = std::str::from_utf8(v)?;
    Ok(v)
}

/// After collecting enough bytes to frame a packet (packet's frame())
/// , It's possible that content itself in the stream is wrong. Like expected
/// packet id or qos not being present. In cases where `read_mqtt_string` or
/// `read_mqtt_bytes` exhausted remaining length but packet framing expects to
/// parse qos next, these pre checks will prevent `bytes` crashes

fn read_u32(stream: &mut Bytes) -> Result<u32, Error> {
    if stream.len() < 4 {
        return Err(Error::MalformedPacket);
    }

    Ok(stream.get_u32())
}

pub fn read_u16(stream: &mut Bytes) -> Result<u16, Error> {
    if stream.len() < 2 {
        return Err(Error::MalformedPacket);
    }

    Ok(stream.get_u16())
}

fn read_u8(stream: &mut Bytes) -> Result<u8, Error> {
    if stream.len() < 1 {
        return Err(Error::MalformedPacket);
    }

    Ok(stream.get_u8())
}

/// Reads a series of bytes with a length from a byte stream
fn read_mqtt_bytes(stream: &mut Bytes) -> Result<Bytes, Error> {
    let len = read_u16(stream)? as usize;

    // Prevent attacks with wrong remaining length. This method is used in
    // `packet.assembly()` with (enough) bytes to frame packet. Ensures that
    // reading variable len string or bytes doesn't cross promised boundary
    // with `read_fixed_header()`
    if len > stream.len() {
        return Err(Error::BoundaryCrossed(len));
    }

    Ok(stream.split_to(len))
}

/// Serializes bytes to stream (including length)
fn write_mqtt_bytes(stream: &mut BytesMut, bytes: &[u8]) {
    stream.put_u16(bytes.len() as u16);
    stream.extend_from_slice(bytes);
}

/// Serializes a string to stream
pub fn write_mqtt_string(stream: &mut BytesMut, string: &str) {
    write_mqtt_bytes(stream, string.as_bytes());
}

/// Writes remaining length to stream and returns number of bytes for remaining length
pub fn write_remaining_length(stream: &mut BytesMut, len: usize) -> Result<usize, Error> {
    if len > 268_435_455 {
        return Err(Error::PayloadTooLong);
    }

    let mut done = false;
    let mut x = len;
    let mut count = 0;

    while !done {
        let mut byte = (x % 128) as u8;
        x /= 128;
        if x > 0 {
            byte |= 128;
        }

        stream.put_u8(byte);
        count += 1;
        done = x == 0;
    }

    Ok(count)
}

/// Return number of remaining length bytes required for encoding length
fn len_len(len: usize) -> usize {
    if len >= 2_097_152 {
        4
    } else if len >= 16_384 {
        3
    } else if len >= 128 {
        2
    } else {
        1
    }
}

/// Error during serialization and deserialization
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("...")]
    NotConnect(PacketType),
    #[error("...")]
    UnexpectedConnect,
    #[error("...")]
    InvalidConnectReturnCode(u8),
    #[error("...")]
    InvalidReason(u8),
    #[error("...")]
    InvalidProtocol,
    #[error("...")]
    InvalidProtocolLevel(u8),
    #[error("...")]
    IncorrectPacketFormat,
    #[error("...")]
    InvalidPacketType(u8),
    #[error("...")]
    UnsupportedPacket(PacketType),
    #[error("...")]
    InvalidRetainForwardRule(u8),
    #[error("...")]
    InvalidQoS(u8),
    #[error("...")]
    InvalidSubscribeReasonCode(u8),
    #[error("...")]
    PacketIdZero,
    #[error("...")]
    SubscriptionIdZero,
    #[error("...")]
    PayloadSizeIncorrect,
    #[error("...")]
    PayloadTooLong,
    #[error("...")]
    PayloadSizeLimitExceeded(usize),
    #[error("...")]
    PayloadRequired,
    #[error("Topic not utf-8 = {0}")]
    TopicNotUtf8(#[from] Utf8Error),
    #[error("...")]
    BoundaryCrossed(usize),
    #[error("...")]
    MalformedPacket,
    #[error("...")]
    MalformedRemainingLength,
    /// More bytes required to frame packet. Argument
    /// implies minimum additional bytes required to
    /// proceed further
    #[error("...")]
    InsufficientBytes(usize),
    #[error("...")]
    InvalidPropertyType(u8),
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PropertyType {
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
    MaximumQos = 36,
    RetainAvailable = 37,
    UserProperty = 38,
    MaximumPacketSize = 39,
    WildcardSubscriptionAvailable = 40,
    SubscriptionIdentifierAvailable = 41,
    SharedSubscriptionAvailable = 42,
}

fn property(num: u8) -> Result<PropertyType, Error> {
    let property = match num {
        1 => PropertyType::PayloadFormatIndicator,
        2 => PropertyType::MessageExpiryInterval,
        3 => PropertyType::ContentType,
        8 => PropertyType::ResponseTopic,
        9 => PropertyType::CorrelationData,
        11 => PropertyType::SubscriptionIdentifier,
        17 => PropertyType::SessionExpiryInterval,
        18 => PropertyType::AssignedClientIdentifier,
        19 => PropertyType::ServerKeepAlive,
        21 => PropertyType::AuthenticationMethod,
        22 => PropertyType::AuthenticationData,
        23 => PropertyType::RequestProblemInformation,
        24 => PropertyType::WillDelayInterval,
        25 => PropertyType::RequestResponseInformation,
        26 => PropertyType::ResponseInformation,
        28 => PropertyType::ServerReference,
        31 => PropertyType::ReasonString,
        33 => PropertyType::ReceiveMaximum,
        34 => PropertyType::TopicAliasMaximum,
        35 => PropertyType::TopicAlias,
        36 => PropertyType::MaximumQos,
        37 => PropertyType::RetainAvailable,
        38 => PropertyType::UserProperty,
        39 => PropertyType::MaximumPacketSize,
        40 => PropertyType::WildcardSubscriptionAvailable,
        41 => PropertyType::SubscriptionIdentifierAvailable,
        42 => PropertyType::SharedSubscriptionAvailable,
        num => return Err(Error::InvalidPropertyType(num)),
    };

    Ok(property)
}
