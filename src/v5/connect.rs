// TODO: After a network connection is established CONNECT must be the first packet.
// TODO: The Server MUST process a second CONNECT packet sent from a Client as a
//       Protocol Error and close the Network Connection
// TODO: Payload with missing client identifier.
// TODO: Test case for server unavailable.
// TODO: Test case for server busy.
// TODO: The Server MUST validate that the reserved flag in the CONNECT packet is set to 0
// TODO: The Server MAY validate that the Will Message is of the format indicated,
//       and if it is not send a CONNACK with the Reason Code of
//       0x99 (Payload format invalid) as described in section 4.13
// TODO: The Server MUST NOT send packets exceeding Maximum Packet Size in CONNECT msg.
//    ** Where a Packet is too large to send, the Server MUST discard it without sending
//       it and then behave as if it had completed sending that Application Message.
//    ** In the case of a Shared Subscription where the message is too large to send
//       to one or more of the Clients but other Clients can receive it, the Server
//       can choose either discard the message without sending the message to any of
//       the Clients, or to send the message to one of the Clients that can receive it.
// TODO: Protocol confirmance for Request Response Information.
// TODO: Protocol confirmance for Request Problem Information.
// TODO: Extended authentication Section 4.12
// TODO: If a Client sets an Authentication Method in the CONNECT, the Client MUST NOT
//       send any packets other than AUTH or DISCONNECT packets until it has received
//       a CONNACK packet

use std::ops::{Deref, DerefMut};

use crate::util::{self, advance};
use crate::v5::{FixedHeader, PayloadFormat, Property, PropertyType, QoS, UserProperty};
use crate::{Blob, ClientID, MqttProtocol, Packetize, TopicName, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

#[derive(Clone, PartialEq, Debug)]
pub struct ConnectFlags(pub u8);

impl Deref for ConnectFlags {
    type Target = u8;

    fn deref(&self) -> &u8 {
        &self.0
    }
}

impl DerefMut for ConnectFlags {
    fn deref_mut(&mut self) -> &mut u8 {
        &mut self.0
    }
}

impl ConnectFlags {
    pub const CLEAN_START: ConnectFlags = ConnectFlags(0b_0000_0010);
    pub const WILL_FLAG: ConnectFlags = ConnectFlags(0b_0000_0100);
    pub const WILL_QOS0: ConnectFlags = ConnectFlags(0b_0000_0000);
    pub const WILL_QOS1: ConnectFlags = ConnectFlags(0b_0000_1000);
    pub const WILL_QOS2: ConnectFlags = ConnectFlags(0b_0001_0000);
    pub const WILL_RETAIN: ConnectFlags = ConnectFlags(0b_0010_0000);
    pub const USERNAME: ConnectFlags = ConnectFlags(0b_0100_0000);
    pub const PASSWORD: ConnectFlags = ConnectFlags(0b_1000_0000);

    const QOS_MASK: u8 = 0b_0001_1000;

    pub fn new(flags: &[ConnectFlags]) -> ConnectFlags {
        flags.iter().fold(ConnectFlags(0), |acc, flag| ConnectFlags(acc.0 | flag.0))
    }

    pub fn is_clean_start(&self) -> bool {
        (self.0 & Self::CLEAN_START.0) > 0
    }

    pub fn is_will_flag(&self) -> bool {
        (self.0 & Self::WILL_FLAG.0) > 0
    }

    pub fn qos(&self) -> Result<QoS> {
        (self.0 & Self::QOS_MASK >> 3).try_into()
    }

    pub fn is_retain(&self) -> bool {
        (self.0 & Self::WILL_RETAIN.0) > 0
    }

    pub fn is_username(&self) -> bool {
        (self.0 & Self::USERNAME.0) > 0
    }

    pub fn is_password(&self) -> bool {
        (self.0 & Self::PASSWORD.0) > 0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Connect {
    pub protocol_name: String,
    pub protocol_version: MqttProtocol,
    pub flags: ConnectFlags,
    pub keep_alive: u16,
    pub properties: Option<ConnectProperties>,
    pub payload: ConnectPayload,
}

impl Packetize for Connect {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        let (_, mut n) = FixedHeader::decode(stream)?;

        let (protocol_name, m) = String::decode(advance(stream, n)?)?;
        n += m;
        if protocol_name != "MQTT" {
            err!(
                UnsupportedProtocolVersion,
                code: UnsupportedProtocolVersion,
                "invalid protocol name {:?}",
                protocol_name
            )?;
        }

        let (val, m) = u8::decode(advance(stream, n)?)?;
        let protocol_version = match MqttProtocol::try_from(val)? {
            MqttProtocol::V5 => MqttProtocol::V5,
            protocol_version => err!(
                UnsupportedProtocolVersion,
                code: UnsupportedProtocolVersion,
                "can't support {:?}",
                protocol_version
            )?,
        };
        n += m;

        let (flags, m) = {
            let (val, m) = u8::decode(advance(stream, n)?)?;
            (ConnectFlags(val), m)
        };
        n += m;
        if (*flags & 1) > 0 {
            err!(MalformedPacket, code: MalformedPacket, "connect-flags has reserved")?;
        }
        flags.qos()?; // validate qos

        let (keep_alive, m) = u16::decode(advance(stream, n)?)?;
        n += m;

        let (properties, m) = match VarU32::decode(advance(stream, n)?)? {
            (VarU32(0), m) => (None, m),
            _ => {
                let (p, m) = ConnectProperties::decode(advance(stream, n)?)?;
                (Some(p), m)
            }
        };
        n += m;

        let (client_id, m) = String::decode(advance(stream, n)?)?;
        n += m;

        let (will_properties, m) = match flags.is_will_flag() {
            true => {
                let (val, m) = WillProperties::decode(advance(stream, n)?)?;
                (Some(val), m)
            }
            false => (None, 0),
        };
        n += m;

        let (will_topic, m) = match flags.is_will_flag() {
            true => {
                let (val, m) = String::decode(advance(stream, n)?)?;
                (Some(TopicName::try_from(val)?), m)
            }
            false => (None, 0),
        };
        n += m;

        let (will_payload, m) = match flags.is_will_flag() {
            true => {
                let (val, m) = Vec::<u8>::decode(advance(stream, n)?)?;
                (Some(val), m)
            }
            false => (None, 0),
        };
        n += m;

        let (user_name, m) = match flags.is_username() {
            true => {
                let (val, m) = String::decode(advance(stream, n)?)?;
                (Some(val), m)
            }
            false => (None, 0),
        };
        n += m;

        let (password, m) = match flags.is_username() {
            true => {
                let (val, m) = String::decode(advance(stream, n)?)?;
                (Some(val), m)
            }
            false => (None, 0),
        };
        n += m;

        let val = Connect {
            protocol_name,
            protocol_version,
            flags,
            keep_alive,
            properties,
            payload: ConnectPayload {
                client_id: ClientID(client_id),
                will_properties,
                will_topic,
                will_payload,
                user_name,
                password,
            },
        };

        Ok((val, n))
    }

    fn encode(&self) -> Result<Blob> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ConnectProperties {
    pub session_expiry_interval: Option<u32>, // 0=disable, 0xFFFFFFFF=indefinite
    pub receive_maximum: Option<u16>,         // default=65535, can't be ZERO
    pub max_packet_size: Option<u32>,         // default=protocol-limit, can't be ZERO
    pub topic_alias_max: Option<u16>,         // default=0
    pub request_response_info: Option<bool>,
    pub request_problem_info: Option<bool>,
    pub user_properties: Vec<UserProperty>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Vec<u8>>,
}

impl Packetize for ConnectProperties {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        let mut dups = [false; 256];
        let mut props = ConnectProperties::default();

        let (count, mut n) = VarU32::decode(stream)?;

        for _i in 0..*count {
            let (property, m) = Property::decode(&stream[n..])?;
            n += m;

            let pt = property.to_property_type();
            if pt != PropertyType::UserProp && dups[pt as usize] {
                err!(ProtocolError, code: ProtocolError, "duplicate property {:?}", pt)?
            }
            dups[pt as usize] = true;

            match property {
                Property::SessionExpiryInterval(val) => {
                    props.session_expiry_interval = Some(val);
                }
                Property::ReceiveMaximum(val) if val == 0 => {
                    err!(ProtocolError, code: ProtocolError, "receive_maximum is ZERO")?;
                }
                Property::ReceiveMaximum(val) => {
                    props.receive_maximum = Some(val);
                }
                Property::MaximumPacketSize(val) if val == 0 => {
                    err!(ProtocolError, code: ProtocolError, "max_packet_size is ZERO")?;
                }
                Property::TopicAliasMaximum(val) => {
                    props.topic_alias_max = Some(val);
                }
                Property::RequestResponseInformation(val) => {
                    props.request_response_info =
                        Some(util::u8_to_bool(val, "request_response_info")?);
                }
                Property::RequestProblemInformation(val) => {
                    props.request_problem_info =
                        Some(util::u8_to_bool(val, "request_problem_info")?);
                }
                Property::UserProp(val) => {
                    props.user_properties.push(val);
                }
                Property::AuthenticationMethod(val) => {
                    props.authentication_method = Some(val);
                }
                Property::AuthenticationData(val)
                    if props.authentication_method.is_some() =>
                {
                    props.authentication_data = Some(val);
                }
                p => err!(
                    ProtocolError,
                    code: ProtocolError,
                    "{:?} found in will properties",
                    p.to_property_type()
                )?,
            }
        }

        Ok((props, n))
    }

    fn encode(&self) -> Result<Blob> {
        let mut data = Vec::with_capacity(64);
        if let Some(val) = &self.session_expiry_interval {
            data.extend_from_slice(
                Property::SessionExpiryInterval(*val).encode()?.as_ref(),
            );
        }
        if let Some(val) = &self.receive_maximum {
            data.extend_from_slice(Property::ReceiveMaximum(*val).encode()?.as_ref());
        }
        if let Some(val) = &self.max_packet_size {
            data.extend_from_slice(Property::MaximumPacketSize(*val).encode()?.as_ref());
        }
        if let Some(val) = &self.topic_alias_max {
            data.extend_from_slice(Property::TopicAliasMaximum(*val).encode()?.as_ref());
        }
        if let Some(val) = &self.request_response_info {
            data.extend_from_slice(
                Property::RequestResponseInformation(util::bool_to_u8(*val))
                    .encode()?
                    .as_ref(),
            );
        }
        if let Some(val) = &self.request_problem_info {
            data.extend_from_slice(
                Property::RequestProblemInformation(util::bool_to_u8(*val))
                    .encode()?
                    .as_ref(),
            );
        }
        if let Some(val) = self.authentication_method.clone() {
            data.extend_from_slice(
                Property::AuthenticationMethod(val).encode()?.as_ref(),
            );
        }
        if let Some(val) = self.authentication_data.clone() {
            data.extend_from_slice(Property::AuthenticationData(val).encode()?.as_ref());
        }

        for uprop in self.user_properties.iter() {
            data.extend_from_slice(Property::UserProp(uprop.clone()).encode()?.as_ref());
        }

        Ok(Blob::Large { data })
    }
}

impl ConnectProperties {
    pub const RECEIVE_MAXIMUM: u16 = 65535;
    pub const TOPIC_ALIAS_MAXIMUM: u16 = 0;

    pub fn session_expiry_interval(&self) -> u32 {
        self.session_expiry_interval.unwrap_or(0)
    }

    pub fn receive_maximum(&self) -> u16 {
        self.receive_maximum.clone().unwrap_or(Self::RECEIVE_MAXIMUM)
    }

    pub fn topic_alias_maximum(&self) -> u16 {
        self.topic_alias_max.clone().unwrap_or(Self::TOPIC_ALIAS_MAXIMUM)
    }

    pub fn request_response_info(&self) -> bool {
        self.request_response_info.clone().unwrap_or(false)
    }

    pub fn request_problem_info(&self) -> bool {
        self.request_response_info.clone().unwrap_or(true)
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ConnectPayload {
    pub client_id: ClientID,
    pub will_properties: Option<WillProperties>,
    pub will_topic: Option<TopicName>,
    pub will_payload: Option<Vec<u8>>,
    pub user_name: Option<String>,
    pub password: Option<String>,
}

#[derive(Clone, Default, PartialEq, Debug)]
pub struct WillProperties {
    pub will_delay_interval: Option<u32>,
    pub payload_format_indicator: PayloadFormat, // default=PayloadFormat::Binary
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
    pub user_properties: Vec<UserProperty>,
}

impl Packetize for WillProperties {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        let mut dups = [false; 256];
        let mut wps = WillProperties::default();

        let (count, mut n) = VarU32::decode(stream)?;

        for _i in 0..*count {
            let (property, m) = Property::decode(&stream[n..])?;
            n += m;

            let pt = property.to_property_type();
            if pt != PropertyType::UserProp && dups[pt as usize] {
                err!(ProtocolError, code: ProtocolError, "duplicate property {:?}", pt)?
            }
            dups[pt as usize] = true;

            match property {
                Property::WillDelayInterval(val) => {
                    wps.will_delay_interval = Some(val);
                }
                Property::PayloadFormatIndicator(val) => {
                    wps.payload_format_indicator = val.try_into()?;
                }
                Property::MessageExpiryInterval(val) => {
                    wps.message_expiry_interval = Some(val);
                }
                Property::ContentType(val) => {
                    wps.content_type = Some(val);
                }
                Property::ResponseTopic(val) => {
                    wps.response_topic = Some(val);
                }
                Property::CorrelationData(val) => {
                    wps.correlation_data = Some(val);
                }
                Property::UserProp(val) => {
                    wps.user_properties.push(val);
                }
                p => err!(
                    ProtocolError,
                    code: ProtocolError,
                    "{:?} found in will properties",
                    p.to_property_type()
                )?,
            }
        }

        Ok((wps, n))
    }

    fn encode(&self) -> Result<Blob> {
        let mut data = Vec::with_capacity(64);
        if let Some(val) = &self.will_delay_interval {
            data.extend_from_slice(Property::WillDelayInterval(*val).encode()?.as_ref());
        }
        if let PayloadFormat::Utf8 = self.payload_format_indicator {
            data.extend_from_slice(
                Property::PayloadFormatIndicator(PayloadFormat::Utf8.into())
                    .encode()?
                    .as_ref(),
            );
        }
        if let Some(val) = self.message_expiry_interval {
            data.extend_from_slice(
                Property::MessageExpiryInterval(val).encode()?.as_ref(),
            );
        }
        if let Some(val) = &self.content_type {
            data.extend_from_slice(Property::ContentType(val.clone()).encode()?.as_ref());
        }
        if let Some(val) = &self.response_topic {
            data.extend_from_slice(
                Property::ResponseTopic(val.clone()).encode()?.as_ref(),
            );
        }
        if let Some(val) = &self.correlation_data {
            data.extend_from_slice(
                Property::CorrelationData(val.clone()).encode()?.as_ref(),
            );
        }
        for uprop in self.user_properties.iter() {
            data.extend_from_slice(Property::UserProp(uprop.clone()).encode()?.as_ref());
        }

        Ok(Blob::Large { data })
    }
}

impl WillProperties {
    pub const WILL_DELAY_INTERVAL: u32 = 0;

    pub fn will_delay_interval(&self) -> u32 {
        self.will_delay_interval.clone().unwrap_or(Self::WILL_DELAY_INTERVAL)
    }
}
