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

#[derive(Clone, PartialEq, Debug)]
pub struct ConnectPayload {
    pub client_id: ClientID,
    pub will_properties: Option<WillProperties>,
    pub will_topic: Option<TopicName>,
    pub will_payload: Option<Vec<u8>>,
    pub user_name: Option<String>,
    pub password: Option<String>,
}

impl Packetize for Connect {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        use crate::dec_props;

        let (fh, mut n) = FixedHeader::decode(stream)?;
        fh.validate()?;

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

        let (properties, m) = dec_props!(ConnectProperties, stream, n)?;
        n += m;

        // payload

        let (client_id, m) = String::decode(advance(stream, n)?)?;
        n += m;

        let (will_properties, m) = match flags.is_will_flag() {
            true => dec_props!(WillProperties, stream, n)?,
            false => (None, 0),
        };
        n += m;

        let (will_topic, m) = match flags.is_will_flag() {
            true => {
                let (val, m) = TopicName::decode(advance(stream, n)?)?;
                (Some(val), m)
            }
            false => (None, 0),
        };
        n += m;

        let (will_payload, m) = match &will_properties {
            Some(will_properties) => {
                let (val, m) = Vec::<u8>::decode(advance(stream, n)?)?;
                if will_properties.payload_format_indicator.is_utf8() {
                    match std::str::from_utf8(&val) {
                        Err(err) => err!(
                            ProtocolError,
                            code: PayloadFormatInvalid,
                            cause: err,
                            "payload format invalid in will message"
                        )?,
                        _ => (),
                    }
                }
                (Some(val), m)
            }
            None => (None, 0),
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
        use crate::v5::{insert_fixed_header, PacketType};

        let mut data = Vec::with_capacity(64);
        data.extend_from_slice(self.protocol_name.encode()?.as_ref());
        data.extend_from_slice(u8::from(self.protocol_version).encode()?.as_ref());
        data.extend_from_slice((*self.flags).encode()?.as_ref());
        data.extend_from_slice(self.keep_alive.encode()?.as_ref());
        if let Some(properties) = &self.properties {
            data.extend_from_slice(properties.encode()?.as_ref());
        } else {
            data.extend_from_slice(VarU32(0).encode()?.as_ref());
        }

        // payload
        data.extend_from_slice((*self.payload.client_id).encode()?.as_ref());
        if let Some(will_properties) = &self.payload.will_properties {
            data.extend_from_slice(will_properties.encode()?.as_ref());
        } else {
            data.extend_from_slice(VarU32(0).encode()?.as_ref());
        }
        if let Some(will_topic) = &self.payload.will_topic {
            data.extend_from_slice(will_topic.encode()?.as_ref());
        }
        if let Some(will_payload) = &self.payload.will_payload {
            data.extend_from_slice(will_payload.encode()?.as_ref());
        }
        if let Some(user_name) = &self.payload.user_name {
            data.extend_from_slice(user_name.encode()?.as_ref());
        }
        if let Some(password) = &self.payload.password {
            data.extend_from_slice(password.encode()?.as_ref());
        }

        let fh = FixedHeader::new(PacketType::Connect, VarU32(data.len().try_into()?))?;
        data = insert_fixed_header(fh, data)?;

        Ok(Blob::Large { data })
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
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Vec<u8>>,
    pub user_properties: Vec<UserProperty>,
}

impl Packetize for ConnectProperties {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        let mut dups = [false; 256];
        let mut props = ConnectProperties::default();

        let (len, mut n) = VarU32::decode(stream)?;
        let limit = usize::try_from(*len)? + n;

        while n < limit {
            let (property, m) = Property::decode(advance(stream, n)?)?;
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
                Property::MaximumPacketSize(val) => {
                    props.max_packet_size = Some(val);
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
                _ => err!(
                    ProtocolError,
                    code: ProtocolError,
                    "{:?} found in will properties",
                    pt
                )?,
            }
        }

        Ok((props, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::{enc_prop, v5::insert_property_len};

        let mut data = Vec::with_capacity(64);

        enc_prop!(opt: data, SessionExpiryInterval, self.session_expiry_interval);
        enc_prop!(opt: data, ReceiveMaximum, self.receive_maximum);
        enc_prop!(opt: data, MaximumPacketSize, self.max_packet_size);
        enc_prop!(opt: data, TopicAliasMaximum, self.topic_alias_max);
        if let Some(val) = self.request_response_info {
            let val = util::bool_to_u8(val);
            enc_prop!(data, RequestResponseInformation, val);
        }
        if let Some(val) = self.request_problem_info {
            let val = util::bool_to_u8(val);
            enc_prop!(data, RequestProblemInformation, val);
        }
        enc_prop!(opt: data, AuthenticationMethod, &self.authentication_method);
        enc_prop!(opt: data, AuthenticationData, &self.authentication_method);

        for uprop in self.user_properties.iter() {
            enc_prop!(data, UserProp, uprop)
        }

        data = insert_property_len(data.len(), data)?;

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
        self.receive_maximum.unwrap_or(Self::RECEIVE_MAXIMUM)
    }

    pub fn topic_alias_maximum(&self) -> u16 {
        self.topic_alias_max.unwrap_or(Self::TOPIC_ALIAS_MAXIMUM)
    }

    pub fn request_response_info(&self) -> bool {
        self.request_response_info.unwrap_or(false)
    }

    pub fn request_problem_info(&self) -> bool {
        self.request_response_info.unwrap_or(true)
    }
}

#[derive(Clone, Default, PartialEq, Debug)]
pub struct WillProperties {
    pub will_delay_interval: Option<u32>,
    pub payload_format_indicator: PayloadFormat, // default=PayloadFormat::Binary
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<TopicName>,
    pub correlation_data: Option<Vec<u8>>,
    pub user_properties: Vec<UserProperty>,
}

impl Packetize for WillProperties {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        let mut dups = [false; 256];
        let mut wps = WillProperties::default();

        let (len, mut n) = VarU32::decode(stream)?;
        let limit = usize::try_from(*len)? + n;

        while n < limit {
            let (property, m) = Property::decode(advance(stream, n)?)?;
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
                _ => err!(
                    ProtocolError,
                    code: ProtocolError,
                    "{:?} found in will properties",
                    pt
                )?,
            }
        }

        Ok((wps, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::{enc_prop, v5::insert_property_len};

        let mut data = Vec::with_capacity(64);

        enc_prop!(opt: data, WillDelayInterval, self.will_delay_interval);
        if self.payload_format_indicator.is_utf8() {
            let val = u8::from(PayloadFormat::Utf8);
            enc_prop!(data, PayloadFormatIndicator, val);
        }
        enc_prop!(opt: data, MessageExpiryInterval, self.message_expiry_interval);
        enc_prop!(opt: data, ContentType, &self.content_type);
        enc_prop!(opt: data, ResponseTopic, &self.response_topic);
        enc_prop!(opt: data, CorrelationData, &self.correlation_data);

        for uprop in self.user_properties.iter() {
            enc_prop!(data, UserProp, uprop);
        }

        let data = insert_property_len(data.len(), data)?;

        Ok(Blob::Large { data })
    }
}

impl WillProperties {
    pub const WILL_DELAY_INTERVAL: u32 = 0;

    pub fn will_delay_interval(&self) -> u32 {
        self.will_delay_interval.unwrap_or(Self::WILL_DELAY_INTERVAL)
    }
}
