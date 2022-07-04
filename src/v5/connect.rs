use std::ops::{Deref, DerefMut};

use crate::util::{self, advance};
use crate::v5::{FixedHeader, PayloadFormat, Property, PropertyType, QoS, UserProperty};
use crate::{Blob, ClientID, Config, MqttProtocol, Packetize, TopicName, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

const PP: &'static str = "Packet::Connect";

#[derive(Clone, Copy, PartialEq, Debug)]
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

impl Packetize for ConnectFlags {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (flags, n) = dec_field!(u8, stream, 0);
        let flags = ConnectFlags(flags);

        Ok((flags, n))
    }

    fn encode(&self) -> Result<Blob> {
        self.0.encode()
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

    const WILL_QOS_MASK: u8 = 0b_0001_1000;

    pub fn new(flags: &[ConnectFlags]) -> ConnectFlags {
        flags.iter().fold(ConnectFlags(0), |acc, flag| ConnectFlags(acc.0 | flag.0))
    }

    // return (clean_start, will_flag, will_qos, will_retain)
    pub fn unwrap(&self) -> (bool, bool, QoS, bool) {
        let clean_start: bool = (self.0 & Self::CLEAN_START.0) > 0;
        let will_flag: bool = (self.0 & Self::WILL_FLAG.0) > 0;
        let will_qos: QoS = (self.0 & Self::WILL_QOS_MASK >> 3).try_into().unwrap();
        let will_retain: bool = (self.0 & Self::WILL_RETAIN.0) > 0;

        (clean_start, will_flag, will_qos, will_retain)
    }

    pub fn is_will_flag(&self) -> bool {
        (self.0 & Self::WILL_FLAG.0) > 0
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
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (fh, n) = dec_field!(FixedHeader, stream, 0);
        fh.validate()?;

        let (protocol_name, n) = dec_field!(String, stream, n);
        let (protocol_version, n) = {
            let (val, n) = dec_field!(u8, stream, n);
            (MqttProtocol::try_from(val)?, n)
        };
        let (flags, n) = dec_field!(ConnectFlags, stream, n);
        let (keep_alive, n) = dec_field!(u16, stream, n);
        let (properties, n) = dec_props!(ConnectProperties, stream, n);
        let will_flag = flags.is_will_flag();

        // payload
        let (client_id, n) = dec_field!(String, stream, n);
        let (will_properties, n) = dec_props!(WillProperties, stream, n; will_flag);
        let (will_topic, n) = dec_field!(TopicName, stream, n; will_flag);
        let (will_payload, n) = dec_field!(Vec<u8>, stream, n; will_flag);
        let (user_name, n) = dec_field!(String, stream, n; flags.is_username());
        let (password, n) = dec_field!(String, stream, n; flags.is_password());

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
        val.validate()?;

        Ok((val, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::v5::{insert_fixed_header, PacketType};

        self.validate()?;

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

impl Connect {
    fn validate(&self) -> Result<()> {
        if self.protocol_name != "MQTT" {
            err!(
                ProtocolError,
                code: UnsupportedProtocolVersion,
                "{} proto-name {:?}",
                PP,
                self.protocol_name
            )?;
        }
        if self.protocol_version != MqttProtocol::V5 {
            err!(
                ProtocolError,
                code: UnsupportedProtocolVersion,
                "{} proto-version {:?}",
                PP,
                self.protocol_version
            )?;
        };

        let flags = *self.flags;
        if (flags & 0b_0000_0001) > 0 {
            err!(MalformedPacket, code: MalformedPacket, "{} flags {:0x?}", PP, flags)?;
        }
        QoS::try_from((flags & ConnectFlags::WILL_QOS_MASK) >> 3)?;
        if (flags & *ConnectFlags::WILL_FLAG) > 0 {
            // TODO: Wonder why the spec. says that properites and payload MUST be
            // specified
            if self.payload.will_topic.is_none() {
                err!(
                    MalformedPacket,
                    code: MalformedPacket,
                    "{} missing will-topic",
                    PP
                )?;
            } else if self.payload.will_properties.is_none() {
                err!(
                    MalformedPacket,
                    code: MalformedPacket,
                    "{} missing will-properties",
                    PP
                )?;
            } else if self.payload.will_payload.is_none() {
                err!(
                    MalformedPacket,
                    code: MalformedPacket,
                    "{} missing will-payload",
                    PP
                )?;
            }
        }

        let pld = &self.payload;
        if let Some(true) = pld.will_properties.as_ref().map(|p| p.is_utf8()) {
            if let Err(err) = std::str::from_utf8(pld.will_payload.as_ref().unwrap()) {
                err!(
                    MalformedPacket,
                    code: MalformedPacket,
                    cause: err,
                    "{} will-message:payload not utf8",
                    PP
                )?
            }
        }

        Ok(())
    }

    pub fn receive_maximum(&self) -> u16 {
        match &self.properties {
            Some(props) => props.receive_maximum(),
            None => ConnectProperties::RECEIVE_MAXIMUM,
        }
    }

    pub fn max_packet_size(&self) -> u32 {
        match &self.properties {
            Some(props) => props.max_packet_size(),
            None => Config::DEF_MQTT_MAX_PACKET_SIZE,
        }
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
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        use crate::v5::Property::*;

        let stream: &[u8] = stream.as_ref();

        let mut dups = [false; 256];
        let mut props = ConnectProperties::default();

        let (len, mut n) = dec_field!(VarU32, stream, 0);
        let limit = usize::try_from(*len)? + n;

        while n < limit {
            let (property, m) = dec_field!(Property, stream, n);
            n = m;

            let pt = property.to_property_type();
            if pt != PropertyType::UserProp && dups[pt as usize] {
                err!(ProtocolError, code: ProtocolError, "{} repeat prop {:?}", PP, pt)?
            }
            dups[pt as usize] = true;

            match property {
                SessionExpiryInterval(val) => props.session_expiry_interval = Some(val),
                ReceiveMaximum(0) => {
                    err!(ProtocolError, code: ProtocolError, "{} receive_maximum:0", PP)?;
                }
                ReceiveMaximum(val) => props.receive_maximum = Some(val),
                MaximumPacketSize(0) => {
                    err!(ProtocolError, code: ProtocolError, "{} max_packet_size:0", PP)?;
                }
                MaximumPacketSize(val) => props.max_packet_size = Some(val),
                TopicAliasMaximum(val) => props.topic_alias_max = Some(val),
                RequestResponseInformation(val) => {
                    props.request_response_info =
                        Some(util::u8_to_bool(val, "request_response_info")?);
                }
                RequestProblemInformation(val) => {
                    props.request_problem_info =
                        Some(util::u8_to_bool(val, "request_problem_info")?);
                }
                UserProp(val) => props.user_properties.push(val),
                AuthenticationMethod(val) => props.authentication_method = Some(val),
                AuthenticationData(val) => props.authentication_data = Some(val),
                _ => {
                    err!(ProtocolError, code: ProtocolError, "{} bad prop {:?}", PP, pt)?
                }
            }
        }

        Ok((props, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::v5::insert_property_len;

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

    pub fn max_packet_size(&self) -> u32 {
        self.max_packet_size.unwrap_or(Config::DEF_MQTT_MAX_PACKET_SIZE)
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
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        use crate::v5::Property::*;

        let stream: &[u8] = stream.as_ref();

        let mut dups = [false; 256];
        let mut wps = WillProperties::default();

        let (len, mut n) = dec_field!(VarU32, stream, 0);
        let limit = usize::try_from(*len)? + n;

        while n < limit {
            let (property, m) = dec_field!(Property, stream, n);
            n = m;

            let pt = property.to_property_type();
            if pt != PropertyType::UserProp && dups[pt as usize] {
                err!(
                    ProtocolError,
                    code: ProtocolError,
                    "{} repeat prop in will-message {:?}",
                    PP,
                    pt
                )?
            }
            dups[pt as usize] = true;

            match property {
                WillDelayInterval(val) => wps.will_delay_interval = Some(val),
                PayloadFormatIndicator(val) => {
                    wps.payload_format_indicator = val.try_into()?;
                }
                MessageExpiryInterval(val) => wps.message_expiry_interval = Some(val),
                ContentType(val) => wps.content_type = Some(val),
                ResponseTopic(val) => wps.response_topic = Some(val),
                CorrelationData(val) => wps.correlation_data = Some(val),
                UserProp(val) => wps.user_properties.push(val),
                _ => err!(
                    ProtocolError,
                    code: ProtocolError,
                    "{} bad prop in will-message {:?}",
                    PP,
                    pt
                )?,
            }
        }

        Ok((wps, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::v5::insert_property_len;

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

    pub fn is_utf8(&self) -> bool {
        self.payload_format_indicator == PayloadFormat::Utf8
    }
}
