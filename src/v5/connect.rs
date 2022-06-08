// TODO: After a network connection is established CONNECT must be the first packet.
// TODO: The Server MUST process a second CONNECT packet sent from a Client as a
//       Protocol Error and close the Network Connection
// TODO: Payload with missing client identifier.
// TODO: Test case for server unavailable.
// TODO: Test case for server busy.
// TODO: The Server MUST validate that the reserved flag in the CONNECT packet is set to 0

use std::ops::{Deref, DerefMut};

use crate::v5::{QoS, UserProperty};
use crate::{Blob, ClientID, MqttProtocol, Packetize, Result, TopicName};

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectProperties {
    pub session_expiry_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub max_packet_size: Option<u32>,
    pub topic_alias_max: Option<u16>,
    pub request_response_info: Option<u8>,
    pub request_problem_info: Option<u8>,
    pub user_properties: Vec<UserProperty>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectPayload {
    pub client_id: ClientID,
    pub will_properties: Option<WillProperties>,
    pub topic: TopicName,
    pub payload: Vec<u8>,
    pub user_name: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WillProperties {
    pub will_delay_interval: Option<u32>,
    pub payload_format_indicator: u8,
    pub message_expiry_interval: Option<u32>,
    pub content_type: String,
    pub response_topic: String,
    pub correlation_data: Vec<u8>,
    pub user_properties: Vec<UserProperty>,
}

impl WillProperties {
    const WILL_DELAY_INTERVAL: u32 = 0;

    fn will_delay_interval(&self) -> u32 {
        self.will_delay_interval
            .as_ref()
            .map(|v| *v)
            .unwrap_or(Self::WILL_DELAY_INTERVAL)
    }
}

impl Packetize for WillProperties {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        todo!()
    }

    fn encode(&self) -> Result<Blob> {
        todo!()
    }
}

impl Connect {
    //pub fn new<S: Into<String>>(id: S) -> Connect {
    //    Connect {
    //        keep_alive: 10,
    //        client_id: id.into(),
    //        clean_session: true,
    //        last_will: None,
    //        login: None,
    //        properties: None,
    //    }
    //}
}

//impl Packetize for Connect {
//    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
//        let (fh, mut n) = FixedHeader::decode(stream)?;
//
//        let (proto_name, m) = String::decode(stream[n..]);
//        n += m;
//
//        if &proto_name != "MQTT" {
//            err!(
//                UnsupportedProtocolVersion,
//                code: UnsupportedProtocolVersion,
//                "protocol is {}",
//                proto_name
//            )?
//        }
//
//        let version = stream[n];
//        n += 1;
//
//        if version != 5 {
//            err!(
//                UnsupportedProtocolVersion,
//                code: UnsupportedProtocolVersion,
//                "protocol version {}",
//                version
//            )?
//        }
//    }
//
//    fn encode(&self) -> Result<Blob> {
//        todo!()
//    }
//
//    pub fn read(fixed_header: FixedHeader, mut bytes: Bytes) -> Result<Connect, Error> {
//        let variable_header_index = fixed_header.fixed_header_len;
//        bytes.advance(variable_header_index);
//
//        // Variable header
//        let protocol_name = read_mqtt_bytes(&mut bytes)?;
//        let protocol_name = std::str::from_utf8(&protocol_name)?.to_owned();
//        if protocol_name != "MQTT" {
//            return Err(Error::InvalidProtocol);
//        }
//
//        let protocol_level = read_u8(&mut bytes)?;
//        if protocol_level != 5 {
//            return Err(Error::InvalidProtocolLevel(protocol_level));
//        }
//
//        let connect_flags = read_u8(&mut bytes)?;
//        let clean_session = (connect_flags & 0b10) != 0;
//        let keep_alive = read_u16(&mut bytes)?;
//
//        let properties = ConnectProperties::read(&mut bytes)?;
//
//        // Payload
//        let client_id = read_mqtt_bytes(&mut bytes)?;
//        let client_id = std::str::from_utf8(&client_id)?.to_owned();
//        let last_will = LastWill::read(connect_flags, &mut bytes)?;
//        let login = Login::read(connect_flags, &mut bytes)?;
//
//        let connect = Connect {
//            keep_alive,
//            client_id,
//            clean_session,
//            last_will,
//            login,
//            properties,
//        };
//
//        Ok(connect)
//    }
//}

//impl LastWill {
//    pub fn _new(
//        topic: impl Into<String>,
//        payload: impl Into<Vec<u8>>,
//        qos: QoS,
//        retain: bool,
//    ) -> LastWill {
//        LastWill {
//            topic: topic.into(),
//            message: Bytes::from(payload.into()),
//            qos,
//            retain,
//        }
//    }
//
//    fn len(&self) -> usize {
//        let mut len = 0;
//        len += 2 + self.topic.len() + 2 + self.message.len();
//        len
//    }
//
//    fn read(connect_flags: u8, mut bytes: &mut Bytes) -> Result<Option<LastWill>, Error> {
//        let last_will = match connect_flags & 0b100 {
//            0 if (connect_flags & 0b0011_1000) != 0 => {
//                return Err(Error::IncorrectPacketFormat);
//            }
//            0 => None,
//            _ => {
//                let will_topic = read_mqtt_bytes(&mut bytes)?;
//                let will_topic = std::str::from_utf8(&will_topic)?.to_owned();
//                let will_message = read_mqtt_bytes(&mut bytes)?;
//                let will_qos = qos((connect_flags & 0b11000) >> 3)?;
//                Some(LastWill {
//                    topic: will_topic,
//                    message: will_message,
//                    qos: will_qos,
//                    retain: (connect_flags & 0b0010_0000) != 0,
//                })
//            }
//        };
//
//        Ok(last_will)
//    }
//}

//impl Login {
//    pub fn new<S: Into<String>>(u: S, p: S) -> Login {
//        Login { username: u.into(), password: p.into() }
//    }
//
//    fn read(connect_flags: u8, mut bytes: &mut Bytes) -> Result<Option<Login>, Error> {
//        let username = match connect_flags & 0b1000_0000 {
//            0 => String::new(),
//            _ => {
//                let username = read_mqtt_bytes(&mut bytes)?;
//                std::str::from_utf8(&username)?.to_owned()
//            }
//        };
//
//        let password = match connect_flags & 0b0100_0000 {
//            0 => String::new(),
//            _ => {
//                let password = read_mqtt_bytes(&mut bytes)?;
//                std::str::from_utf8(&password)?.to_owned()
//            }
//        };
//
//        if username.is_empty() && password.is_empty() {
//            Ok(None)
//        } else {
//            Ok(Some(Login { username, password }))
//        }
//    }
//
//    fn len(&self) -> usize {
//        let mut len = 0;
//
//        if !self.username.is_empty() {
//            len += 2 + self.username.len();
//        }
//
//        if !self.password.is_empty() {
//            len += 2 + self.password.len();
//        }
//
//        len
//    }
//}

//impl ConnectProperties {
//    fn _new() -> ConnectProperties {
//        ConnectProperties {
//            session_expiry_interval: None,
//            receive_maximum: None,
//            max_packet_size: None,
//            topic_alias_max: None,
//            request_response_info: None,
//            request_problem_info: None,
//            user_properties: Vec::new(),
//            authentication_method: None,
//            authentication_data: None,
//        }
//    }
//
//    fn read(mut bytes: &mut Bytes) -> Result<Option<ConnectProperties>, Error> {
//        let mut session_expiry_interval = None;
//        let mut receive_maximum = None;
//        let mut max_packet_size = None;
//        let mut topic_alias_max = None;
//        let mut request_response_info = None;
//        let mut request_problem_info = None;
//        let mut user_properties = Vec::new();
//        let mut authentication_method = None;
//        let mut authentication_data = None;
//
//        let (properties_len_len, properties_len) = length(bytes.iter())?;
//        bytes.advance(properties_len_len);
//        if properties_len == 0 {
//            return Ok(None);
//        }
//
//        let mut cursor = 0;
//        // read until cursor reaches property length. properties_len = 0 will skip this loop
//        while cursor < properties_len {
//            let prop = read_u8(&mut bytes)?;
//            cursor += 1;
//            match property(prop)? {
//                PropertyType::SessionExpiryInterval => {
//                    session_expiry_interval = Some(read_u32(&mut bytes)?);
//                    cursor += 4;
//                }
//                PropertyType::ReceiveMaximum => {
//                    receive_maximum = Some(read_u16(&mut bytes)?);
//                    cursor += 2;
//                }
//                PropertyType::MaximumPacketSize => {
//                    max_packet_size = Some(read_u32(&mut bytes)?);
//                    cursor += 4;
//                }
//                PropertyType::TopicAliasMaximum => {
//                    topic_alias_max = Some(read_u16(&mut bytes)?);
//                    cursor += 2;
//                }
//                PropertyType::RequestResponseInformation => {
//                    request_response_info = Some(read_u8(&mut bytes)?);
//                    cursor += 1;
//                }
//                PropertyType::RequestProblemInformation => {
//                    request_problem_info = Some(read_u8(&mut bytes)?);
//                    cursor += 1;
//                }
//                PropertyType::UserProperty => {
//                    let key = read_mqtt_bytes(&mut bytes)?;
//                    let key = std::str::from_utf8(&key)?.to_owned();
//                    let value = read_mqtt_bytes(&mut bytes)?;
//                    let value = std::str::from_utf8(&value)?.to_owned();
//                    cursor += 2 + key.len() + 2 + value.len();
//                    user_properties.push((key, value));
//                }
//                PropertyType::AuthenticationMethod => {
//                    let method = read_mqtt_bytes(&mut bytes)?;
//                    let method = std::str::from_utf8(&method)?.to_owned();
//                    cursor += 2 + method.len();
//                    authentication_method = Some(method);
//                }
//                PropertyType::AuthenticationData => {
//                    let data = read_mqtt_bytes(&mut bytes)?;
//                    cursor += 2 + data.len();
//                    authentication_data = Some(data);
//                }
//                _ => return Err(Error::InvalidPropertyType(prop)),
//            }
//        }
//
//        Ok(Some(ConnectProperties {
//            session_expiry_interval,
//            receive_maximum,
//            max_packet_size,
//            topic_alias_max,
//            request_response_info,
//            request_problem_info,
//            user_properties,
//            authentication_method,
//            authentication_data,
//        }))
//    }
//
//    fn len(&self) -> usize {
//        let mut len = 0;
//
//        if self.session_expiry_interval.is_some() {
//            len += 1 + 4;
//        }
//
//        if self.receive_maximum.is_some() {
//            len += 1 + 2;
//        }
//
//        if self.max_packet_size.is_some() {
//            len += 1 + 4;
//        }
//
//        if self.topic_alias_max.is_some() {
//            len += 1 + 2;
//        }
//
//        if self.request_response_info.is_some() {
//            len += 1 + 1;
//        }
//
//        if self.request_problem_info.is_some() {
//            len += 1 + 1;
//        }
//
//        for (key, value) in self.user_properties.iter() {
//            len += 1 + 2 + key.len() + 2 + value.len();
//        }
//
//        if let Some(authentication_method) = &self.authentication_method {
//            len += 1 + 2 + authentication_method.len();
//        }
//
//        if let Some(authentication_data) = &self.authentication_data {
//            len += 1 + 2 + authentication_data.len();
//        }
//
//        len
//    }
//}
