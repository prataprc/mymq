use std::ops::{Deref, DerefMut};

use crate::util::{self, advance};
use crate::v5::{FixedHeader, Property, PropertyType, QoS};
use crate::{Blob, Packetize, UserProperty, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

#[derive(Clone, PartialEq, Debug)]
pub struct ConnackFlags(pub u8);

impl Deref for ConnackFlags {
    type Target = u8;

    fn deref(&self) -> &u8 {
        &self.0
    }
}

impl DerefMut for ConnackFlags {
    fn deref_mut(&mut self) -> &mut u8 {
        &mut self.0
    }
}

impl ConnackFlags {
    pub const SESSION_PRESENT: ConnackFlags = ConnackFlags(0b_0000_0001);

    pub fn new(flags: &[ConnackFlags]) -> ConnackFlags {
        flags.iter().fold(ConnackFlags(0), |acc, flag| ConnackFlags(acc.0 | flag.0))
    }

    pub fn is_session_present_flag(&self) -> bool {
        (self.0 & Self::SESSION_PRESENT.0) > 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectReasonCode {
    Success = 0x00,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationSpecificError = 0x83,
    UnsupportedProtocolVersion = 0x84,
    ClientIdentifierNotValid = 0x85,
    BadUserNamePassword = 0x86,
    NotAuthorized = 0x87,
    ServerUnavailable = 0x88,
    ServerBusy = 0x89,
    Banned = 0x8a,
    BadAuthenticationMethod = 0x8c,
    TopicNameInvalid = 0x90,
    PacketTooLarge = 0x95,
    QuotaExceeded = 0x97,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9a,
    QoSNotSupported = 0x9b,
    UseAnotherServer = 0x9c,
    ServerMoved = 0x9d,
    ConnectionRateExceeded = 0x94,
}

impl TryFrom<u8> for ConnectReasonCode {
    type Error = Error;

    fn try_from(val: u8) -> Result<ConnectReasonCode> {
        match val {
            0x00 => Ok(ConnectReasonCode::Success),
            0x80 => Ok(ConnectReasonCode::UnspecifiedError),
            0x81 => Ok(ConnectReasonCode::MalformedPacket),
            0x82 => Ok(ConnectReasonCode::ProtocolError),
            0x83 => Ok(ConnectReasonCode::ImplementationSpecificError),
            0x84 => Ok(ConnectReasonCode::UnsupportedProtocolVersion),
            0x85 => Ok(ConnectReasonCode::ClientIdentifierNotValid),
            0x86 => Ok(ConnectReasonCode::BadUserNamePassword),
            0x87 => Ok(ConnectReasonCode::NotAuthorized),
            0x88 => Ok(ConnectReasonCode::ServerUnavailable),
            0x89 => Ok(ConnectReasonCode::ServerBusy),
            0x8a => Ok(ConnectReasonCode::Banned),
            0x8c => Ok(ConnectReasonCode::BadAuthenticationMethod),
            0x90 => Ok(ConnectReasonCode::TopicNameInvalid),
            0x95 => Ok(ConnectReasonCode::PacketTooLarge),
            0x97 => Ok(ConnectReasonCode::QuotaExceeded),
            0x99 => Ok(ConnectReasonCode::PayloadFormatInvalid),
            0x9a => Ok(ConnectReasonCode::RetainNotSupported),
            0x9b => Ok(ConnectReasonCode::QoSNotSupported),
            0x9c => Ok(ConnectReasonCode::UseAnotherServer),
            0x9d => Ok(ConnectReasonCode::ServerMoved),
            0x94 => Ok(ConnectReasonCode::ConnectionRateExceeded),
            val => err!(ProtocolError, code: ProtocolError, "reason-code {:?}", val),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnAck {
    pub flags: ConnackFlags,
    pub code: ConnectReasonCode,
    pub properties: Option<ConnAckProperties>,
}

impl Packetize for ConnAck {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        let (_, mut n) = FixedHeader::decode(stream)?;

        let (flags, m) = {
            let (val, m) = u8::decode(advance(stream, n)?)?;
            (ConnackFlags(val), m)
        };
        n += m;

        let (code, m) = {
            let (val, m) = u8::decode(advance(stream, n)?)?;
            (ConnectReasonCode::try_from(val)?, m)
        };
        n += m;

        let (properties, m) = match VarU32::decode(advance(stream, n)?)? {
            (VarU32(0), m) => (None, m),
            _ => {
                let (properties, m) = ConnAckProperties::decode(advance(stream, n)?)?;
                (Some(properties), m)
            }
        };
        n += m;

        let val = ConnAck { flags, code, properties };
        Ok((val, n))
    }

    fn encode(&self) -> Result<Blob> {
        let mut data = Vec::with_capacity(64);
        data.extend_from_slice((*self.flags).encode()?.as_ref());
        data.extend_from_slice((self.code as u8).encode()?.as_ref());
        if let Some(properties) = &self.properties {
            data.extend_from_slice(properties.encode()?.as_ref());
        } else {
            data.extend_from_slice(VarU32(0).encode()?.as_ref());
        }

        Ok(Blob::Large { data })
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ConnAckProperties {
    pub session_expiry_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub max_qos: Option<QoS>,
    pub retain_available: Option<bool>,
    pub max_packet_size: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub topic_alias_maximum: Option<u16>,
    pub reason_string: Option<String>,
    pub user_properties: Vec<UserProperty>,
    pub wildcard_subscription_available: Option<bool>,
    pub subscription_identifiers_available: Option<bool>,
    pub shared_subscription_available: Option<bool>,
    pub server_keep_alive: Option<u16>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Vec<u8>>,
}

impl Packetize for ConnAckProperties {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        let mut dups = [false; 256];
        let mut props = ConnAckProperties::default();

        let (count, mut n) = VarU32::decode(stream)?;

        for _i in 0..*count {
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
                Property::ReceiveMaximum(val) => {
                    props.receive_maximum = Some(val);
                }
                Property::MaximumQoS(val) if val == QoS::ExactlyOnce => {
                    err!(ProtocolError, code: ProtocolError, "max_qos {:?}", val)?;
                }
                Property::MaximumQoS(val) => {
                    props.max_qos = Some(val);
                }
                Property::RetainAvailable(val) => {
                    props.retain_available =
                        Some(util::u8_to_bool(val, "retain_available")?);
                }
                Property::MaximumPacketSize(val) if val == 0 => {
                    err!(ProtocolError, code: ProtocolError, "max_packet_size is ZERO")?;
                }
                Property::MaximumPacketSize(val) => {
                    props.max_packet_size = Some(val);
                }
                Property::AssignedClientIdentifier(val) => {
                    props.assigned_client_identifier = Some(val);
                }
                Property::TopicAliasMaximum(val) => {
                    props.topic_alias_maximum = Some(val);
                }
                Property::ReasonString(val) => {
                    props.reason_string = Some(val);
                }
                Property::UserProp(val) => {
                    props.user_properties.push(val);
                }
                Property::WildcardSubscriptionAvailable(val) => {
                    props.wildcard_subscription_available =
                        Some(util::u8_to_bool(val, "wildcard_subscription_available")?);
                }
                Property::SubscriptionIdentifierAvailable(val) => {
                    props.subscription_identifiers_available = Some(util::u8_to_bool(
                        val,
                        "subscription_identifiers_available",
                    )?);
                }
                Property::SharedSubscriptionAvailable(val) => {
                    props.shared_subscription_available =
                        Some(util::u8_to_bool(val, "shared_subscription_available")?);
                }
                Property::ServerKeepAlive(val) => {
                    props.server_keep_alive = Some(val);
                }
                Property::ResponseInformation(val) => {
                    props.response_information = Some(val);
                }
                Property::ServerReference(val) => {
                    props.server_reference = Some(val);
                }
                Property::AuthenticationMethod(val) => {
                    props.authentication_method = Some(val);
                }
                Property::AuthenticationData(val) => {
                    props.authentication_data = Some(val);
                }
                p => err!(
                    ProtocolError,
                    code: ProtocolError,
                    "{:?} found in connack properties",
                    p.to_property_type()
                )?,
            };
        }

        Ok((props, n))
    }

    fn encode(&self) -> Result<Blob> {
        let mut data = Vec::with_capacity(64);

        data.extend_from_slice(self.count().encode()?.as_ref());

        if let Some(val) = &self.session_expiry_interval {
            data.extend_from_slice(
                Property::SessionExpiryInterval(*val).encode()?.as_ref(),
            );
        }
        if let Some(val) = &self.receive_maximum {
            data.extend_from_slice(Property::ReceiveMaximum(*val).encode()?.as_ref());
        }
        match &self.max_qos {
            Some(QoS::ExactlyOnce) => err!(InvalidInput, desc:"max_qos ExactlyOnce")?,
            Some(val) => data.extend_from_slice(
                Property::MaximumQoS((*val).into()).encode()?.as_ref(),
            ),
            None => (),
        }
        if let Some(val) = &self.retain_available {
            data.extend_from_slice(
                Property::RetainAvailable(util::bool_to_u8(*val)).encode()?.as_ref(),
            );
        }
        if let Some(val) = &self.max_packet_size {
            data.extend_from_slice(Property::MaximumPacketSize(*val).encode()?.as_ref());
        }
        if let Some(val) = &self.assigned_client_identifier {
            data.extend_from_slice(
                VarU32(PropertyType::AssignedClientIdentifier as u32).encode()?.as_ref(),
            );
            data.extend_from_slice(val.encode()?.as_ref());
        }
        if let Some(val) = &self.topic_alias_maximum {
            data.extend_from_slice(Property::TopicAliasMaximum(*val).encode()?.as_ref());
        }
        if let Some(val) = &self.reason_string {
            data.extend_from_slice(
                VarU32(PropertyType::ReasonString as u32).encode()?.as_ref(),
            );
            data.extend_from_slice(val.encode()?.as_ref());
        }
        if let Some(val) = &self.wildcard_subscription_available {
            data.extend_from_slice(
                Property::WildcardSubscriptionAvailable(util::bool_to_u8(*val))
                    .encode()?
                    .as_ref(),
            );
        }
        if let Some(val) = &self.subscription_identifiers_available {
            data.extend_from_slice(
                Property::SubscriptionIdentifierAvailable(util::bool_to_u8(*val))
                    .encode()?
                    .as_ref(),
            );
        }
        if let Some(val) = &self.shared_subscription_available {
            data.extend_from_slice(
                Property::SharedSubscriptionAvailable(util::bool_to_u8(*val))
                    .encode()?
                    .as_ref(),
            );
        }
        if let Some(val) = &self.server_keep_alive {
            data.extend_from_slice(Property::ServerKeepAlive(*val).encode()?.as_ref());
        }
        if let Some(val) = &self.response_information {
            data.extend_from_slice(
                VarU32(PropertyType::ResponseInformation as u32).encode()?.as_ref(),
            );
            data.extend_from_slice(val.encode()?.as_ref());
        }
        if let Some(val) = &self.server_reference {
            data.extend_from_slice(
                VarU32(PropertyType::ServerReference as u32).encode()?.as_ref(),
            );
            data.extend_from_slice(val.encode()?.as_ref());
        }
        if let Some(val) = &self.authentication_method {
            data.extend_from_slice(
                VarU32(PropertyType::AuthenticationMethod as u32).encode()?.as_ref(),
            );
            data.extend_from_slice(val.encode()?.as_ref());
        }
        if let Some(val) = &self.authentication_data {
            data.extend_from_slice(
                VarU32(PropertyType::AuthenticationData as u32).encode()?.as_ref(),
            );
            data.extend_from_slice(val.encode()?.as_ref());
        }
        for uprop in self.user_properties.iter() {
            data.extend_from_slice(
                VarU32(PropertyType::UserProp as u32).encode()?.as_ref(),
            );

            let (key, value) = uprop;
            data.extend_from_slice(key.encode()?.as_ref());
            data.extend_from_slice(value.encode()?.as_ref());
        }

        Ok(Blob::Large { data })
    }
}

impl ConnAckProperties {
    pub const RECEIVE_MAXIMUM: u16 = 65535;
    pub const MAXIMUM_QOS: QoS = QoS::ExactlyOnce;
    pub const TOPIC_ALIAS_MAXIMUM: u16 = 0;

    pub fn max_qos(&self) -> QoS {
        self.max_qos.unwrap_or(Self::MAXIMUM_QOS)
    }

    pub fn receive_maximum(&self) -> u16 {
        self.receive_maximum.unwrap_or(Self::RECEIVE_MAXIMUM)
    }

    pub fn topic_alias_maximum(&self) -> u16 {
        self.topic_alias_maximum.unwrap_or(Self::TOPIC_ALIAS_MAXIMUM)
    }

    pub fn wildcard_subscription_available(&self) -> bool {
        self.wildcard_subscription_available.unwrap_or(true)
    }

    pub fn subscription_identifiers_available(&self) -> bool {
        self.subscription_identifiers_available.unwrap_or(true)
    }

    pub fn shared_subscription_available(&self) -> bool {
        self.shared_subscription_available.unwrap_or(true)
    }

    fn count(&self) -> VarU32 {
        let n = if self.session_expiry_interval.is_some() { 1 } else { 0 }
            + if self.receive_maximum.is_some() { 0 } else { 1 }
            + if self.max_qos.is_some() { 1 } else { 0 }
            + if self.retain_available.is_some() { 1 } else { 0 }
            + if self.max_packet_size.is_some() { 1 } else { 0 }
            + if self.assigned_client_identifier.is_some() { 1 } else { 0 }
            + if self.topic_alias_maximum.is_some() { 1 } else { 0 }
            + if self.reason_string.is_some() { 1 } else { 0 }
            + self.user_properties.len() as u32
            + if self.wildcard_subscription_available.is_some() { 1 } else { 0 }
            + if self.subscription_identifiers_available.is_some() { 1 } else { 0 }
            + if self.shared_subscription_available.is_some() { 1 } else { 0 }
            + if self.server_keep_alive.is_some() { 1 } else { 0 }
            + if self.response_information.is_some() { 1 } else { 0 }
            + if self.server_reference.is_some() { 1 } else { 0 }
            + if self.authentication_method.is_some() { 1 } else { 0 }
            + if self.authentication_data.is_some() { 1 } else { 0 };

        VarU32(n)
    }
}
