#[cfg(any(feature = "fuzzy", test))]
use arbitrary::{Arbitrary, Error as ArbitraryError, Unstructured};

use std::ops::{Deref, DerefMut};
use std::{fmt, result};

use crate::v5::{FixedHeader, Property, PropertyType, UserProperty};
use crate::{Blob, Packetize, QoS, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

const PP: &'static str = "Packet::ConnAck";

/// Flags carried in CONNACK packet.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct ConnAckFlags(pub u8);

impl Deref for ConnAckFlags {
    type Target = u8;

    fn deref(&self) -> &u8 {
        &self.0
    }
}

impl DerefMut for ConnAckFlags {
    fn deref_mut(&mut self) -> &mut u8 {
        &mut self.0
    }
}

impl Default for ConnAckFlags {
    fn default() -> ConnAckFlags {
        ConnAckFlags(0)
    }
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for ConnAckFlags {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let mut flags = vec![];
        if uns.arbitrary::<bool>()? {
            flags.push(Self::SESSION_PRESENT)
        }

        Ok(ConnAckFlags::new(&flags))
    }
}

impl Packetize for ConnAckFlags {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (flags, n) = dec_field!(u8, stream, 0);
        let flags = ConnAckFlags(flags);
        flags.unwrap()?;

        flags.validate()?;
        Ok((flags, n))
    }

    fn encode(&self) -> Result<Blob> {
        self.unwrap()?;
        self.0.encode()
    }
}

impl ConnAckFlags {
    pub const SESSION_PRESENT: ConnAckFlags = ConnAckFlags(0b_0000_0001);

    pub fn new(flags: &[ConnAckFlags]) -> ConnAckFlags {
        flags.iter().fold(ConnAckFlags(0), |acc, flag| ConnAckFlags(acc.0 | flag.0))
    }

    /// Return `session_present` flag.
    pub fn unwrap(&self) -> Result<bool> {
        if (self.0 & 0b_1111_1110) > 0 {
            err!(MalformedPacket, code: MalformedPacket, "{} flags {:?}", PP, self.0)?;
        }

        Ok(self.0 & (*Self::SESSION_PRESENT) > 0)
    }

    fn validate(&self) -> Result<()> {
        self.unwrap()?;
        Ok(())
    }
}

/// Error codes allowed in CONNACK packet.
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum ConnAckReasonCode {
    Success = 0x00,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationError = 0x83,
    UnsupportedProtocolVersion = 0x84,
    InvalidClientID = 0x85,
    BadLogin = 0x86,
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
    InvalidQoS = 0x9b,
    UseAnotherServer = 0x9c,
    ServerMoved = 0x9d,
    ExceedConnectionRate = 0x9f,
}

impl fmt::Display for ConnAckReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        use ConnAckReasonCode::*;

        match self {
            Success => write!(f, "success"),
            UnspecifiedError => write!(f, "unspecified_error"),
            MalformedPacket => write!(f, "malformed_packet"),
            ProtocolError => write!(f, "protocol_error"),
            ImplementationError => write!(f, "implementation_error"),
            UnsupportedProtocolVersion => write!(f, "unsupported_protocolversion"),
            InvalidClientID => write!(f, "invalid_clientid"),
            BadLogin => write!(f, "bad_login"),
            NotAuthorized => write!(f, "not_authorized"),
            ServerUnavailable => write!(f, "server_unavailable"),
            ServerBusy => write!(f, "server_busy"),
            Banned => write!(f, "banned"),
            BadAuthenticationMethod => write!(f, "bad_authentication_method"),
            TopicNameInvalid => write!(f, "topicname_invalid"),
            PacketTooLarge => write!(f, "packet_toolarge"),
            QuotaExceeded => write!(f, "quota_exceeded"),
            PayloadFormatInvalid => write!(f, "payloadformat_invalid"),
            RetainNotSupported => write!(f, "retain_notsupported"),
            InvalidQoS => write!(f, "invalid_qos"),
            UseAnotherServer => write!(f, "use_anotherserver"),
            ServerMoved => write!(f, "server_moved"),
            ExceedConnectionRate => write!(f, "exceed_connectionrate"),
        }
    }
}

impl TryFrom<u8> for ConnAckReasonCode {
    type Error = Error;

    fn try_from(val: u8) -> Result<ConnAckReasonCode> {
        match val {
            0x00 => Ok(ConnAckReasonCode::Success),
            0x80 => Ok(ConnAckReasonCode::UnspecifiedError),
            0x81 => Ok(ConnAckReasonCode::MalformedPacket),
            0x82 => Ok(ConnAckReasonCode::ProtocolError),
            0x83 => Ok(ConnAckReasonCode::ImplementationError),
            0x84 => Ok(ConnAckReasonCode::UnsupportedProtocolVersion),
            0x85 => Ok(ConnAckReasonCode::InvalidClientID),
            0x86 => Ok(ConnAckReasonCode::BadLogin),
            0x87 => Ok(ConnAckReasonCode::NotAuthorized),
            0x88 => Ok(ConnAckReasonCode::ServerUnavailable),
            0x89 => Ok(ConnAckReasonCode::ServerBusy),
            0x8a => Ok(ConnAckReasonCode::Banned),
            0x8c => Ok(ConnAckReasonCode::BadAuthenticationMethod),
            0x90 => Ok(ConnAckReasonCode::TopicNameInvalid),
            0x95 => Ok(ConnAckReasonCode::PacketTooLarge),
            0x97 => Ok(ConnAckReasonCode::QuotaExceeded),
            0x99 => Ok(ConnAckReasonCode::PayloadFormatInvalid),
            0x9a => Ok(ConnAckReasonCode::RetainNotSupported),
            0x9b => Ok(ConnAckReasonCode::InvalidQoS),
            0x9c => Ok(ConnAckReasonCode::UseAnotherServer),
            0x9d => Ok(ConnAckReasonCode::ServerMoved),
            0x9f => Ok(ConnAckReasonCode::ExceedConnectionRate),
            val => {
                err!(MalformedPacket, code: MalformedPacket, "{} reason-code {}", PP, val)
            }
        }
    }
}

impl TryFrom<ReasonCode> for ConnAckReasonCode {
    type Error = Error;

    fn try_from(val: ReasonCode) -> Result<ConnAckReasonCode> {
        (val as u8).try_into()
    }
}

/// CONNACK packet
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConnAck {
    pub flags: ConnAckFlags,
    pub code: ConnAckReasonCode,
    pub properties: Option<ConnAckProperties>,
}

impl fmt::Display for ConnAck {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "CONNACK flags:{:2x} code:{}\n", *self.flags, self.code)?;

        if let Some(properties) = &self.properties {
            let mut props = Vec::default();
            if let Some(val) = properties.session_expiry_interval {
                props.push(format!("  session_expiry_interval: {}", val));
            }
            if let Some(val) = properties.receive_maximum {
                props.push(format!("  receive_maximum: {}", val));
            }
            if let Some(val) = properties.maximum_qos {
                props.push(format!("  maximum_qos: {}", val));
            }
            if let Some(val) = properties.retain_available {
                props.push(format!("  retain_available: {}", val));
            }
            if let Some(val) = properties.max_packet_size {
                props.push(format!("  max_packet_size: {}", val));
            }
            if let Some(val) = &properties.assigned_client_identifier {
                props.push(format!("  assigned_client_identifier: {:?}", val));
            }
            if let Some(val) = properties.topic_alias_max {
                props.push(format!("  topic_alias_max: {}", val));
            }
            if let Some(val) = &properties.reason_string {
                props.push(format!("  reason_string: {:?}", val));
            }
            if let Some(val) = properties.wildcard_subscription_available {
                props.push(format!("  wildcard_subscription_available: {}", val));
            }
            if let Some(val) = properties.subscription_identifiers_available {
                props.push(format!("  subscription_identifiers_available: {}", val));
            }
            if let Some(val) = properties.shared_subscription_available {
                props.push(format!("  shared_subscription_available: {}", val));
            }
            if let Some(val) = properties.server_keep_alive {
                props.push(format!("  server_keep_alive: {}", val));
            }
            if let Some(val) = &properties.response_information {
                props.push(format!("  response_information: {:?}", val));
            }
            if let Some(val) = &properties.server_reference {
                props.push(format!("  server_reference: {:?}", val));
            }
            if let Some(val) = &properties.authentication_method {
                props.push(format!("  authentication_method: {:?}", val));
            }
            if let Some(val) = &properties.authentication_data {
                props.push(format!("  authentication_data: {}", val.len()));
            }
            for (key, val) in properties.user_properties.iter() {
                props.push(format!("  {:?}: {:?}", key, val));
            }
            write!(f, "{}\n", props.join("\n"))?;
        }

        Ok(())
    }
}

impl Default for ConnAck {
    fn default() -> ConnAck {
        ConnAck {
            flags: ConnAckFlags::default(),
            code: ConnAckReasonCode::Success,
            properties: None,
        }
    }
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for ConnAck {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let val = ConnAck {
            flags: uns.arbitrary()?,
            code: uns.arbitrary()?,
            properties: uns.arbitrary()?,
        };

        Ok(val)
    }
}

impl Packetize for ConnAck {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (fh, n) = dec_field!(FixedHeader, stream, 0);
        fh.validate()?;

        let (flags, n) = dec_field!(ConnAckFlags, stream, n);
        let (code, n) = dec_field!(u8, stream, n);
        let code = ConnAckReasonCode::try_from(code)?;
        let (properties, n) = dec_props!(ConnAckProperties, stream, n);

        let val = ConnAck { flags, code, properties };

        val.validate()?;
        Ok((val, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::{v5::insert_fixed_header, PacketType};

        let mut data = Vec::with_capacity(64);

        data.extend_from_slice((*self.flags).encode()?.as_ref());
        data.extend_from_slice((self.code as u8).encode()?.as_ref());
        if let Some(properties) = &self.properties {
            data.extend_from_slice(properties.encode()?.as_ref());
        } else {
            data.extend_from_slice(VarU32(0).encode()?.as_ref());
        }

        let fh = FixedHeader::new(PacketType::ConnAck, VarU32(data.len().try_into()?))?;
        data = insert_fixed_header(fh, data)?;

        Ok(Blob::Large { data })
    }
}

impl ConnAck {
    pub fn new_success(props: ConnAckProperties) -> ConnAck {
        ConnAck {
            flags: ConnAckFlags::default(),
            code: ConnAckReasonCode::Success,
            properties: Some(props),
        }
    }

    pub fn from_reason_code(code: ConnAckReasonCode) -> ConnAck {
        let flags = ConnAckFlags::default();
        ConnAck { flags, code, properties: None }
    }

    pub fn set_session_present(&mut self, session_present: bool) {
        self.flags = if session_present {
            ConnAckFlags(*self.flags | *ConnAckFlags::SESSION_PRESENT)
        } else {
            ConnAckFlags(*self.flags & !(*ConnAckFlags::SESSION_PRESENT))
        }
    }

    pub fn receive_maximum(&self) -> u16 {
        match &self.properties {
            Some(props) => props.receive_maximum(),
            None => ConnAckProperties::RECEIVE_MAXIMUM,
        }
    }

    #[cfg(any(feature = "fuzzy", test))]
    pub fn normalize(&mut self) {
        if let Some(props) = &mut self.properties {
            if props.is_empty() {
                self.properties = None
            }
        }
    }

    pub fn validate(&self) -> Result<()> {
        self.flags.validate()?;
        Ok(())
    }
}

/// Collection of MQTT properties allowed in CONNACK packet
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct ConnAckProperties {
    pub session_expiry_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_qos: Option<QoS>,
    pub retain_available: Option<bool>,
    pub max_packet_size: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub topic_alias_max: Option<u16>,
    pub reason_string: Option<String>,
    pub wildcard_subscription_available: Option<bool>,
    pub subscription_identifiers_available: Option<bool>,
    pub shared_subscription_available: Option<bool>,
    pub server_keep_alive: Option<u16>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Vec<u8>>,
    pub user_properties: Vec<UserProperty>,
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for ConnAckProperties {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        use crate::v5::valid_user_props;

        let str_choice: Vec<String> =
            vec!["", "unit-testing"].into_iter().map(|s| s.to_string()).collect();

        let assigned_client_identifier = match uns.arbitrary::<u8>()? % 2 {
            0 => Some(uns.choose(&str_choice)?.to_string()),
            1 => None,
            _ => unreachable!(),
        };
        let reason_string = match uns.arbitrary::<u8>()? % 2 {
            0 => Some(uns.choose(&str_choice)?.to_string()),
            1 => None,
            _ => unreachable!(),
        };
        let response_information = match uns.arbitrary::<u8>()? % 2 {
            0 => Some(uns.choose(&str_choice)?.to_string()),
            1 => None,
            _ => unreachable!(),
        };
        let server_reference = match uns.arbitrary::<u8>()? % 2 {
            0 => Some(uns.choose(&str_choice)?.to_string()),
            1 => None,
            _ => unreachable!(),
        };
        let authentication_method = match uns.arbitrary::<u8>()? % 2 {
            0 => Some(uns.choose(&str_choice)?.to_string()),
            1 => None,
            _ => unreachable!(),
        };

        let n_user_props = uns.arbitrary::<usize>()? % 4;
        let val = ConnAckProperties {
            session_expiry_interval: uns.arbitrary()?,
            receive_maximum: uns.arbitrary::<Option<u16>>()?.map(|x| x.saturating_add(1)),
            maximum_qos: uns.arbitrary()?,
            retain_available: uns.arbitrary()?,
            max_packet_size: uns.arbitrary()?,
            assigned_client_identifier,
            topic_alias_max: uns.arbitrary()?,
            reason_string,
            wildcard_subscription_available: uns.arbitrary()?,
            subscription_identifiers_available: uns.arbitrary()?,
            shared_subscription_available: uns.arbitrary()?,
            server_keep_alive: uns.arbitrary()?,
            response_information,
            server_reference,
            authentication_method,
            authentication_data: uns.arbitrary()?,
            user_properties: valid_user_props(uns, n_user_props)?,
        };

        Ok(val)
    }
}

impl Packetize for ConnAckProperties {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        use crate::v5::Property::*;

        let stream: &[u8] = stream.as_ref();

        let mut dups = [false; 256];
        let mut props = ConnAckProperties::default();

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
                MaximumQoS(val) => props.maximum_qos = Some(val),
                RetainAvailable(0) => props.retain_available = Some(false),
                RetainAvailable(1) => props.retain_available = Some(true),
                RetainAvailable(val) => err!(
                    ProtocolError,
                    code: ProtocolError,
                    "retain-available invalid {:?}",
                    val
                )?,
                MaximumPacketSize(0) => {
                    err!(ProtocolError, code: ProtocolError, "{} max_packet_size:0", PP)?;
                }
                MaximumPacketSize(val) => props.max_packet_size = Some(val),
                AssignedClientIdentifier(val) => {
                    props.assigned_client_identifier = Some(val);
                }
                TopicAliasMaximum(val) => props.topic_alias_max = Some(val),
                ReasonString(val) => props.reason_string = Some(val),
                UserProp(val) => props.user_properties.push(val),
                WildcardSubscriptionAvailable(0) => {
                    props.wildcard_subscription_available = Some(false);
                }
                WildcardSubscriptionAvailable(1) => {
                    props.wildcard_subscription_available = Some(true);
                }
                WildcardSubscriptionAvailable(val) => err!(
                    ProtocolError,
                    code: ProtocolError,
                    "wildcard-subscription-available invalid {:?}",
                    val
                )?,
                SubscriptionIdentifierAvailable(0) => {
                    props.subscription_identifiers_available = Some(false);
                }
                SubscriptionIdentifierAvailable(1) => {
                    props.subscription_identifiers_available = Some(true);
                }
                SubscriptionIdentifierAvailable(val) => err!(
                    ProtocolError,
                    code: ProtocolError,
                    "subscription-identifier-available invalid {:?}",
                    val
                )?,
                SharedSubscriptionAvailable(0) => {
                    props.shared_subscription_available = Some(false);
                }
                SharedSubscriptionAvailable(1) => {
                    props.shared_subscription_available = Some(true);
                }
                SharedSubscriptionAvailable(val) => err!(
                    ProtocolError,
                    code: ProtocolError,
                    "shared-subscription-available invalid {:?}",
                    val
                )?,
                ServerKeepAlive(val) => props.server_keep_alive = Some(val),
                ResponseInformation(val) => props.response_information = Some(val),
                ServerReference(val) => props.server_reference = Some(val),
                AuthenticationMethod(val) => props.authentication_method = Some(val),
                AuthenticationData(val) => props.authentication_data = Some(val),
                _ => {
                    err!(ProtocolError, code: ProtocolError, "{} bad prop {:?}", PP, pt)?
                }
            };
        }

        Ok((props, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::v5::insert_property_len;

        let mut data = Vec::with_capacity(64);

        enc_prop!(opt: data, SessionExpiryInterval, self.session_expiry_interval);
        enc_prop!(opt: data, ReceiveMaximum, self.receive_maximum);
        match &self.maximum_qos {
            Some(val) => enc_prop!(data, MaximumQoS, u8::from(*val)),
            None => (),
        }
        if let Some(val) = self.retain_available {
            let val: u8 = if val { 1 } else { 0 };
            enc_prop!(data, RetainAvailable, val);
        }
        enc_prop!(opt: data, MaximumPacketSize, self.max_packet_size);
        enc_prop!(opt: data, AssignedClientIdentifier, &self.assigned_client_identifier);
        enc_prop!(opt: data, TopicAliasMaximum, self.topic_alias_max);
        enc_prop!(opt: data, ReasonString, &self.reason_string);
        if let Some(val) = self.wildcard_subscription_available {
            let val: u8 = if val { 1 } else { 0 };
            enc_prop!(data, WildcardSubscriptionAvailable, val);
        }
        if let Some(val) = self.subscription_identifiers_available {
            let val: u8 = if val { 1 } else { 0 };
            enc_prop!(data, SubscriptionIdentifierAvailable, val);
        }
        if let Some(val) = self.shared_subscription_available {
            let val: u8 = if val { 1 } else { 0 };
            enc_prop!(data, SharedSubscriptionAvailable, val);
        }
        enc_prop!(opt: data, ServerKeepAlive, self.server_keep_alive);
        enc_prop!(opt: data, ResponseInformation, &self.response_information);
        enc_prop!(opt: data, ServerReference, &self.server_reference);
        enc_prop!(opt: data, AuthenticationMethod, &self.authentication_method);
        enc_prop!(opt: data, AuthenticationData, &self.authentication_data);

        for uprop in self.user_properties.iter() {
            enc_prop!(data, UserProp, uprop)
        }

        let data = insert_property_len(data.len(), data)?;

        Ok(Blob::Large { data })
    }
}

impl ConnAckProperties {
    pub const RECEIVE_MAXIMUM: u16 = 65_535_u16;
    pub const MAXIMUM_QOS: QoS = QoS::ExactlyOnce;
    pub const TOPIC_ALIAS_MAXIMUM: u16 = 0_u16;

    /// Use this method to confirm to MQTT specification's default.
    /// DEFAULT: [Self::MAXIMUM_QOS]
    pub fn maximum_qos(&self) -> QoS {
        self.maximum_qos.unwrap_or(Self::MAXIMUM_QOS)
    }

    /// Use this method to confirm to MQTT specification's default.
    /// DEFAULT: [Self::RECEIVE_MAXIMUM]
    pub fn receive_maximum(&self) -> u16 {
        self.receive_maximum.unwrap_or(Self::RECEIVE_MAXIMUM)
    }

    /// Use this method to confirm to MQTT specification's default.
    /// DEFAULT: [Self::TOPIC_ALIAS_MAXIMUM]
    pub fn topic_alias_max(&self) -> u16 {
        self.topic_alias_max.unwrap_or(Self::TOPIC_ALIAS_MAXIMUM)
    }

    /// DEFAULT: true
    pub fn wildcard_subscription_available(&self) -> bool {
        self.wildcard_subscription_available.unwrap_or(true)
    }

    /// DEFAULT: true
    pub fn subscription_identifiers_available(&self) -> bool {
        self.subscription_identifiers_available.unwrap_or(true)
    }

    /// DEFAULT: true
    pub fn shared_subscription_available(&self) -> bool {
        self.shared_subscription_available.unwrap_or(true)
    }
}

impl ConnAckProperties {
    #[cfg(any(feature = "fuzzy", test))]
    pub fn is_empty(&self) -> bool {
        self.session_expiry_interval.is_none()
            && self.receive_maximum.is_none()
            && self.maximum_qos.is_none()
            && self.retain_available.is_none()
            && self.max_packet_size.is_none()
            && self.assigned_client_identifier.is_none()
            && self.topic_alias_max.is_none()
            && self.reason_string.is_none()
            && self.wildcard_subscription_available.is_none()
            && self.subscription_identifiers_available.is_none()
            && self.shared_subscription_available.is_none()
            && self.server_keep_alive.is_none()
            && self.response_information.is_none()
            && self.server_reference.is_none()
            && self.authentication_method.is_none()
            && self.authentication_data.is_none()
            && self.user_properties.len() == 0
    }
}
