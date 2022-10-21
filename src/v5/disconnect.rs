#[cfg(any(feature = "fuzzy", test))]
use arbitrary::{Arbitrary, Error as ArbitraryError, Unstructured};

use std::{fmt, result};

use crate::v5::{FixedHeader, Property, PropertyType, UserProperty};
use crate::{Blob, Packetize, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

const PP: &'static str = "Packet::Disconnect";

/// Error codes allowed in DISCONNECT packet.
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum DisconnReasonCode {
    NormalDisconnect = 0x00,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationError = 0x83,
    NotAuthorized = 0x87,
    ServerBusy = 0x89,
    ServerShutdown = 0x8B,
    KeepAliveTimeout = 0x8D,
    SessionTakenOver = 0x8E,
    TopicFilterInvalid = 0x8F,
    TopicNameInvalid = 0x90,
    ExceededReceiveMaximum = 0x93,
    TopicAliasInvalid = 0x94,
    PacketTooLarge = 0x95,
    ExceedMessageRate = 0x96,
    QuotaExceeded = 0x97,
    AdminAction = 0x98,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9A,
    QoSNotSupported = 0x9B,
    UseAnotherServer = 0x9C,
    ServerMoved = 0x9D,
    UnsupportedSharedSubscription = 0x9E,
    ExceedConnectionRate = 0x9F,
    ExceedMaximumConnectTime = 0xA0,
    SubscriptionIdNotSupported = 0xA1,
    WildcardSubscriptionsNotSupported = 0xA2,
}

impl fmt::Display for DisconnReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        use DisconnReasonCode::*;

        match self {
            NormalDisconnect => write!(f, "normal_disconnect"),
            UnspecifiedError => write!(f, "unspecified_error"),
            MalformedPacket => write!(f, "malformed_packet"),
            ProtocolError => write!(f, "protocol_error"),
            ImplementationError => write!(f, "implementation_error"),
            NotAuthorized => write!(f, "not_authorized"),
            ServerBusy => write!(f, "server_busy"),
            ServerShutdown => write!(f, "server_shutdown"),
            KeepAliveTimeout => write!(f, "keepalive_timeout"),
            SessionTakenOver => write!(f, "session_takenover"),
            TopicFilterInvalid => write!(f, "invalid_topicfilter"),
            TopicNameInvalid => write!(f, "topicname_invalid"),
            ExceededReceiveMaximum => write!(f, "exceeded_receive_maximum"),
            TopicAliasInvalid => write!(f, "topicalias_invalid"),
            PacketTooLarge => write!(f, "packet_toolarge"),
            ExceedMessageRate => write!(f, "exceed_messagerate"),
            QuotaExceeded => write!(f, "quota_exceeded"),
            AdminAction => write!(f, "admin_action"),
            PayloadFormatInvalid => write!(f, "payload_format_invalid"),
            RetainNotSupported => write!(f, "retain_notsupported"),
            QoSNotSupported => write!(f, "qos_notsupported"),
            UseAnotherServer => write!(f, "use_anotherserver"),
            ServerMoved => write!(f, "server_moved"),
            UnsupportedSharedSubscription => write!(f, "unsupported_sharedsubscription"),
            ExceedConnectionRate => write!(f, "exceed_connectionrate"),
            ExceedMaximumConnectTime => write!(f, "exceed_maximumconnecttime"),
            SubscriptionIdNotSupported => write!(f, "subscriptionid_notsupported"),
            WildcardSubscriptionsNotSupported => {
                write!(f, "wildcardsubscriptions_notsupported")
            }
        }
    }
}

impl TryFrom<u8> for DisconnReasonCode {
    type Error = Error;

    fn try_from(val: u8) -> Result<DisconnReasonCode> {
        match val {
            0x00 => Ok(DisconnReasonCode::NormalDisconnect),
            0x80 => Ok(DisconnReasonCode::UnspecifiedError),
            0x81 => Ok(DisconnReasonCode::MalformedPacket),
            0x82 => Ok(DisconnReasonCode::ProtocolError),
            0x83 => Ok(DisconnReasonCode::ImplementationError),
            0x87 => Ok(DisconnReasonCode::NotAuthorized),
            0x89 => Ok(DisconnReasonCode::ServerBusy),
            0x8B => Ok(DisconnReasonCode::ServerShutdown),
            0x8D => Ok(DisconnReasonCode::KeepAliveTimeout),
            0x8E => Ok(DisconnReasonCode::SessionTakenOver),
            0x8F => Ok(DisconnReasonCode::TopicFilterInvalid),
            0x90 => Ok(DisconnReasonCode::TopicNameInvalid),
            0x93 => Ok(DisconnReasonCode::ExceededReceiveMaximum),
            0x94 => Ok(DisconnReasonCode::TopicAliasInvalid),
            0x95 => Ok(DisconnReasonCode::PacketTooLarge),
            0x96 => Ok(DisconnReasonCode::ExceedMessageRate),
            0x97 => Ok(DisconnReasonCode::QuotaExceeded),
            0x98 => Ok(DisconnReasonCode::AdminAction),
            0x99 => Ok(DisconnReasonCode::PayloadFormatInvalid),
            0x9A => Ok(DisconnReasonCode::RetainNotSupported),
            0x9B => Ok(DisconnReasonCode::QoSNotSupported),
            0x9C => Ok(DisconnReasonCode::UseAnotherServer),
            0x9D => Ok(DisconnReasonCode::ServerMoved),
            0x9E => Ok(DisconnReasonCode::UnsupportedSharedSubscription),
            0x9F => Ok(DisconnReasonCode::ExceedConnectionRate),
            0xA0 => Ok(DisconnReasonCode::ExceedMaximumConnectTime),
            0xA1 => Ok(DisconnReasonCode::SubscriptionIdNotSupported),
            0xA2 => Ok(DisconnReasonCode::WildcardSubscriptionsNotSupported),
            val => {
                err!(
                    MalformedPacket,
                    code: MalformedPacket,
                    " {} reason-code {}",
                    PP,
                    val
                )
            }
        }
    }
}

impl TryFrom<ReasonCode> for DisconnReasonCode {
    type Error = Error;

    fn try_from(val: ReasonCode) -> Result<DisconnReasonCode> {
        DisconnReasonCode::try_from(val as u8)
    }
}

impl TryFrom<DisconnReasonCode> for ReasonCode {
    type Error = Error;

    fn try_from(val: DisconnReasonCode) -> Result<ReasonCode> {
        ReasonCode::try_from(val as u8)
    }
}

/// DISCONNECT Packet
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Disconnect {
    pub code: ReasonCode,
    pub properties: Option<DisconnProperties>,
}

impl fmt::Display for Disconnect {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "DISCONNECT code:{}", self.code)?;
        if let Some(properties) = &self.properties {
            let mut props = Vec::default();
            if let Some(val) = properties.session_expiry_interval {
                props.push(format!("  session_expiry_interval: {}", val));
            }
            if let Some(val) = &properties.reason_string {
                props.push(format!("  reason_string: {:?}", val));
            }
            if let Some(val) = &properties.server_reference {
                props.push(format!("  server_reference: {:?}", val));
            }
            for (key, val) in properties.user_properties.iter() {
                props.push(format!("  {:?}: {:?}", key, val));
            }
            write!(f, "{}\n", props.join("\n"))?;
        }

        Ok(())
    }
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for Disconnect {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let code: DisconnReasonCode = uns.arbitrary()?;
        let val = Disconnect {
            code: ReasonCode::try_from(code as u8).unwrap(),
            properties: uns.arbitrary()?,
        };

        Ok(val)
    }
}

impl Disconnect {
    #[cfg(any(feature = "fuzzy", test))]
    pub fn normalize(&mut self) {
        if let Some(props) = &mut self.properties {
            if props.is_empty() {
                self.properties = None
            }
        }
    }

    pub fn is_publish_will_message(&self) -> bool {
        if (self.code as u8) == 0x4 {
            true
        } else {
            false
        }
    }

    pub fn reason_string(&self) -> Option<String> {
        self.properties.as_ref()?.reason_string.clone()
    }

    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

impl Packetize for Disconnect {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        // println!("Disconnect decode {:?}", stream);

        let (fh, n) = dec_field!(FixedHeader, stream, 0);
        fh.validate()?;

        let (val, n) = match *fh.remaining_len {
            0 => {
                let code = ReasonCode::Success;
                (Disconnect { code, properties: None }, n)
            }
            m if m < 2 => {
                let (code, n) = dec_field!(u8, stream, n);
                let code = ReasonCode::try_from(code)?;
                (Disconnect { code, properties: None }, n)
            }
            _ => {
                let (code, n) = dec_field!(u8, stream, n);
                let code = ReasonCode::try_from(code)?;
                let (properties, n) = dec_props!(DisconnProperties, stream, n);
                (Disconnect { code, properties }, n)
            }
        };

        val.validate()?;
        Ok((val, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::{v5::insert_fixed_header, PacketType::Disconnect};

        let mut data = Vec::with_capacity(64);

        data.extend_from_slice((self.code as u8).encode()?.as_ref());
        if let Some(properties) = &self.properties {
            data.extend_from_slice(properties.encode()?.as_ref());
        } else {
            data.extend_from_slice(VarU32(0).encode()?.as_ref());
        }

        let fh = FixedHeader::new(Disconnect, VarU32(data.len().try_into()?))?;
        data = insert_fixed_header(fh, data)?;

        // println!("Disconnect encoded {:?}", data);

        Ok(Blob::Large { data })
    }
}

/// Collection of MQTT properties allowed in DISCONNECT packet
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct DisconnProperties {
    pub session_expiry_interval: Option<u32>,
    pub reason_string: Option<String>,
    pub server_reference: Option<String>,
    pub user_properties: Vec<UserProperty>,
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for DisconnProperties {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        use crate::v5::valid_user_props;

        let rs_choice: Vec<String> =
            vec!["", "unit-testing"].into_iter().map(|s| s.to_string()).collect();
        let reason_string = match uns.arbitrary::<u8>()? % 2 {
            0 => Some(uns.choose(&rs_choice)?.to_string()),
            1 => None,
            _ => unreachable!(),
        };
        let server_reference = match uns.arbitrary::<u8>()? % 3 {
            0 => Some("".to_string()),
            1 => Some("a.b.com:1883".to_string()),
            2 => None,
            _ => unreachable!(),
        };

        let n_user_props = uns.arbitrary::<usize>()? % 4;
        let val = DisconnProperties {
            session_expiry_interval: uns.arbitrary()?,
            reason_string,
            user_properties: valid_user_props(uns, n_user_props)?,
            server_reference,
        };

        Ok(val)
    }
}

impl Packetize for DisconnProperties {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        use crate::v5::Property::*;

        let stream: &[u8] = stream.as_ref();

        let mut dups = [false; 256];
        let mut props = DisconnProperties::default();

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
                ReasonString(val) => props.reason_string = Some(val),
                ServerReference(val) => props.server_reference = Some(val),
                UserProp(val) => props.user_properties.push(val),
                _ => {
                    err!(ProtocolError, code: ProtocolError, "{} bad prop, {:?}", PP, pt)?
                }
            };
        }

        Ok((props, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::v5::insert_property_len;

        let mut data = Vec::with_capacity(64);

        enc_prop!(opt: data, SessionExpiryInterval, self.session_expiry_interval);
        enc_prop!(opt: data, ReasonString, &self.reason_string);
        enc_prop!(opt: data, ServerReference, &self.server_reference);

        for uprop in self.user_properties.iter() {
            enc_prop!(data, UserProp, uprop)
        }

        let data = insert_property_len(data.len(), data)?;

        Ok(Blob::Large { data })
    }
}

impl DisconnProperties {
    #[cfg(any(feature = "fuzzy", test))]
    pub fn is_empty(&self) -> bool {
        self.session_expiry_interval.is_none()
            && self.reason_string.is_none()
            && self.user_properties.len() == 0
            && self.server_reference.is_none()
    }
}
