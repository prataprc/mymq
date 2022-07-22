#[cfg(any(feature = "fuzzy", test))]
use arbitrary::{Arbitrary, Error as ArbitraryError, Unstructured};

#[cfg(any(feature = "fuzzy", test))]
use std::result;

use crate::util::advance;
use crate::v5::{FixedHeader, Property, PropertyType};
use crate::{Blob, Packetize, UserProperty, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

const PP: &'static str = "Packet::Disconnect";

/// Error codes allowed in DISCONNECT packet.
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum DisconnReasonCode {
    NormalDisconnect = 0x00,
    DiconnectWillMessage = 0x04,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationError = 0x83,
    NotAuthorized = 0x87,
    ServerBusy = 0x89,
    ServerShutdown = 0x8B,
    KeepAliveTimeout = 0x8D,
    SessionTakenOver = 0x8E,
    InvalidTopicFilter = 0x8F,
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

impl TryFrom<u8> for DisconnReasonCode {
    type Error = Error;

    fn try_from(val: u8) -> Result<DisconnReasonCode> {
        match val {
            0x00 => Ok(DisconnReasonCode::NormalDisconnect),
            0x04 => Ok(DisconnReasonCode::DiconnectWillMessage),
            0x80 => Ok(DisconnReasonCode::UnspecifiedError),
            0x81 => Ok(DisconnReasonCode::MalformedPacket),
            0x82 => Ok(DisconnReasonCode::ProtocolError),
            0x83 => Ok(DisconnReasonCode::ImplementationError),
            0x87 => Ok(DisconnReasonCode::NotAuthorized),
            0x89 => Ok(DisconnReasonCode::ServerBusy),
            0x8B => Ok(DisconnReasonCode::ServerShutdown),
            0x8D => Ok(DisconnReasonCode::KeepAliveTimeout),
            0x8E => Ok(DisconnReasonCode::SessionTakenOver),
            0x8F => Ok(DisconnReasonCode::InvalidTopicFilter),
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

/// DISCONNECT Packet
#[derive(Clone, PartialEq, Debug)]
pub struct Disconnect {
    pub code: DisconnReasonCode,
    pub properties: Option<DisconnProperties>,
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for Disconnect {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let val = Disconnect {
            code: uns.arbitrary()?,
            properties: uns.arbitrary()?,
        };

        Ok(val)
    }
}

impl Disconnect {
    pub fn new(code: DisconnReasonCode, props: Option<DisconnProperties>) -> Disconnect {
        Disconnect { code, properties: props }
    }

    #[cfg(any(feature = "fuzzy", test))]
    pub fn normalize(&mut self) {
        if let Some(props) = &mut self.properties {
            if props.is_empty() {
                self.properties = None
            }
        }
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
                let code = DisconnReasonCode::NormalDisconnect;
                (Disconnect { code, properties: None }, n)
            }
            m if m < 2 => {
                let (code, n) = dec_field!(u8, stream, n);
                let code = DisconnReasonCode::try_from(code)?;
                (Disconnect { code, properties: None }, n)
            }
            _ => {
                let (code, n) = dec_field!(u8, stream, n);
                let code = DisconnReasonCode::try_from(code)?;
                let (properties, n) = dec_props!(DisconnProperties, stream, n);
                (Disconnect { code, properties }, n)
            }
        };

        val.validate()?;
        Ok((val, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::v5::{insert_fixed_header, PacketType::Disconnect};

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

impl Disconnect {
    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

/// Collection of MQTT properties allowed in DISCONNECT packet
#[derive(Clone, PartialEq, Debug, Default)]
pub struct DisconnProperties {
    pub session_expiry_interval: Option<u32>,
    pub reason_string: Option<String>,
    pub user_properties: Vec<UserProperty>,
    pub server_reference: Option<String>,
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for DisconnProperties {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        use crate::types;

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
            user_properties: types::valid_user_props(uns, n_user_props)?,
            server_reference,
        };

        Ok(val)
    }
}

impl DisconnProperties {
    #[cfg(any(feature = "fuzzy", test))]
    pub fn is_empty(&mut self) -> bool {
        self.session_expiry_interval.is_none()
            && self.reason_string.is_none()
            && self.user_properties.len() == 0
            && self.server_reference.is_none()
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
