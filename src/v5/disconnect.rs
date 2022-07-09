use crate::util::advance;
use crate::v5::{FixedHeader, Property, PropertyType};
use crate::{Blob, Packetize, UserProperty, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

const PP: &'static str = "Packet::Disconnect";

#[derive(Clone, Copy, PartialEq, Debug)]
#[repr(u8)]
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
    InvalidTopicName = 0x90,
    ExceededReceiveMaximum = 0x93,
    InvalidTopicAlias = 0x94,
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
            0x90 => Ok(DisconnReasonCode::InvalidTopicName),
            0x93 => Ok(DisconnReasonCode::ExceededReceiveMaximum),
            0x94 => Ok(DisconnReasonCode::InvalidTopicAlias),
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

#[derive(Clone, PartialEq, Debug)]
pub struct Disconnect {
    code: Option<DisconnReasonCode>,
    properties: Option<DisconnProperties>,
}

impl Disconnect {
    pub fn new(code: DisconnReasonCode, props: Option<DisconnProperties>) -> Disconnect {
        Disconnect { code: Some(code), properties: props }
    }
}

impl Packetize for Disconnect {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (fh, n) = dec_field!(FixedHeader, stream, 0);
        fh.validate()?;

        let (val, n) = match *fh.remaining_len {
            0 => {
                let code = Some(DisconnReasonCode::NormalDisconnect);
                (Disconnect { code, properties: None }, n)
            }
            m if m < 2 => {
                let (code, n) = dec_field!(u8, stream, n);
                let code = Some(DisconnReasonCode::try_from(code)?);
                (Disconnect { code, properties: None }, n)
            }
            _ => {
                let (code, n) = dec_field!(u8, stream, n);
                let code = Some(DisconnReasonCode::try_from(code)?);
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

        let code = self.code.unwrap_or(DisconnReasonCode::NormalDisconnect);
        data.extend_from_slice((code as u8).encode()?.as_ref());
        if let Some(properties) = &self.properties {
            data.extend_from_slice(properties.encode()?.as_ref());
        } else {
            data.extend_from_slice(VarU32(0).encode()?.as_ref());
        }

        let fh = FixedHeader::new(Disconnect, VarU32(data.len().try_into()?))?;
        data = insert_fixed_header(fh, data)?;

        Ok(Blob::Large { data })
    }
}

impl Disconnect {
    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, PartialEq, Debug, Default)]
pub struct DisconnProperties {
    pub session_expiry_interval: Option<u32>,
    pub reason_string: Option<String>,
    pub user_properties: Vec<UserProperty>,
    pub server_reference: Option<String>,
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
