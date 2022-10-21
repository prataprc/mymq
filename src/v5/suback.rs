#[cfg(any(feature = "fuzzy", test))]
use arbitrary::{Arbitrary, Error as ArbitraryError, Unstructured};

use std::{fmt, result};

use crate::v5::UserProperty;
use crate::v5::{FixedHeader, Property, PropertyType, Subscribe};
use crate::{Blob, PacketType, Packetize, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

const PP: &'static str = "Packet::SubAck";

/// Error codes allowed in SUBACK packet.
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum SubAckReasonCode {
    QoS0 = 0x0,
    QoS1 = 0x1,
    QoS2 = 0x2,
    UnspecifiedError = 0x80,
    ImplementationError = 0x83,
    NotAuthorized = 0x87,
    TopicFilterInvalid = 0x8f,
    PacketIdInuse = 0x91,
    QuotaExceeded = 0x97,
    SharedSubscriptionsNotSupported = 0x9e,
    SubscriptionIdNotSupported = 0xa1,
    WildcardSubscriptionsNotSupported = 0xa2,
}

impl TryFrom<u8> for SubAckReasonCode {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        let v = match value {
            0x00 => SubAckReasonCode::QoS0,
            0x01 => SubAckReasonCode::QoS1,
            0x02 => SubAckReasonCode::QoS2,
            0x80 => SubAckReasonCode::UnspecifiedError,
            0x83 => SubAckReasonCode::ImplementationError,
            0x87 => SubAckReasonCode::NotAuthorized,
            0x8f => SubAckReasonCode::TopicFilterInvalid,
            0x91 => SubAckReasonCode::PacketIdInuse,
            0x97 => SubAckReasonCode::QuotaExceeded,
            0x9e => SubAckReasonCode::SharedSubscriptionsNotSupported,
            0xa1 => SubAckReasonCode::SubscriptionIdNotSupported,
            0xa2 => SubAckReasonCode::WildcardSubscriptionsNotSupported,
            val => err!(
                MalformedPacket,
                code: MalformedPacket,
                "{} reason-code {}",
                PP,
                val
            )?,
        };

        Ok(v)
    }
}

impl TryFrom<ReasonCode> for SubAckReasonCode {
    type Error = Error;

    fn try_from(value: ReasonCode) -> Result<Self> {
        SubAckReasonCode::try_from(value as u8)
    }
}

/// SUBACK Packet
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubAck {
    pub packet_id: u16,
    pub return_codes: Vec<SubAckReasonCode>,
    pub properties: Option<SubAckProperties>,
}

impl fmt::Display for SubAck {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "SUBACK packet_id:{} rc:{:?}", self.packet_id, self.return_codes)?;

        if let Some(properties) = &self.properties {
            let mut props = Vec::default();
            if let Some(val) = &properties.reason_string {
                props.push(format!("  reason_string: {:?}", val));
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
impl<'a> Arbitrary<'a> for SubAck {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let mut return_codes: Vec<SubAckReasonCode> = vec![];
        for _i in 0..((uns.arbitrary::<u8>()? % 32) + 1) {
            return_codes.push(uns.arbitrary()?)
        }

        let val = SubAck {
            packet_id: uns.arbitrary()?,
            properties: uns.arbitrary()?,
            return_codes,
        };

        Ok(val)
    }
}

impl Packetize for SubAck {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (fh, fh_len) = dec_field!(FixedHeader, stream, 0);
        fh.validate()?;

        let (packet_id, n) = dec_field!(u16, stream, fh_len);
        let (properties, n) = dec_props!(SubAckProperties, stream, n);

        let (payload, n) = match fh_len + usize::try_from(*fh.remaining_len)? {
            m if m == n => {
                err!(MalformedPacket, code: MalformedPacket, "{} no payload", PP)?
            }
            m if m <= stream.len() => (stream[n..m].to_vec(), m),
            m => err!(MalformedPacket, code: MalformedPacket, "{} in payload {}", PP, m)?,
        };

        let mut return_codes: Vec<SubAckReasonCode> = Vec::with_capacity(payload.len());
        for code in payload.into_iter() {
            return_codes.push(code.try_into()?);
        }

        let val = SubAck { packet_id, properties, return_codes };

        val.validate()?;
        Ok((val, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::v5::insert_fixed_header;

        let mut data = Vec::with_capacity(64);

        data.extend_from_slice(self.packet_id.encode()?.as_ref());
        if let Some(properties) = &self.properties {
            data.extend_from_slice(properties.encode()?.as_ref());
        } else {
            data.extend_from_slice(VarU32(0).encode()?.as_ref());
        }
        for code in self.return_codes.clone().into_iter() {
            data.push(code as u8)
        }

        let fh = FixedHeader::new(PacketType::SubAck, VarU32(data.len().try_into()?))?;
        data = insert_fixed_header(fh, data)?;

        Ok(Blob::Large { data })
    }
}

impl SubAck {
    pub fn from_sub(sub: &Subscribe, codes: Vec<ReasonCode>) -> SubAck {
        SubAck {
            packet_id: sub.packet_id,
            properties: None,
            return_codes: codes
                .into_iter()
                .map(|code| SubAckReasonCode::try_from(code).unwrap())
                .collect(),
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

    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

/// Collection of MQTT properties allowed in SUBACK packet
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct SubAckProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<UserProperty>,
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for SubAckProperties {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        use crate::v5::valid_user_props;

        let rs_choice: Vec<String> =
            vec!["", "unit-testing"].into_iter().map(|s| s.to_string()).collect();
        let reason_string = match uns.arbitrary::<u8>()? % 2 {
            0 => Some(uns.choose(&rs_choice)?.to_string()),
            1 => None,
            _ => unreachable!(),
        };

        let n_user_props = uns.arbitrary::<usize>()? % 4;
        let val = SubAckProperties {
            reason_string,
            user_properties: valid_user_props(uns, n_user_props)?,
        };

        Ok(val)
    }
}

impl Packetize for SubAckProperties {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let mut dups = [false; 256];
        let mut props = SubAckProperties::default();

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
                Property::ReasonString(val) => props.reason_string = Some(val),
                Property::UserProp(val) => props.user_properties.push(val),
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

        enc_prop!(opt: data, ReasonString, &self.reason_string);

        for uprop in self.user_properties.iter() {
            enc_prop!(data, UserProp, uprop)
        }

        let data = insert_property_len(data.len(), data)?;

        Ok(Blob::Large { data })
    }
}

impl SubAckProperties {
    #[cfg(any(feature = "fuzzy", test))]
    pub fn is_empty(&self) -> bool {
        self.reason_string.is_none() && self.user_properties.len() == 0
    }
}
