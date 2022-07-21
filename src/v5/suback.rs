use crate::util::advance;
use crate::v5::{FixedHeader, PacketType, Property, PropertyType};
use crate::{Blob, Packetize, UserProperty, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

const PP: &'static str = "Packet::SubAck";

/// Error codes allowed in SUBACK packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SubAckReasonCode {
    QoS0 = 0x0,
    QoS1 = 0x1,
    QoS2 = 0x2,
    UnspecifiedError = 0x80,
    ImplementationError = 0x83,
    NotAuthorized = 0x87,
    InvalidTopicFilter = 0x8f,
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
            0x8f => SubAckReasonCode::InvalidTopicFilter,
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

/// SUBACK Packet
#[derive(Debug, Clone, PartialEq)]
pub struct SubAck {
    pub packet_id: u16,
    pub properties: Option<SubAckProperties>,
    pub return_codes: Vec<SubAckReasonCode>,
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
    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

/// Collection of MQTT properties allowed in SUBACK packet
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SubAckProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<UserProperty>,
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
