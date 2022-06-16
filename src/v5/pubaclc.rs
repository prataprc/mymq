use crate::util::advance;
use crate::v5::{FixedHeader, PacketType, Property, PropertyType};
use crate::{Blob, Packetize, UserProperty, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PubReasonCode {
    Success = 0x00,
    NoMatchingSubscribers = 0x10,
    UnspecifiedError = 0x80,
    ImplementationError = 0x83,
    NotAuthorized = 0x87,
    InvalidTopicName = 0x90,
    PacketIdInuse = 0x91,
    PacketIdNotFound = 0x92,
    QuotaExceeded = 0x97,
    PayloadFormatInvalid = 0x99,
}

impl TryFrom<u8> for PubReasonCode {
    type Error = Error;

    fn try_from(val: u8) -> Result<PubReasonCode> {
        match val {
            0x00 => Ok(PubReasonCode::Success),
            0x10 => Ok(PubReasonCode::NoMatchingSubscribers),
            0x80 => Ok(PubReasonCode::UnspecifiedError),
            0x83 => Ok(PubReasonCode::ImplementationError),
            0x87 => Ok(PubReasonCode::NotAuthorized),
            0x90 => Ok(PubReasonCode::InvalidTopicName),
            0x91 => Ok(PubReasonCode::PacketIdInuse),
            0x92 => Ok(PubReasonCode::PacketIdNotFound),
            0x97 => Ok(PubReasonCode::QuotaExceeded),
            0x99 => Ok(PubReasonCode::PayloadFormatInvalid),
            val => err!(ProtocolError, code: ProtocolError, "reason-code {:?}", val),
        }
    }
}

impl Default for PubReasonCode {
    fn default() -> PubReasonCode {
        PubReasonCode::Success
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pub {
    pub packet_type: PacketType,
    pub packet_id: u16,
    pub code: PubReasonCode,
    pub properties: Option<PubProperties>,
}

impl Packetize for Pub {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let code: PubReasonCode = PubReasonCode::Success;
        let properties: Option<PubProperties> = None;

        let (fh, n) = dec_field!(FixedHeader, stream, 0);
        fh.validate()?;
        let (packet_type, _, _, _) = fh.unwrap()?;

        let (packet_id, n) = dec_field!(u16, stream, n);

        if *fh.remaining_len == 2 {
            let packet = Pub { packet_type, packet_id, code, properties };
            return Ok((packet, n));
        }

        let (code, n) = {
            let (val, n) = dec_field!(u8, stream, n);
            (PubReasonCode::try_from(val)?, n)
        };

        if *fh.remaining_len < 4 {
            let packet = Pub { packet_type, packet_id, code, properties };
            return Ok((packet, n));
        }

        let (properties, n) = dec_props!(PubProperties, stream, n);

        let val = Pub { packet_type, packet_id, code, properties };
        val.validate()?;
        Ok((val, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::v5::insert_fixed_header;

        let mut data = Vec::with_capacity(64);

        data.extend_from_slice(self.packet_id.encode()?.as_ref());
        data.extend_from_slice((self.code as u8).encode()?.as_ref());
        if let Some(properties) = &self.properties {
            data.extend_from_slice(properties.encode()?.as_ref());
        } else {
            data.extend_from_slice(VarU32(0).encode()?.as_ref());
        }

        let remlen = VarU32(data.len().try_into()?);
        let fh = match self.packet_type {
            PacketType::PubRel => FixedHeader::new_pubrel(remlen)?,
            PacketType::PubRec => FixedHeader::new(PacketType::PubRec, remlen)?,
            PacketType::PubComp => FixedHeader::new(PacketType::PubComp, remlen)?,
            packet_type => err!(InvalidInput, desc: "packet_type {:?}", packet_type)?,
        };
        data = insert_fixed_header(fh, data)?;

        Ok(Blob::Large { data })
    }
}

impl Pub {
    fn validate(&self) -> Result<()> {
        let invalid_code = match (self.packet_type, self.code) {
            (PacketType::PubAck, PubReasonCode::PacketIdNotFound) => true,
            (PacketType::PubRec, PubReasonCode::PacketIdNotFound) => true,
            (PacketType::PubRel, PubReasonCode::Success) => false,
            (PacketType::PubRel, PubReasonCode::PacketIdNotFound) => false,
            (PacketType::PubRel, _) => true,
            (PacketType::PubComp, PubReasonCode::Success) => false,
            (PacketType::PubComp, PubReasonCode::PacketIdNotFound) => false,
            (PacketType::PubComp, _) => true,
            (_, _) => false,
        };
        if invalid_code {
            err!(MalformedPacket, code: MalformedPacket, "invalid code {:?}", self.code)?
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct PubProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<UserProperty>,
}

impl Packetize for PubProperties {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let mut dups = [false; 256];
        let mut props = PubProperties::default();

        let (len, mut n) = dec_field!(VarU32, stream, 0);
        let limit = usize::try_from(*len)? + n;

        while n < limit {
            let (property, m) = dec_field!(Property, stream, n);
            n = m;

            let pt = property.to_property_type();
            if pt != PropertyType::UserProp && dups[pt as usize] {
                err!(ProtocolError, code: ProtocolError, "duplicate property {:?}", pt)?
            }
            dups[pt as usize] = true;

            match property {
                Property::ReasonString(val) => props.reason_string = Some(val),
                Property::UserProp(val) => props.user_properties.push(val),
                _ => err!(
                    ProtocolError,
                    code: ProtocolError,
                    "{:?} found in puback properties",
                    pt
                )?,
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