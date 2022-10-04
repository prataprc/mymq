#[cfg(any(feature = "fuzzy", test))]
use arbitrary::{Arbitrary, Error as ArbitraryError, Unstructured};

use std::{fmt, result};

use crate::v5::{self, FixedHeader, PacketType, Property, PropertyType, UserProperty};
use crate::{Blob, Packetize, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

/// Error codes allowed in PUBACK packet
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum PubAckReasonCode {
    Success = 0x00,
    NoMatchingSubscribers = 0x10,
    UnspecifiedError = 0x80,
    ImplementationError = 0x83,
    NotAuthorized = 0x87,
    TopicNameInvalid = 0x90,
    PacketIdInuse = 0x91,
    PacketIdNotFound = 0x92,
    QuotaExceeded = 0x97,
    PayloadFormatInvalid = 0x99,
}

/// Error codes allowed in PUBREC packet
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum PubRecReasonCode {
    Success = 0x00,
    NoMatchingSubscribers = 0x10,
    UnspecifiedError = 0x80,
    ImplementationError = 0x83,
    NotAuthorized = 0x87,
    TopicNameInvalid = 0x90,
    PacketIdInuse = 0x91,
    PacketIdNotFound = 0x92,
    QuotaExceeded = 0x97,
    PayloadFormatInvalid = 0x99,
}

/// Error codes allowed in PUBREL packet
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum PubRelReasonCode {
    Success = 0x00,
    PacketIdNotFound = 0x92,
}

/// Error codes allowed in PUBCOMP packet
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum PubCompReasonCode {
    Success = 0x00,
    PacketIdNotFound = 0x92,
}

/// PUBACK, PUBREC, PUBREL, PUBCOMP packets
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Pub {
    pub packet_type: PacketType,
    pub packet_id: u16,
    pub code: ReasonCode,
    pub properties: Option<PubProperties>,
}

impl fmt::Display for Pub {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        let name = match self.packet_type {
            PacketType::PubAck => "PUBACK",
            PacketType::PubRec => "PUBREC",
            PacketType::PubRel => "PUBREL",
            PacketType::PubComp => "PUBCOMP",
            _ => unreachable!(),
        };
        write!(f, "{} packet_id:{} code:{}", name, self.packet_id, self.code)?;

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
impl<'a> Arbitrary<'a> for Pub {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let pkt_types = [
            PacketType::PubAck,
            PacketType::PubRec,
            PacketType::PubRel,
            PacketType::PubComp,
        ];
        let packet_type = uns.choose(&pkt_types)?.clone();
        let code = {
            let code = match packet_type {
                PacketType::PubAck => uns.arbitrary::<PubAckReasonCode>()? as u8,
                PacketType::PubRec => uns.arbitrary::<PubRecReasonCode>()? as u8,
                PacketType::PubRel => uns.arbitrary::<PubRelReasonCode>()? as u8,
                PacketType::PubComp => uns.arbitrary::<PubCompReasonCode>()? as u8,
                _ => unreachable!(),
            };
            ReasonCode::try_from(code).unwrap()
        };
        let val = Pub {
            packet_type,
            packet_id: uns.arbitrary()?,
            code,
            properties: uns.arbitrary()?,
        };

        Ok(val)
    }
}

impl Packetize for Pub {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        // println!("Pub::decode {:?}", stream);

        let (fh, n) = dec_field!(FixedHeader, stream, 0);
        fh.validate()?;

        let (packet_type, _, _, _) = fh.unwrap();
        let (packet_id, n) = dec_field!(u16, stream, n);

        let (packet, n) = match *fh.remaining_len {
            2 => {
                let code: ReasonCode = ReasonCode::Success;
                let properties: Option<PubProperties> = None;
                let packet = Pub { packet_type, packet_id, code, properties };
                (packet, n)
            }
            3 => {
                let (code, n) = {
                    let (val, n) = dec_field!(u8, stream, n);
                    (ReasonCode::try_from(val)?, n)
                };
                let properties: Option<PubProperties> = None;
                let packet = Pub { packet_type, packet_id, code, properties };
                (packet, n)
            }
            _ => {
                let (code, n) = {
                    let (val, n) = dec_field!(u8, stream, n);
                    (ReasonCode::try_from(val)?, n)
                };
                let (properties, n) = dec_props!(PubProperties, stream, n);
                let packet = Pub { packet_type, packet_id, code, properties };
                (packet, n)
            }
        };

        packet.validate()?;
        Ok((packet, n))
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
            PacketType::PubAck => FixedHeader::new(PacketType::PubAck, remlen)?,
            PacketType::PubRel => FixedHeader::new_pubrel(remlen)?,
            PacketType::PubRec => FixedHeader::new(PacketType::PubRec, remlen)?,
            PacketType::PubComp => FixedHeader::new(PacketType::PubComp, remlen)?,
            packet_type => err!(ProtocolError, desc: "packet_type {:?}", packet_type)?,
        };
        data = insert_fixed_header(fh, data)?;

        // println!("Pub::encode {:?}", data);

        Ok(Blob::Large { data })
    }
}

impl Pub {
    pub fn new_pub_ack(packet_id: u16) -> Pub {
        Pub {
            packet_type: v5::PacketType::PubAck,
            packet_id,
            code: (PubAckReasonCode::Success as u8).try_into().unwrap(),
            properties: None,
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
        let invalid_code = match self.packet_type {
            PacketType::PubAck | PacketType::PubRec => match self.code {
                ReasonCode::Success => false,
                ReasonCode::NoMatchingSubscribers => false,
                ReasonCode::UnspecifiedError => false,
                ReasonCode::ImplementationError => false,
                ReasonCode::NotAuthorized => false,
                ReasonCode::TopicNameInvalid => false,
                ReasonCode::PacketIdInuse => false,
                ReasonCode::PacketIdNotFound => false,
                ReasonCode::QuotaExceeded => false,
                ReasonCode::PayloadFormatInvalid => false,
                _ => true,
            },
            PacketType::PubRel | PacketType::PubComp => match self.code {
                ReasonCode::Success => false,
                ReasonCode::PacketIdNotFound => false,
                _ => true,
            },
            _ => unreachable!(),
        };
        if invalid_code {
            err!(MalformedPacket, code: MalformedPacket, "invalid code {:?}", self.code)?
        }

        Ok(())
    }
}

/// Collection of MQTT properties in PUBACK, PUBREC, PUBREL, PUBCOMP packets
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct PubProperties {
    /// Property::ReasonString
    pub reason_string: Option<String>,
    /// Property::UserProp
    pub user_properties: Vec<UserProperty>,
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for PubProperties {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let rs_choice: Vec<String> =
            vec!["", "unit-testing"].into_iter().map(|s| s.to_string()).collect();
        let reason_string = match uns.arbitrary::<u8>()? % 2 {
            0 => Some(uns.choose(&rs_choice)?.to_string()),
            1 => None,
            _ => unreachable!(),
        };

        let n_user_props = uns.arbitrary::<usize>()? % 4;
        let val = PubProperties {
            reason_string,
            user_properties: v5::valid_user_props(uns, n_user_props)?,
        };

        Ok(val)
    }
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
                err!(ProtocolError, code: ProtocolError, "repeat prop {:?}", pt)?
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

impl PubProperties {
    #[cfg(any(feature = "fuzzy", test))]
    pub fn is_empty(&self) -> bool {
        self.reason_string.is_none() && self.user_properties.len() == 0
    }
}
