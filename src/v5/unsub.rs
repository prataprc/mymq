use crate::v5::{FixedHeader, Property, PropertyType};
use crate::{util::advance, Blob, Packetize, TopicFilter, UserProperty, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

const PP: &'static str = "Packet::UnSubscribe";

#[derive(Clone, PartialEq, Debug)]
pub struct UnSubscribe {
    pub packet_id: u16,
    pub properties: Option<UnSubscribeProperties>,
    pub filters: Vec<TopicFilter>,
}

impl Packetize for UnSubscribe {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (fh, fh_len) = dec_field!(FixedHeader, stream, 0);
        fh.validate()?;

        let (packet_id, n) = dec_field!(u16, stream, fh_len);
        let (properties, n) = dec_props!(UnSubscribeProperties, stream, n);
        let (payload, n) = match fh_len + usize::try_from(*fh.remaining_len)? {
            m if m == n => {
                err!(ProtocolError, code: ProtocolError, "{} in payload {}", PP, m)?
            }
            m if m <= stream.len() => (&stream[n..m], m),
            m => err!(ProtocolError, code: ProtocolError, "{} in payload {}", PP, m)?,
        };

        // Assuming that each entry in payload will take up 32 bytes.
        let mut filters = Vec::with_capacity((payload.len() / 32) + 1);
        let mut t = 0;
        while t < payload.len() {
            let (filter, m) = dec_field!(TopicFilter, payload, t);
            t = m;
            filters.push(filter);
        }

        let val = UnSubscribe { packet_id, properties, filters };

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

        for filter in self.filters.iter() {
            data.extend_from_slice(filter.encode()?.as_ref());
        }

        let fh = FixedHeader::new_unsubscribe(VarU32(data.len().try_into()?))?;
        data = insert_fixed_header(fh, data)?;

        Ok(Blob::Large { data })
    }
}

impl UnSubscribe {
    fn validate(&self) -> Result<()> {
        if self.filters.len() == 0 {
            err!(ProtocolError, code: ProtocolError, "{} topic filter missing", PP)?
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct UnSubscribeProperties {
    pub user_properties: Vec<UserProperty>,
}

impl Packetize for UnSubscribeProperties {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        use crate::v5::Property::*;

        let stream: &[u8] = stream.as_ref();

        let mut dups = [false; 256];
        let mut props = UnSubscribeProperties::default();

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
                UserProp(val) => props.user_properties.push(val),
                _ => {
                    err!(ProtocolError, code: ProtocolError, "{} bad prop {:?}", PP, pt)?
                }
            }
        }

        Ok((props, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::v5::insert_property_len;

        let mut data = Vec::with_capacity(64);

        for uprop in self.user_properties.iter() {
            enc_prop!(data, UserProp, uprop);
        }

        data = insert_property_len(data.len(), data)?;

        Ok(Blob::Large { data })
    }
}
