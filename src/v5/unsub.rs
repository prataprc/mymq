#[cfg(any(feature = "fuzzy", test))]
use arbitrary::{Arbitrary, Error as ArbitraryError, Unstructured};

use std::{fmt, result};

use crate::v5::{Blob, FixedHeader, Property, PropertyType, UserProperty, VarU32};
use crate::v5::{Error, ErrorKind, ReasonCode, Result};
use crate::{Packetize, TopicFilter};

const PP: &'static str = "Packet::UnSubscribe";

/// UNSUBSCRIBE Packet
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct UnSubscribe {
    pub packet_id: u16,
    pub filters: Vec<TopicFilter>,
    pub properties: Option<UnSubscribeProperties>,
}

impl fmt::Display for UnSubscribe {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "UNSUBSCRIBE packet_id:{}", self.packet_id)?;

        let mut filters = Vec::default();
        for topic_filter in self.filters.iter() {
            filters.push(format!("filter:{:?}", topic_filter));
        }
        write!(f, "{}", filters.join("\n"))?;

        if let Some(properties) = &self.properties {
            let mut props = Vec::default();
            for (key, val) in properties.user_properties.iter() {
                props.push(format!("  {:?}: {:?}", key, val));
            }
            write!(f, "{}\n", props.join("\n"))?;
        }

        Ok(())
    }
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for UnSubscribe {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let mut filters: Vec<TopicFilter> = vec![];
        for _i in 0..((uns.arbitrary::<u8>()? % 32) + 1) {
            filters.push(uns.arbitrary()?)
        }

        let val = UnSubscribe {
            packet_id: uns.arbitrary()?,
            properties: uns.arbitrary()?,
            filters,
        };

        Ok(val)
    }
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
    #[cfg(any(feature = "fuzzy", test))]
    pub fn normalize(&mut self) {
        if let Some(props) = &mut self.properties {
            if props.is_empty() {
                self.properties = None
            }
        }
    }

    fn validate(&self) -> Result<()> {
        if self.filters.len() == 0 {
            err!(ProtocolError, code: ProtocolError, "{} topic filter missing", PP)?
        }

        Ok(())
    }
}

/// Collection of MQTT properties allowed in UNSUBSCRIBE packet
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct UnSubscribeProperties {
    pub user_properties: Vec<UserProperty>,
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for UnSubscribeProperties {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        use crate::v5::valid_user_props;

        let n_user_props = uns.arbitrary::<usize>()? % 4;
        let val = UnSubscribeProperties {
            user_properties: valid_user_props(uns, n_user_props)?,
        };

        Ok(val)
    }
}

impl UnSubscribeProperties {
    #[cfg(any(feature = "fuzzy", test))]
    pub fn is_empty(&self) -> bool {
        self.user_properties.len() == 0
    }
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
