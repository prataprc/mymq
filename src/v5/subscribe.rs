use crate::v5::{FixedHeader, Property, PropertyType, QoS};
use crate::{util::advance, Blob, Packetize, TopicFilter, UserProperty, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

#[derive(Clone, PartialEq, Debug)]
pub struct SubscriptionOpt(u8);

impl Packetize for SubscriptionOpt {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        let (opt, n) = u8::decode(stream)?;

        if (opt & 0b11000000) > 0 {
            err!(
                MalformedPacket,
                code: MalformedPacket,
                "subscription option reserved bit is non-ZERO {:?}",
                opt
            )?
        }

        let val = SubscriptionOpt(opt);
        val.unwrap()?;

        Ok((val, n))
    }

    fn encode(&self) -> Result<Blob> {
        self.unwrap()?;
        self.0.encode()
    }
}

impl SubscriptionOpt {
    pub fn new(rfr: RetainForwardRule, rap: bool, nl: bool, qos: QoS) -> Self {
        let rfr: u8 = u8::from(rfr) << 4;
        let rap: u8 = if rap { 0b1000 } else { 0b0000 };
        let nl: u8 = if nl { 0b0100 } else { 0b0000 };
        let qos: u8 = qos.into();

        SubscriptionOpt(rfr | rap | nl | qos)
    }

    fn unwrap(&self) -> Result<(RetainForwardRule, bool, bool, QoS)> {
        let qos: QoS = (self.0 & 0b0011).try_into()?;
        let nl: bool = (self.0 & 0b0100) > 0;
        let rap: bool = (self.0 & 0b1000) > 0;
        let rfr: RetainForwardRule = ((self.0 >> 4) & 0b0011).try_into()?;
        Ok((rfr, rap, nl, qos))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RetainForwardRule {
    OnEverySubscribe = 0,
    OnNewSubscribe = 1,
    Never = 2,
}

impl TryFrom<u8> for RetainForwardRule {
    type Error = Error;

    fn try_from(val: u8) -> Result<RetainForwardRule> {
        let val = match val {
            0 => RetainForwardRule::OnEverySubscribe,
            1 => RetainForwardRule::OnNewSubscribe,
            2 => RetainForwardRule::Never,
            _ => err!(ProtocolError, code: ProtocolError, "forbidden packet")?,
        };

        Ok(val)
    }
}

impl From<RetainForwardRule> for u8 {
    fn from(val: RetainForwardRule) -> u8 {
        match val {
            RetainForwardRule::OnEverySubscribe => 0,
            RetainForwardRule::OnNewSubscribe => 1,
            RetainForwardRule::Never => 2,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct Subscribe {
    pub packet_id: u16,
    pub properties: Option<SubscribeProperties>,
    pub filters: Vec<SubscribeFilter>,
}

impl Packetize for Subscribe {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        use crate::dec_props;

        let (fh, fh_len) = FixedHeader::decode(stream)?;
        fh.validate()?;

        let mut n = fh_len;

        let (packet_id, m) = u16::decode(advance(stream, n)?)?;
        n += m;

        let (properties, m) = dec_props!(SubscribeProperties, stream, n)?;
        n += m;

        let (payload, m) = match fh_len + usize::try_from(*fh.remaining_len)? {
            m if m == n => err!(ProtocolError, code: ProtocolError, "in payload {}", m)?,
            m if m <= stream.len() => (&stream[n..m], m - n),
            m => err!(ProtocolError, code: ProtocolError, "in payload {}", m)?,
        };
        n += m;

        let mut filters = vec![];
        let mut t = 0;
        while t < payload.len() {
            let (filter, m) = SubscribeFilter::decode(advance(payload, t)?)?;
            t += m;
            filters.push(filter);
        }

        let val = Subscribe { packet_id, properties, filters };

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

        let fh = FixedHeader::new_subscribe(VarU32(data.len().try_into()?))?;
        data = insert_fixed_header(fh, data)?;

        Ok(Blob::Large { data })
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SubscribeProperties {
    pub subscription_id: Option<VarU32>,
    pub user_properties: Vec<UserProperty>,
}

impl Packetize for SubscribeProperties {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        let mut dups = [false; 256];
        let mut props = SubscribeProperties::default();

        let (len, mut n) = VarU32::decode(stream)?;
        let limit = usize::try_from(*len)? + n;

        while n < limit {
            let (property, m) = Property::decode(advance(stream, n)?)?;
            n += m;

            let pt = property.to_property_type();
            if pt != PropertyType::UserProp && dups[pt as usize] {
                err!(ProtocolError, code: ProtocolError, "duplicate property {:?}", pt)?
            }
            dups[pt as usize] = true;

            match property {
                Property::SubscriptionIdentifier(val) => {
                    props.subscription_id = Some(val);
                }
                Property::UserProp(val) => {
                    props.user_properties.push(val);
                }
                _ => err!(
                    ProtocolError,
                    code: ProtocolError,
                    "{:?} found in will properties",
                    pt
                )?,
            }
        }

        Ok((props, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::{enc_prop, v5::insert_property_len};

        let mut data = Vec::with_capacity(64);

        enc_prop!(opt: data, SubscriptionIdentifier, self.subscription_id);

        for uprop in self.user_properties.iter() {
            enc_prop!(data, UserProp, uprop);
        }

        data = insert_property_len(data.len(), data)?;

        Ok(Blob::Large { data })
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct SubscribeFilter {
    pub topic_filter: TopicFilter,
    pub opt: SubscriptionOpt,
}

impl Packetize for SubscribeFilter {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        let (topic_filter, mut n) = TopicFilter::decode(stream)?;

        let (opt, m) = SubscriptionOpt::decode(advance(stream, n)?)?;
        n += m;

        let val = SubscribeFilter { topic_filter, opt };
        Ok((val, n))
    }

    fn encode(&self) -> Result<Blob> {
        let mut data = Vec::with_capacity(64);

        data.extend_from_slice(self.topic_filter.encode()?.as_ref());
        data.extend_from_slice(self.opt.encode()?.as_ref());

        Ok(Blob::Large { data })
    }
}
