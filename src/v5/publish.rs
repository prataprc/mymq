#[cfg(any(feature = "fuzzy", test))]
use arbitrary::{Arbitrary, Error as ArbitraryError, Unstructured};

use std::{cmp, fmt, result};

use crate::util::advance;
use crate::v5::{FixedHeader, PayloadFormat, Property, PropertyType, QoS};
use crate::{Blob, Packetize, TopicName, UserProperty, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

const PP: &'static str = "Packet::Publish";

/// PUBLISH Packet
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    pub retain: bool,
    pub qos: QoS,
    pub duplicate: bool,
    pub topic_name: TopicName,
    pub packet_id: Option<u16>,
    pub properties: Option<PublishProperties>,
    pub payload: Option<Vec<u8>>,
}

impl fmt::Display for Publish {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        let packet_id = self.packet_id.map(|x| x.to_string()).unwrap_or("-".to_string());
        write!(f, "PUBLISH<{},packet_id:{}>", *self.topic_name, packet_id)
    }
}

impl PartialOrd for Publish {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.topic_name.partial_cmp(&other.topic_name)
    }
}

impl Ord for Publish {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.topic_name.cmp(&other.topic_name)
    }
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for Publish {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let properties: Option<PublishProperties> = uns.arbitrary()?;

        let payload: Option<Vec<u8>> = match uns.arbitrary::<bool>()? {
            true => match &properties {
                Some(props) => match props.payload_format_indicator {
                    PayloadFormat::Binary => Some(uns.arbitrary::<Vec<u8>>()?),
                    PayloadFormat::Utf8 => Some("payload-as-utf8".to_string().into()),
                },
                None => uns.arbitrary()?,
            },
            false => None,
        };
        let qos = uns.arbitrary()?;
        let (packet_id, duplicate) = match qos {
            QoS::AtMostOnce => (None, false),
            QoS::AtLeastOnce => (Some(uns.arbitrary()?), uns.arbitrary()?),
            QoS::ExactlyOnce => (Some(uns.arbitrary()?), uns.arbitrary()?),
        };

        let val = Publish {
            retain: uns.arbitrary()?,
            qos,
            duplicate,
            topic_name: uns.arbitrary()?,
            packet_id,
            properties,
            payload,
        };

        Ok(val)
    }
}

impl Packetize for Publish {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        // println!("{:?}", stream);

        let (fh, fh_len) = dec_field!(FixedHeader, stream, 0);
        fh.validate()?;
        let (_, retain, qos, duplicate) = fh.unwrap();

        let (topic_name, n) = dec_field!(TopicName, stream, fh_len);
        let (packet_id, n) = dec_field!(
            u16,
            stream,
            n;
            matches!(qos, QoS::AtLeastOnce | QoS::ExactlyOnce)
        );
        let (properties, n) = dec_props!(PublishProperties, stream, n);

        let (payload, n) = match fh_len + usize::try_from(*fh.remaining_len)? {
            m if m == n => (None, n),
            m if m <= stream.len() => (Some(stream[n..m].to_vec()), m),
            m => err!(MalformedPacket, code: MalformedPacket, "{} in payload {}", PP, m)?,
        };

        let val = Publish {
            retain,
            qos,
            duplicate,
            topic_name,
            packet_id,
            properties,
            payload,
        };

        val.validate()?;
        Ok((val, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::v5::insert_fixed_header;

        let mut data = Vec::with_capacity(64);

        data.extend_from_slice(self.topic_name.encode()?.as_ref());
        if let Some(packet_id) = self.packet_id {
            data.extend_from_slice(packet_id.encode()?.as_ref());
        }
        if let Some(properties) = &self.properties {
            data.extend_from_slice(properties.encode()?.as_ref());
        } else {
            data.extend_from_slice(VarU32(0).encode()?.as_ref());
        }
        if let Some(payload) = &self.payload {
            data.extend_from_slice(payload)
        }

        let fh = FixedHeader::new_publish(
            self.retain,
            self.qos,
            self.duplicate,
            VarU32(data.len().try_into()?),
        )?;
        data = insert_fixed_header(fh, data)?;

        // println!("{:?}", data);

        Ok(Blob::Large { data })
    }
}

impl Publish {
    pub fn set_fixed_header(&mut self, retain: bool, qos: QoS, dup: bool) -> &mut Self {
        self.retain = retain;
        self.qos = qos;
        self.duplicate = dup;
        self
    }

    pub fn set_packet_id(&mut self, packet_id: u16) -> &mut Self {
        self.packet_id = Some(packet_id);
        self
    }

    pub fn add_subscription_id(&mut self, id: Option<u32>) {
        match id {
            Some(id) => match &mut self.properties {
                Some(props) => props.subscribtion_identifier.push(VarU32(id)),
                None => {
                    self.properties = Some(PublishProperties {
                        subscribtion_identifier: vec![VarU32(id)],
                        ..PublishProperties::default()
                    });
                }
            },
            None => (),
        }
    }

    fn validate(&self) -> Result<()> {
        match self.qos {
            QoS::AtMostOnce if self.duplicate => err!(
                MalformedPacket,
                code: MalformedPacket,
                "{} DUP is set for QoS-0",
                PP
            )?,
            QoS::AtLeastOnce | QoS::ExactlyOnce if self.packet_id.is_none() => err!(
                MalformedPacket,
                code: MalformedPacket,
                "{} packet_id missing for QoS > 0 {:?}",
                PP,
                self.qos
            )?,
            _ => (),
        }

        if let (Some(payload), Some(true)) =
            (self.payload.as_ref(), self.properties.as_ref().map(|p| p.is_payload_utf8()))
        {
            if let Err(err) = std::str::from_utf8(&payload) {
                err!(
                    MalformedPacket,
                    code: PayloadFormatInvalid,
                    cause: err,
                    "{} payload invalid utf8 ",
                    PP
                )?;
            }
        }

        Ok(())
    }

    #[cfg(any(feature = "fuzzy", test))]
    pub fn normalize(&mut self) {
        if let Some(props) = &self.properties {
            if props.is_empty() {
                self.properties = None
            }
        }
        if let Some(payload) = &self.payload {
            if payload.len() == 0 {
                self.payload = None
            }
        }
    }

    pub fn as_topic_name(&self) -> &TopicName {
        &self.topic_name
    }

    pub fn topic_alias(&self) -> Option<u16> {
        match &self.properties {
            Some(props) => props.topic_alias,
            None => None,
        }
    }
}

/// Collection of MQTT properties allowed in PUBLISH packet
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PublishProperties {
    pub payload_format_indicator: PayloadFormat, // default=PayloadFormat::Binary
    pub message_expiry_interval: Option<u32>,
    pub topic_alias: Option<u16>,
    pub response_topic: Option<TopicName>,
    pub correlation_data: Option<Vec<u8>>,
    pub subscribtion_identifier: Vec<VarU32>,
    pub content_type: Option<String>,
    pub user_properties: Vec<UserProperty>,
}

impl<'a> Arbitrary<'a> for PublishProperties {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        use crate::types;

        let ct_choice: Vec<String> =
            vec!["", "img/png"].into_iter().map(|s| s.to_string()).collect();
        let content_type = match uns.arbitrary::<u8>()? % 2 {
            0 => Some(uns.choose(&ct_choice)?.to_string()),
            1 => None,
            _ => unreachable!(),
        };

        let n_user_props = uns.arbitrary::<usize>()? % 4;
        let val = PublishProperties {
            payload_format_indicator: uns.arbitrary()?,
            message_expiry_interval: uns.arbitrary()?,
            topic_alias: uns.arbitrary()?,
            response_topic: uns.arbitrary()?,
            correlation_data: uns.arbitrary()?,
            subscribtion_identifier: uns.arbitrary()?,
            content_type,
            user_properties: types::valid_user_props(uns, n_user_props)?,
        };

        Ok(val)
    }
}

impl Packetize for PublishProperties {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        use crate::v5::Property::*;

        let stream: &[u8] = stream.as_ref();

        let mut dups = [false; 256];
        let mut props = PublishProperties::default();

        let (len, mut n) = dec_field!(VarU32, stream, 0);
        let limit = usize::try_from(*len)? + n;

        while n < limit {
            let (property, m) = dec_field!(Property, stream, n);
            n = m;

            let dup_ok = [PropertyType::UserProp, PropertyType::SubscriptionIdentifier];
            let pt = property.to_property_type();
            if !dup_ok.contains(&pt) && dups[pt as usize] {
                err!(ProtocolError, code: ProtocolError, "{} repeat prop {:?}", PP, pt)?
            }
            dups[pt as usize] = true;

            match property {
                PayloadFormatIndicator(val) => {
                    props.payload_format_indicator = val.try_into()?;
                }
                MessageExpiryInterval(val) => props.message_expiry_interval = Some(val),
                TopicAlias(0) => {
                    err!(ProtocolError, code: ProtocolError, "{} topic-alias=ZERO", PP)?
                }
                TopicAlias(val) => props.topic_alias = Some(val),
                ResponseTopic(val) => props.response_topic = Some(val),
                CorrelationData(val) => props.correlation_data = Some(val),
                SubscriptionIdentifier(val) => props.subscribtion_identifier.push(val),
                ContentType(val) => props.content_type = Some(val),
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

        if self.payload_format_indicator.is_utf8() {
            let val = u8::from(PayloadFormat::Utf8);
            enc_prop!(data, PayloadFormatIndicator, val);
        }
        enc_prop!(opt: data, MessageExpiryInterval, self.message_expiry_interval);
        enc_prop!(opt: data, TopicAlias, self.topic_alias);
        enc_prop!(opt: data, ResponseTopic, &self.response_topic);
        enc_prop!(opt: data, CorrelationData, &self.correlation_data);
        enc_prop!(opt: data, ContentType, &self.content_type);

        for subid in self.subscribtion_identifier.iter() {
            enc_prop!(data, SubscriptionIdentifier, subid);
        }
        for uprop in self.user_properties.iter() {
            enc_prop!(data, UserProp, uprop);
        }

        data = insert_property_len(data.len(), data)?;

        Ok(Blob::Large { data })
    }
}

impl PublishProperties {
    fn is_payload_utf8(&self) -> bool {
        self.payload_format_indicator.is_utf8()
    }

    #[cfg(any(feature = "fuzzy", test))]
    pub fn is_empty(&self) -> bool {
        self.payload_format_indicator == PayloadFormat::Binary
            && self.message_expiry_interval.is_none()
            && self.topic_alias.is_none()
            && self.response_topic.is_none()
            && self.correlation_data.is_none()
            && self.subscribtion_identifier.len() == 0
            && self.content_type.is_none()
            && self.user_properties.len() == 0
    }
}
