// TODO: The DUP flag MUST be set to 0 for all QoS 0 messages
// TODO: Test case to detect validity of duplicate message, that there should be
//       no duplicate message for QoS0. Duplicate message for QoS1 is allowed but not
//       after sender has received PUBACK. Duplicate message of QoS2 is allowed but not
//       after PUBREL.
// TODO: The receiver of an MQTT Control Packet that contains the DUP flag set to 1
//       cannot assume that it has seen an earlier copy of this packet.
// TODO: A PUBLISH packet MUST NOT contain a Packet Identifier if its QoS value is set
//       to 0.
// TODO: If the Server included a Maximum QoS in its CONNACK response to a Client and
//       it receives a PUBLISH packet with a QoS greater than this, then it uses
//       DISCONNECT with Reason Code 0x9B (QoS not supported).
// TODO: If the Payload contains zero bytes it is processed normally by the Server but
//       any retained message with the same topic name MUST be removed and any future
//       subscribers for the topic will not receive a retained message.
// TODO: A retained message with a Payload containing zero bytes MUST NOT be stored
//       as a retained message on the Server.
// TODO: If the Server included Retain Available in its CONNACK response to a Client
//       with its value set to 0 and it receives a PUBLISH packet with the RETAIN flag
//       is set to 1, then it uses the DISCONNECT Reason Code of 0x9A (Retain not
//       supported).
// TODO: what should be RETAIN flag when broker forwards a PUBLISH message.
// TODO: what should be RETAIN flag when broker forwards a retain message for matching
//       new subscriptions.
// TODO: The Topic Name in a PUBLISH packet sent by a Server to a subscribing Client
//       MUST match the Subscription’s Topic Filter according to the matching process
//       defined in section 4.7 [MQTT-3.3.2-3]. However, as the Server is permitted
//       to map the Topic Name to another name, it might not be the same as the
//       Topic Name in the original PUBLISH packet.
// TODO: It is a Protocol Error if the Topic Name is zero length and there is no
//       Topic Alias.
// TODO: Test cases to include packet-identifier for QoS0 messages.
// TODO: If the Message Expiry Interval has passed and the Server has not managed to
//       start onward delivery to a matching subscriber, then it MUST delete the copy
//       of the message for that subscriber

use crate::util::advance;
use crate::v5::{FixedHeader, PayloadFormat, Property, PropertyType, QoS};
use crate::{Blob, Packetize, TopicName, UserProperty, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct Publish {
    pub retain: bool,
    pub qos: QoS,
    pub duplicate: bool,
    pub topic_name: TopicName,
    pub packet_id: Option<u16>,
    pub properties: Option<PublishProperties>,
    pub payload: Option<Vec<u8>>,
}

impl Packetize for Publish {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        use crate::dec_props;

        let (fh, mut n) = FixedHeader::decode(stream)?;
        fh.validate()?;
        let (_, retain, qos, duplicate) = fh.unwrap()?;

        let (topic_name, m) = TopicName::decode(advance(stream, n)?)?;
        n += m;

        let (packet_id, m) = match qos {
            QoS::AtLeastOnce | QoS::ExactlyOnce => {
                let (packet_id, m) = u16::decode(advance(stream, n)?)?;
                (Some(packet_id), m)
            }
            _ => (None, 0),
        };
        n += m;

        let (properties, m) = dec_props!(PublishProperties, stream, n)?;
        n += m;

        let (payload, m) = match advance(stream, n)? {
            stream if stream.len() == 0 => (None, 0),
            stream => {
                let (payload, n) = (stream.to_vec(), stream.len());
                match properties.as_ref().map(|p| p.payload_format_indicator.is_utf8()) {
                    Some(false) | None => (Some(payload), n),
                    Some(true) => match std::str::from_utf8(&payload) {
                        Ok(_) => (Some(payload), n),
                        Err(err) => err!(
                            ProtocolError,
                            code: PayloadFormatInvalid,
                            cause: err,
                            "payload format invalid in publish"
                        )?,
                    },
                }
            }
        };
        n += m;

        let val = Publish {
            retain,
            qos,
            duplicate,
            topic_name,
            packet_id,
            properties,
            payload,
        };

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

        Ok(Blob::Large { data })
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct PublishProperties {
    pub payload_format_indicator: PayloadFormat, // default=PayloadFormat::Binary
    pub message_expiry_interval: Option<u32>,
    pub topic_alias: Option<u16>,
    pub response_topic: Option<TopicName>,
    pub correlation_data: Option<Vec<u8>>,
    pub subscribtion_identifier: Option<VarU32>,
    pub content_type: Option<String>,
    pub user_properties: Vec<UserProperty>,
}

impl Packetize for PublishProperties {
    fn decode(stream: &[u8]) -> Result<(Self, usize)> {
        let mut dups = [false; 256];
        let mut props = PublishProperties::default();

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
                Property::PayloadFormatIndicator(val) => {
                    props.payload_format_indicator = val.try_into()?;
                }
                Property::MessageExpiryInterval(val) => {
                    props.message_expiry_interval = Some(val);
                }
                Property::TopicAlias(0) => {
                    err!(ProtocolError, code: ProtocolError, "topic-alias is ZERO")?
                }
                Property::TopicAlias(val) => {
                    props.topic_alias = Some(val);
                }
                Property::ResponseTopic(val) => {
                    props.response_topic = Some(val);
                }
                Property::CorrelationData(val) => {
                    props.correlation_data = Some(val);
                }
                Property::SubscriptionIdentifier(val) => {
                    props.subscribtion_identifier = Some(val);
                }
                Property::ContentType(val) => {
                    props.content_type = Some(val);
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

        if self.payload_format_indicator.is_utf8() {
            let val = u8::from(PayloadFormat::Utf8);
            enc_prop!(data, PayloadFormatIndicator, val);
        }
        enc_prop!(opt: data, MessageExpiryInterval, self.message_expiry_interval);
        enc_prop!(opt: data, TopicAlias, self.topic_alias);
        enc_prop!(opt: data, ResponseTopic, &self.response_topic);
        enc_prop!(opt: data, CorrelationData, &self.correlation_data);
        enc_prop!(opt: data, SubscriptionIdentifier, self.subscribtion_identifier);
        enc_prop!(opt: data, ContentType, &self.content_type);

        for uprop in self.user_properties.iter() {
            enc_prop!(data, UserProp, uprop);
        }

        data = insert_property_len(data.len(), data)?;

        Ok(Blob::Large { data })
    }
}
