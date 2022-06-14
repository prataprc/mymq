#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeReasonCode {
    QoS0 = 0,
    QoS1 = 1,
    QoS2 = 2,
    Unspecified = 128,
    ImplementationSpecific = 131,
    NotAuthorized = 135,
    TopicFilterInvalid = 143,
    PkidInUse = 145,
    QuotaExceeded = 151,
    SharedSubscriptionsNotSupported = 158,
    SubscriptionIdNotSupported = 161,
    WildcardSubscriptionsNotSupported = 162,
}

impl TryFrom<u8> for SubscribeReasonCode {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let v = match value {
            0x00 => SubscribeReasonCode::QoS0,
            0x01 => SubscribeReasonCode::QoS1,
            0x02 => SubscribeReasonCode::QoS2,
            0x80 => SubscribeReasonCode::Unspecified,
            0x83 => SubscribeReasonCode::ImplementationSpecific,
            0x87 => SubscribeReasonCode::NotAuthorized,
            0x8f => SubscribeReasonCode::TopicFilterInvalid,
            0x91 => SubscribeReasonCode::PkidInUse,
            0x97 => SubscribeReasonCode::QuotaExceeded,
            0x9e => SubscribeReasonCode::SharedSubscriptionsNotSupported,
            0xa1 => SubscribeReasonCode::SubscriptionIdNotSupported,
            0xa2 => SubscribeReasonCode::WildcardSubscriptionsNotSupported,
            val => err!(ProtocolError, code: ProtocolError, "reason-code {:?}", val),
        };

        Ok(v)
    }
}

/// Acknowledgement to subscribe
#[derive(Debug, Clone, PartialEq)]
pub struct SubAck {
    pub packet_id: u16,
    pub return_codes: Vec<SubscribeReasonCode>,
    pub properties: Option<SubAckProperties>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubAckProperties {
    pub reason_string: Option<String>,
    pub user_properties: Vec<UserProperty>,
}

pub fn codes(c: Vec<u8>) -> Vec<SubscribeReasonCode> {
    c.into_iter()
        .map(|v| match qos(v).unwrap() {
            QoS::AtMostOnce => SubscribeReasonCode::QoS0,
            QoS::AtLeastOnce => SubscribeReasonCode::QoS1,
        })
        .collect()
}
