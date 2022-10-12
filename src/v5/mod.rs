//! Module can speak MQTT Version-5 protocol.

#[cfg(any(feature = "fuzzy", test))]
use arbitrary::{Error as ArbitraryError, Unstructured};

#[cfg(any(feature = "fuzzy", test))]
use std::result;

use crate::{Packetize, Result, VarU32};

// TODO: review all v5::* code to check error-kind, must either be MalformedPacket or
//       ProtocolError.

/// MQTT packetization, decode a single field.
macro_rules! dec_field {
    ($type:ty, $stream:expr, $n:expr; $($pred:tt)*) => {{
        if $($pred)* {
            let (val, m) = <$type>::decode(crate::util::advance($stream, $n)?)?;
            (Some(val), $n + m)
        } else {
            (None, $n)
        }
    }};
    ($type:ty, $stream:expr, $n:expr) => {{
        let (val, m) = <$type>::decode(crate::util::advance($stream, $n)?)?;
        (val, $n + m)
    }};
}

/// MQTT packetization, decode a single property.
macro_rules! dec_prop {
    ($varn:ident, $valtype:ty, $stream:expr) => {{
        let (val, n) = <$valtype>::decode($stream)?;
        (Property::$varn(val), n)
    }};
}

/// MQTT packetization, decode a list of properties.
macro_rules! dec_props {
    ($type:ty, $stream:expr, $n:expr; $($pred:tt)*) => {{
        use crate::util::advance;

        if $($pred)* {
            match VarU32::decode(advance($stream, $n)?)? {
                (VarU32(0), m) => (None, $n + m),
                (VarU32(p), m) => {
                    let (properties, r) = <$type>::decode(advance($stream, $n)?)?;
                    let p = usize::try_from(p)?;
                    if r == (m + p) {
                        (Some(properties), $n + r)
                    } else {
                        err!(
                            ProtocolError,
                            code: ProtocolError,
                            "property len mismatching {}",
                            r
                        )?
                    }
                }
            }
        } else {
            (None, $n)
        }
    }};
    ($type:ty, $stream:expr, $n:expr) => {{
        use crate::util::advance;

        match VarU32::decode(advance($stream, $n)?)? {
            (VarU32(0), m) => (None, $n + m),
            (VarU32(p), m) => {
                let (properties, r) = <$type>::decode(advance($stream, $n)?)?;
                let p = usize::try_from(p)?;
                if r == (m + p) {
                    (Some(properties), $n + r)
                } else {
                    err!(
                        ProtocolError,
                        code: ProtocolError,
                        "property len mismatching {}",
                        r
                    )?
                }
            }
        }
    }};
}

/// MQTT packetization, enocde a single property.
macro_rules! enc_prop {
    (opt: $data:ident, $varn:ident, $($val:tt)*) => {{
        if let Some(val) = $($val)* {
            let pt = PropertyType::$varn as u32;
            $data.extend_from_slice(VarU32(pt).encode()?.as_ref());
            $data.extend_from_slice(val.encode()?.as_ref())
        }
    }};
    ($data:ident, $varn:ident, $($val:tt)*) => {{
        // println!("enc_prop {:?} {:?}", PropertyType::$varn, $data);
        $data.extend_from_slice(VarU32(PropertyType::$varn as u32).encode()?.as_ref());
        $data.extend_from_slice($($val)*.encode()?.as_ref());
        // println!("enc_prop out {:?}", $data);
    }};
}

mod config;
mod packet;
mod protocol;
mod types;

pub use config::Config;
pub use packet::{MQTTRead, MQTTWrite};
pub use protocol::{Protocol, Socket};
pub use types::{FixedHeader, MqttProtocol, Packet, Property};
pub use types::{PayloadFormat, PropertyType, UserProperty};

mod auth;
mod connack;
mod connect;
mod disconnect;
mod ping;
mod pubaclc;
mod publish;
mod sub;
mod suback;
mod unsub;
mod unsuback;

pub use auth::{Auth, AuthProperties, AuthReasonCode};
pub use connack::{ConnAck, ConnAckFlags, ConnAckProperties, ConnAckReasonCode};
pub use connect::WillProperties;
pub use connect::{Connect, ConnectFlags, ConnectPayload, ConnectProperties};
pub use disconnect::{DisconnProperties, DisconnReasonCode, Disconnect};
pub use ping::{PingReq, PingResp};
pub use pubaclc::{Pub, PubProperties};
pub use publish::{Publish, PublishProperties};
pub use sub::{Subscribe, SubscribeFilter, SubscribeProperties, SubscriptionOpt};
pub use suback::{SubAck, SubAckProperties, SubAckReasonCode};
pub use unsub::{UnSubscribe, UnSubscribeProperties};
pub use unsuback::{UnsubAck, UnsubAckProperties, UnsubAckReasonCode};

#[cfg(feature = "client")]
pub mod client;

#[cfg(any(feature = "fuzzy", test))]
pub fn valid_user_props<'a>(
    uns: &mut Unstructured<'a>,
    n: usize, // TODO: increase n to make size > 1/2 byte.
) -> result::Result<Vec<UserProperty>, ArbitraryError> {
    let mut props = vec![];
    for _ in 0..n {
        let keys: Vec<String> =
            vec!["", "key"].into_iter().map(|s| s.to_string()).collect();
        let vals: Vec<String> =
            vec!["", "val"].into_iter().map(|s| s.to_string()).collect();

        let key: String = uns.choose(&keys)?.to_string();
        let val: String = uns.choose(&vals)?.to_string();
        props.push((key, val))
    }

    Ok(props)
}

fn insert_fixed_header(fh: FixedHeader, mut data: Vec<u8>) -> Result<Vec<u8>> {
    let a = data.len();

    let fh_blob = fh.encode()?;
    let fh_bytes = fh_blob.as_ref();
    let n = fh_bytes.len();

    data.extend_from_slice(fh_bytes);
    data.copy_within(..a, n);
    (&mut data[..n]).copy_from_slice(fh_bytes);

    Ok(data)
}

fn insert_property_len(n: usize, mut data: Vec<u8>) -> Result<Vec<u8>> {
    let a = data.len();

    let n = u32::try_from(n)?;

    let blob = VarU32(n).encode()?;
    let bytes = blob.as_ref();
    let m = bytes.len();

    data.extend_from_slice(bytes);
    data.copy_within(..a, m);
    (&mut data[..m]).copy_from_slice(bytes);

    Ok(data)
}

//TODO
//#[cfg(any(feature = "fuzzy", test))]
//#[path = "mod_fuzzy.rs"]
//mod mod_fuzzy;
