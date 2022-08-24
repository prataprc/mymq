#[cfg(any(feature = "fuzzy", test))]
use arbitrary::{Arbitrary, Error as ArbitraryError, Unstructured};

use std::{fmt, result};

use crate::v5::{FixedHeader, PacketType, Property, PropertyType};
use crate::{util::advance, Blob, Packetize, UserProperty, VarU32};
use crate::{Error, ErrorKind, ReasonCode, Result};

const PP: &'static str = "Packet::Auth";

/// Error codes allowed in AUTH packet.
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum AuthReasonCode {
    Success = 0x00,
    ContinueAuthentication = 0x18,
    ReAuthenticate = 0x19,
}

impl fmt::Display for AuthReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        use AuthReasonCode::*;

        match self {
            Success => write!(f, "success"),
            ContinueAuthentication => write!(f, "continue_authentication"),
            ReAuthenticate => write!(f, "re_authenticate"),
        }
    }
}

impl TryFrom<u8> for AuthReasonCode {
    type Error = Error;

    fn try_from(val: u8) -> Result<AuthReasonCode> {
        match val {
            0x00 => Ok(AuthReasonCode::Success),
            0x18 => Ok(AuthReasonCode::ContinueAuthentication),
            0x19 => Ok(AuthReasonCode::ReAuthenticate),
            val => err!(ProtocolError, code: ProtocolError, "{} reason-code {}", PP, val),
        }
    }
}

/// AUTH packet
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Auth {
    pub code: AuthReasonCode,
    pub properties: Option<AuthProperties>,
}

impl fmt::Display for Auth {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "AUTH code:{}", self.code)?;
        if let Some(properties) = &self.properties {
            let mut props = Vec::default();
            props.push(format!(
                "  authentication_method: {:?}",
                properties.authentication_method
            ));
            props.push(format!(
                "  authentication_data: {}",
                properties.authentication_data.len()
            ));
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
impl<'a> Arbitrary<'a> for Auth {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        let val = Auth {
            code: uns.arbitrary()?,
            properties: uns.arbitrary()?,
        };

        Ok(val)
    }
}

impl Packetize for Auth {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (fh, n) = dec_field!(FixedHeader, stream, 0);
        fh.validate()?;

        let (code, properties, n) = if *fh.remaining_len == 0 {
            (AuthReasonCode::Success, None, n)
        } else {
            let (code, n) = dec_field!(u8, stream, n);
            let code = AuthReasonCode::try_from(code)?;
            let (properties, n) = dec_props!(AuthProperties, stream, n);
            (code, properties, n)
        };

        let val = Auth { code, properties };

        val.validate()?;
        Ok((val, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::v5::insert_fixed_header;

        let mut data = Vec::with_capacity(64);

        data.extend_from_slice((self.code as u8).encode()?.as_ref());
        if let Some(properties) = &self.properties {
            data.extend_from_slice(properties.encode()?.as_ref());
        } else {
            data.extend_from_slice(VarU32(0).encode()?.as_ref());
        }

        let fh = FixedHeader::new(PacketType::Auth, VarU32(data.len().try_into()?))?;
        data = insert_fixed_header(fh, data)?;

        Ok(Blob::Large { data })
    }
}

impl Auth {
    pub fn normalize(&mut self) {
        ()
    }

    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

/// Collection of MQTT properties allowed in AUTH packet
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct AuthProperties {
    /// Property::AuthenticationMethod
    pub authentication_method: String,
    /// Property::AuthenticationData
    pub authentication_data: Vec<u8>,
    /// Property::ReasonString
    pub reason_string: Option<String>,
    /// Property::UserProp
    pub user_properties: Vec<UserProperty>,
}

#[cfg(any(feature = "fuzzy", test))]
impl<'a> Arbitrary<'a> for AuthProperties {
    fn arbitrary(uns: &mut Unstructured<'a>) -> result::Result<Self, ArbitraryError> {
        use crate::types;

        let am_choice: Vec<String> =
            vec!["", "digest"].into_iter().map(|s| s.to_string()).collect();
        let rs_choice: Vec<String> =
            vec!["", "unit-testing"].into_iter().map(|s| s.to_string()).collect();
        let reason_string = match uns.arbitrary::<u8>()? % 2 {
            0 => Some(uns.choose(&rs_choice)?.to_string()),
            1 => None,
            _ => unreachable!(),
        };

        let n_user_props = uns.arbitrary::<usize>()? % 4;
        let val = AuthProperties {
            authentication_method: uns.choose(&am_choice)?.to_string(),
            authentication_data: uns.arbitrary()?,
            reason_string,
            user_properties: types::valid_user_props(uns, n_user_props)?,
        };

        Ok(val)
    }
}

impl Packetize for AuthProperties {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        use crate::v5::Property::*;

        let stream: &[u8] = stream.as_ref();

        let mut dups = [false; 256];
        let mut props = AuthProperties::default();

        let (len, mut n) = dec_field!(VarU32, stream, 0);
        let limit = usize::try_from(*len)? + n;

        let mut authentication_method: Option<String> = None;
        let mut authentication_data: Option<Vec<u8>> = None;
        while n < limit {
            let (property, m) = dec_field!(Property, stream, n);
            n = m;

            let pt = property.to_property_type();
            if pt != PropertyType::UserProp && dups[pt as usize] {
                err!(ProtocolError, code: ProtocolError, "{} repeat prop {:?}", PP, pt)?
            }
            dups[pt as usize] = true;

            match property {
                AuthenticationMethod(val) => authentication_method = Some(val),
                AuthenticationData(val) => authentication_data = Some(val),
                ReasonString(val) => props.reason_string = Some(val),
                UserProp(val) => props.user_properties.push(val),
                _ => {
                    err!(ProtocolError, code: ProtocolError, "{} bad prop {:?}", PP, pt)?
                }
            };
        }

        match authentication_method {
            Some(val) => props.authentication_method = val,
            None => {
                err!(ProtocolError, code: ProtocolError, "{} missing auth-method", PP)?
            }
        }
        match authentication_data {
            Some(val) => props.authentication_data = val,
            None => err!(ProtocolError, code: ProtocolError, "{} missing auth-data", PP)?,
        }

        Ok((props, n))
    }

    fn encode(&self) -> Result<Blob> {
        use crate::v5::insert_property_len;

        let mut data = Vec::with_capacity(64);

        enc_prop!(data, AuthenticationMethod, self.authentication_method);
        enc_prop!(data, AuthenticationData, &self.authentication_data);
        enc_prop!(opt: data, ReasonString, &self.reason_string);

        for uprop in self.user_properties.iter() {
            enc_prop!(data, UserProp, uprop)
        }

        let data = insert_property_len(data.len(), data)?;

        Ok(Blob::Large { data })
    }
}
