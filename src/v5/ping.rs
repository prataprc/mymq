#[cfg(any(feature = "fuzzy", test))]
use arbitrary::Arbitrary;

use crate::v5::{Blob, FixedHeader, PacketType, Result, VarU32};
use crate::Packetize;

/// PINGREQ Packet
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct PingReq;

impl Packetize for PingReq {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (fh, n) = dec_field!(FixedHeader, stream, 0);
        fh.validate()?;

        Ok((PingReq, n))
    }

    fn encode(&self) -> Result<Blob> {
        let mut data = [0_u8; 32];

        let fh = FixedHeader::new(PacketType::PingReq, VarU32(0))?;
        data[..2].copy_from_slice(fh.encode()?.as_ref());

        Ok(Blob::Small { data, size: 2 })
    }
}

/// PINGRESP Packet
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct PingResp;

impl Packetize for PingResp {
    fn decode<T: AsRef<[u8]>>(stream: T) -> Result<(Self, usize)> {
        let stream: &[u8] = stream.as_ref();

        let (fh, n) = dec_field!(FixedHeader, stream, 0);
        fh.validate()?;

        Ok((PingResp, n))
    }

    fn encode(&self) -> Result<Blob> {
        let mut data = [0_u8; 32];

        let fh = FixedHeader::new(PacketType::PingResp, VarU32(0))?;
        data[..2].copy_from_slice(fh.encode()?.as_ref());

        Ok(Blob::Small { data, size: 2 })
    }
}
