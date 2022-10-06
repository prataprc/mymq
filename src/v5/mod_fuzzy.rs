use arbitrary::Unstructured;
use rand::{rngs::StdRng, Rng};

use crate::fuzzy::{self, Fuzzy};
use crate::PacketType;

use super::*;

impl FixedHeader {
    fn invalid(rng: &mut StdRng, ctx: &mut fuzzy::Context) -> (Self, Error) {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);

        let pkt_type: PacketType = uns.arbitrary().unwrap();
        let retain: bool = uns.arbitrary().unwrap();
        let dup: bool = uns.arbitrary().unwrap();
        let qos: QoS = uns.arbitrary().unwrap();
        let byte1 = match pkt_type {
            PacketType::Publish if ctx.maximum_qos < QoS::AtLeastOnce => {
                let qos = [QoS::AtLeastOnce, QoS::ExactlyOnce][rng.gen::<usize>() % 2];
                fixed_byte!(u8::from(pkt_type), retain, qos, dup)
            }
            PacketType::Publish if ctx.maximum_qos < QoS::ExactlyOnce => {
                let qos = QoS::ExactlyOnce;
                fixed_byte!(u8::from(pkt_type), retain, qos, dup)
            }
            PacketType::Publish => {
                let qos = QoS::ExactlyOnce;
                let byte1 = fixed_byte!(u8::from(pkt_type), retain, qos, dup);
                byte1 | 0x06 // set QoS to 3.
            }
            pkt_type if rng.gen::<bool>() => loop {
                let retain: bool = uns.arbitrary().unwrap();
                let dup: bool = uns.arbitrary().unwrap();
                match fixed_byte!(u8::from(pkt_type), retain, qos, dup) {
                    byte1 if (byte1 & 0xf) == 0 => (),
                    byte1 => break byte1,
                }
            },
            _ => fixed_byte!(0, retain, qos, dup),
        };

        let val = FixedHeader { byte1, remaining_len: VarU32(0) };
        let err = {
            let e: Result<()> = err!(MalformedPacket, code: MalformedPacket, "");
            e.unwrap_err()
        };

        (val, err)
    }
}

impl Fuzzy for FixedHeader {
    fn type_name() -> &'static str {
        "fixed-header"
    }

    fn valid_value(rng: &mut StdRng, _: &mut fuzzy::Context) -> Self {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);

        let pkt_type: PacketType = uns.arbitrary().unwrap();

        match pkt_type {
            PacketType::Publish => {
                let retain: bool = uns.arbitrary().unwrap();
                let qos: QoS = uns.arbitrary().unwrap();
                let dup: bool = uns.arbitrary().unwrap();
                FixedHeader::new_publish(retain, qos, dup, VarU32(0)).unwrap()
            }
            _ => FixedHeader::new(pkt_type, VarU32(0)).unwrap(),
        }
    }

    fn invalid_value(
        rng: &mut StdRng,
        ctx: &mut fuzzy::Context,
    ) -> Option<(Self, Error)> {
        let (val, _) = Self::invalid(rng, ctx);
        let err = {
            let e: Result<()> = err!(NoError, desc: "");
            e.unwrap_err()
        };
        Some((val, err))
    }

    fn valid_binary(rng: &mut StdRng, ctx: &mut fuzzy::Context) -> Vec<u8> {
        Self::valid_value(rng, ctx).encode().unwrap().as_ref().to_vec()
    }

    fn invalid_binary(
        rng: &mut StdRng,
        ctx: &mut fuzzy::Context,
    ) -> Option<(Vec<u8>, Error)> {
        let (val, err) = Self::invalid(rng, ctx);
        let val = val.encode().unwrap().as_ref().to_vec();
        Some((val, err))
    }

    fn validate(&self, ctx: &mut fuzzy::Context) -> Result<()> {
        let (pkt_type, _, qos, _) = self.unwrap();
        match pkt_type {
            PacketType::Publish if qos > ctx.maximum_qos => {
                err!(MalformedPacket, code: MalformedPacket, "")
            }
            _ => Ok(()),
        }
    }
}
