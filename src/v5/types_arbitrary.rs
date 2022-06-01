use arbitrary::{Arbitrary, Unstructured};
use bytes::{BufMut, BytesMut};
use rand::{rngs::StdRng, Rng};

use super::super::common::{Context, TestArbitrary, TestBinary, TestValue};
use super::*;

impl TestArbitrary<u8> for u8 {
    fn value(rng: &mut StdRng, _ctx: &mut Context) -> TestValue<u8> {
        let val: u8 = rng.gen();
        TestValue::Good { val }
    }

    fn binary(rng: &mut StdRng, _ctx: &mut Context) -> TestBinary {
        let mut stream = BytesMut::new();
        stream.put_u8(rng.gen());
        TestBinary::Good {
            stream: stream.into(),
        }
    }
}

impl TestArbitrary<u16> for u16 {
    fn value(rng: &mut StdRng, _ctx: &mut Context) -> TestValue<u16> {
        let val: u16 = rng.gen();
        TestValue::Good { val }
    }

    fn binary(rng: &mut StdRng, _ctx: &mut Context) -> TestBinary {
        let mut stream = BytesMut::new();
        stream.put_u16(rng.gen());
        TestBinary::Good {
            stream: stream.into(),
        }
    }
}

impl TestArbitrary<u32> for u32 {
    fn value(rng: &mut StdRng, _ctx: &mut Context) -> TestValue<u32> {
        let val: u32 = rng.gen();
        TestValue::Good { val }
    }

    fn binary(rng: &mut StdRng, _ctx: &mut Context) -> TestBinary {
        let mut stream = BytesMut::new();
        stream.put_u32(rng.gen());
        TestBinary::Good {
            stream: stream.into(),
        }
    }
}

impl TestArbitrary<VarU32> for VarU32 {
    fn value(rng: &mut StdRng, ctx: &mut Context) -> TestValue<VarU32> {
        match rng.gen::<u32>() {
            val if (val % 1_000) > 0 => {
                ctx.incr_valid_type("var-u32");
                let val = VarU32(val % (*VarU32::MAX + 1));
                TestValue::Good { val }
            }
            val => {
                ctx.incr_invalid_type("var-u32");
                let val = VarU32(*VarU32::MAX + (val % 1_000) + 1);
                TestValue::Bad {
                    val,
                    err: err!(InvalidInput, desc: ""),
                }
            }
        }
    }

    fn binary(rng: &mut StdRng, ctx: &mut Context) -> TestBinary {
        let mut stream = BytesMut::new();
        match rng.gen::<u32>() {
            val if (val % 1_000) > 0 => {
                ctx.incr_valid_type("var-u32");
                VarU32(val).write(&mut stream).unwrap();
                TestBinary::Good {
                    stream: stream.into(),
                }
            }
            val => {
                ctx.incr_invalid_type("var-u32");
                VarU32(33_554_432 + (val % 1_000))
                    .write(&mut stream)
                    .unwrap();
                TestBinary::Bad {
                    stream: stream.into(),
                    err: err!(MalformedPacket, code: MalformedPacket, ""),
                }
            }
        }
    }
}
