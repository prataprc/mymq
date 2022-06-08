use arbitrary::Unstructured;
use rand::{rngs::StdRng, Rng};

use std::{collections::BTreeMap, fmt};

use crate::v5::{PacketType, QoS};
use crate::{Error, ErrorKind, Packetize, ReasonCode, Result};

const INVALID_RATIO: f32 = 0.001;

pub struct Context {
    pub server: bool,
    pub packet_type: PacketType,
    pub max_qos: QoS,
    pub invalid_ratio: BTreeMap<&'static str, f32>,
    // statistics
    pub type_stats: BTreeMap<&'static str, (usize, usize)>,
}

impl Default for Context {
    fn default() -> Context {
        let mut ctx = Context {
            server: false,
            packet_type: PacketType::Publish,
            max_qos: QoS::AtLeastOnce,
            invalid_ratio: BTreeMap::new(),
            // statistics
            type_stats: BTreeMap::new(),
        };
        ctx.invalid_ratio.insert("string", INVALID_RATIO);
        ctx.invalid_ratio.insert("bytes", INVALID_RATIO);
        ctx
    }
}

impl Context {
    pub fn incr_valid_type(&mut self, key: &'static str) {
        match self.type_stats.get_mut(key) {
            Some((count, _)) => *count += 1,
            None => {
                self.type_stats.insert(key, (1, 0));
            }
        }
    }

    pub fn incr_invalid_type(&mut self, key: &'static str) {
        match self.type_stats.get_mut(key) {
            Some((_, count)) => *count += 1,
            None => {
                self.type_stats.insert(key, (0, 1));
            }
        }
    }

    pub fn get_invalid_ratio(&self, key: &str) -> f32 {
        match self.invalid_ratio.get(key) {
            Some(val) => *val,
            None => INVALID_RATIO,
        }
    }

    pub fn print_stats(&self) {
        for (key, val) in self.type_stats.iter() {
            println!("{:20}: {:?}", key, val)
        }
    }
}

pub enum FuzzyValue<T> {
    Good { val: T },
    Bad { val: T, err: Error },
}

pub enum FuzzyBinary {
    Good { stream: Vec<u8> },
    Bad { stream: Vec<u8>, err: Error },
}

pub trait Fuzzy: Sized {
    fn type_name() -> &'static str;

    fn valid_value(rng: &mut StdRng, ctx: &mut Context) -> Self;

    fn invalid_value(rng: &mut StdRng, ctx: &mut Context) -> Option<(Self, Error)>;

    fn valid_binary(rng: &mut StdRng, ctx: &mut Context) -> Vec<u8>;

    fn invalid_binary(rng: &mut StdRng, ctx: &mut Context) -> Option<(Vec<u8>, Error)>;

    fn value(rng: &mut StdRng, ctx: &mut Context) -> FuzzyValue<Self> {
        let type_name = Self::type_name();
        let ratio = ctx.get_invalid_ratio(type_name);
        match rng.gen::<u32>() % ((1.0 / ratio) as u32) {
            0 => match Self::invalid_value(rng, ctx) {
                Some((val, err)) => {
                    ctx.incr_invalid_type(type_name);
                    FuzzyValue::Bad { val, err }
                }
                None => {
                    ctx.incr_valid_type(type_name);
                    FuzzyValue::Good { val: Self::valid_value(rng, ctx) }
                }
            },
            _ => {
                ctx.incr_valid_type(type_name);
                FuzzyValue::Good { val: Self::valid_value(rng, ctx) }
            }
        }
    }

    fn binary(rng: &mut StdRng, ctx: &mut Context) -> FuzzyBinary {
        let type_name = Self::type_name();
        let ratio = ctx.get_invalid_ratio(type_name);
        match rng.gen::<u32>() % ((1.0 / ratio) as u32) {
            0 => match Self::invalid_binary(rng, ctx) {
                Some((stream, err)) => {
                    ctx.incr_invalid_type(type_name);
                    FuzzyBinary::Bad { stream, err }
                }
                None => {
                    ctx.incr_valid_type(type_name);
                    FuzzyBinary::Good { stream: Self::valid_binary(rng, ctx) }
                }
            },
            _ => {
                ctx.incr_valid_type(type_name);
                FuzzyBinary::Good { stream: Self::valid_binary(rng, ctx) }
            }
        }
    }

    fn validate(&self, _: &mut Context) -> Result<()> {
        Ok(())
    }
}

// TODO: generate large string, but < 64K
impl Fuzzy for String {
    fn type_name() -> &'static str {
        "string"
    }

    fn valid_value(rng: &mut StdRng, _: &mut Context) -> Self {
        use crate::is_invalid_utf8_code_point;

        loop {
            let s: String = {
                let bytes = rng.gen::<[u8; 32]>();
                let mut uns = Unstructured::new(&bytes);
                uns.arbitrary().unwrap()
            };
            match s.chars().any(is_invalid_utf8_code_point) {
                false => break s,
                true => (),
            }
        }
    }

    fn invalid_value(rng: &mut StdRng, _: &mut Context) -> Option<(Self, Error)> {
        use crate::is_invalid_utf8_code_point;

        loop {
            let s: String = {
                let bytes = rng.gen::<[u8; 32]>();
                let mut uns = Unstructured::new(&bytes);
                uns.arbitrary().unwrap()
            };
            match s.chars().any(is_invalid_utf8_code_point) {
                true => {
                    let e: Result<()> = err!(InvalidInput, desc: "");
                    break Some((s, e.unwrap_err()));
                }
                false => continue,
            }
        }
    }

    fn valid_binary(rng: &mut StdRng, ctx: &mut Context) -> Vec<u8> {
        Self::valid_value(rng, ctx).encode().unwrap().as_ref().to_vec()
    }

    fn invalid_binary(rng: &mut StdRng, ctx: &mut Context) -> Option<(Vec<u8>, Error)> {
        let (val, _) = Self::invalid_value(rng, ctx).unwrap();
        assert!(val.len() < 65536);

        let mut err = {
            let e: Result<()> = err!(MalformedPacket, desc: "");
            e.unwrap_err()
        };

        let mut data = Vec::with_capacity(2 + val.len());
        data.extend_from_slice(&(val.len() as u16).to_be_bytes());
        data.extend_from_slice(val.as_bytes());

        if (rng.gen::<u32>() % 100) > 90 {
            data[0] = 0xff;
            data[1] = 0xff;
            err = {
                let e: Result<()> =
                    err!(InsufficientBytes, code: MalformedPacket, cause: err, "");
                e.unwrap_err()
            };
        }

        Some((data, err))
    }
}

// TODO: generate large bytes, but < 64K
impl Fuzzy for Vec<u8> {
    fn type_name() -> &'static str {
        "bytes"
    }

    fn valid_value(rng: &mut StdRng, _: &mut Context) -> Self {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);
        uns.arbitrary().unwrap()
    }

    fn invalid_value(_: &mut StdRng, _: &mut Context) -> Option<(Self, Error)> {
        None
    }

    fn valid_binary(rng: &mut StdRng, ctx: &mut Context) -> Vec<u8> {
        Self::valid_value(rng, ctx).encode().unwrap().as_ref().to_vec()
    }

    fn invalid_binary(_: &mut StdRng, _: &mut Context) -> Option<(Vec<u8>, Error)> {
        None
    }
}

pub fn dna_string(rng: &mut StdRng, mut n: usize) -> Vec<u8> {
    let mut rand_bytes: [u8; 32] = rng.gen();

    let mut out = Vec::default();
    while n > 32 {
        out.extend_from_slice(&rand_bytes);
        rand_bytes = rng.gen();
        n -= 32;
    }

    out.extend_from_slice(&rand_bytes[..n]);

    out
}

pub fn test<T>(rng: &mut StdRng, ctx: &mut Context)
where
    T: PartialEq + Clone + Fuzzy + Packetize + fmt::Debug,
{
    use crate::Blob;

    match T::value(rng, ctx) {
        FuzzyValue::Good { val } => {
            let stream: Blob = val.encode().unwrap();
            assert_eq!(
                (val.clone(), stream.as_ref().len()),
                T::decode(stream.as_bytes()).unwrap()
            );
            assert_eq!(
                (val.clone(), stream.as_ref().len()),
                T::decode_unchecked(stream.as_bytes())
            );

            let stream: Blob = val.clone().into_blob().unwrap();
            assert_eq!(
                (val.clone(), stream.as_ref().len()),
                T::decode(stream.as_bytes()).unwrap()
            );
            assert_eq!(
                (val.clone(), stream.as_ref().len()),
                T::decode_unchecked(stream.as_bytes())
            );
        }
        FuzzyValue::Bad { val, err } if err.kind() == ErrorKind::NoError => {
            val.encode().unwrap();
        }
        FuzzyValue::Bad { val, err } => {
            assert!(val.encode().unwrap_err().has(err.kind()), "{:?}", err);
        }
    }

    match T::binary(rng, ctx) {
        FuzzyBinary::Good { stream } => {
            let (val, n) = T::decode(&stream).unwrap();
            assert_eq!(n, stream.len());
            let blob: Blob = val.encode().unwrap();
            assert_eq!(
                (stream.as_ref(), stream.len()),
                (blob.as_ref(), blob.as_ref().len())
            );
            let blob: Blob = val.into_blob().unwrap();
            assert_eq!(
                (stream.as_ref(), stream.len()),
                (blob.as_ref(), blob.as_ref().len())
            );

            let (val, n) = T::decode_unchecked(&stream);
            assert_eq!(n, stream.len());
            let blob: Blob = val.encode().unwrap();
            assert_eq!(
                (stream.as_ref(), stream.len()),
                (blob.as_ref(), blob.as_ref().len())
            );
            let blob: Blob = val.into_blob().unwrap();
            assert_eq!(
                (stream.as_ref(), stream.len()),
                (blob.as_ref(), blob.as_ref().len())
            );
        }
        FuzzyBinary::Bad { stream, err } => {
            let ee = match T::decode(&stream) {
                Ok((val, _)) => val.validate(ctx).unwrap_err(),
                Err(err) => err,
            };
            assert!(ee.has(err.kind()), "{} {:?}", err.kind(), ee.kinds());
        }
    }
}
