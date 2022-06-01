use rand::rngs::StdRng;

use std::collections::BTreeMap;

use crate::v5::{PacketType, QoS};
use crate::Error;

pub struct Context {
    pub server: bool,
    pub packet_type: PacketType,
    pub qos: QoS,
    // statistics
    pub type_stats: BTreeMap<&'static str, (usize, usize)>,
}

impl Context {
    pub fn incr_valid_type(&mut self, key: &'static str) {
        self.type_stats.get_mut(key).unwrap().0 += 1;
    }

    pub fn incr_invalid_type(&mut self, key: &'static str) {
        self.type_stats.get_mut(key).unwrap().1 += 1;
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

pub trait Fuzzy<T> {
    fn value(rng: &mut StdRng, ctx: &mut Context) -> FuzzyValue<T>;

    fn binary(rng: &mut StdRng, ctx: &mut Context) -> FuzzyBinary;
}
