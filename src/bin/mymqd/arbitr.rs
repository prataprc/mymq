use rand::{prelude::random, rngs::StdRng, SeedableRng};
use structopt::StructOpt;

use crate::{new_unstructured, Opt, Result, SubCommand};
use mymq::{TopicFilter, TopicName};

#[derive(Clone, StructOpt)]
pub struct Arbitr {
    #[structopt(long = "topic-filter")]
    topic_filter: bool,

    #[structopt(long = "topic-name")]
    topic_name: bool,

    #[structopt(short = "n", default_value = "10")]
    n: usize,
}

pub fn run(opts: Opt) -> Result<()> {
    let arbitr = match &opts.subcmd {
        SubCommand::Arbitr(arbitr) => arbitr.clone(),
        _ => unreachable!(),
    };
    let mut rng = StdRng::seed_from_u64(opts.seed.unwrap_or(random()));

    if arbitr.topic_name {
        println!("Generating {} TopicName:", arbitr.n);
        generate_topic_name(&opts, &arbitr, &mut rng);
    }
    if arbitr.topic_filter {
        println!("Generating {} TopicFilter:", arbitr.n);
        generate_topic_filter(&opts, &arbitr, &mut rng);
    }

    Ok(())
}

fn generate_topic_name(_opts: &Opt, arbitr: &Arbitr, rng: &mut StdRng) {
    let mut bytes = (0..1_000).map(|i| i as u8).collect::<Vec<u8>>();

    for _i in 0..arbitr.n {
        let mut uns = new_unstructured(rng, &mut bytes);
        let topic_name: TopicName = uns.arbitrary().unwrap();
        println!("  {:?}", *topic_name);
    }
}

fn generate_topic_filter(_opts: &Opt, arbitr: &Arbitr, rng: &mut StdRng) {
    let mut bytes = (0..1_000).map(|i| i as u8).collect::<Vec<u8>>();

    for _i in 0..arbitr.n {
        let mut uns = new_unstructured(rng, &mut bytes);
        let topic_filter: TopicFilter = uns.arbitrary().unwrap();
        println!("  {:?}", *topic_filter);
    }
}
