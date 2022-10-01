use log::info;
use structopt::StructOpt;

use std::sync::mpsc;

use crate::{Opt, Result, SubCommand};
use mymq::broker::{Cluster, Config};

#[derive(Clone, StructOpt)]
pub struct Show {
    #[structopt(long = "uuid-v5")]
    uuid_v5: bool,
}

pub fn run(opts: Opt) -> Result<()> {
    let show = match &opts.subcmd {
        SubCommand::Show(show) => show.clone(),
        _ => unreachable!(),
    };

    if show.uuid_v5 {
        println!("{}", uuid::Uuid::new_v4().to_string())
    }

    Ok(())
}
