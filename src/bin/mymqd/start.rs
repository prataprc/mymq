use log::info;
use structopt::StructOpt;

use std::sync::mpsc;

use crate::{Opt, Result, SubCommand};
use mymq::broker::{Cluster, Config};

#[derive(Clone, StructOpt)]
pub struct Start {
    #[structopt(long = "name", default_value = "mymqd")]
    name: String,

    #[structopt(long = "port", default_value = "1883")]
    port: u16,

    #[structopt(long = "num-shards", default_value = "1")]
    num_shards: u32,
}

pub fn run(opts: Opt) -> Result<()> {
    let start = match &opts.subcmd {
        SubCommand::Start(start) => start.clone(),
        _ => unreachable!(),
    };

    let (tx, rx) = mpsc::sync_channel(2);
    let ctrlc_tx = tx.clone();
    ctrlc::set_handler(move || ctrlc_tx.send("ctrlc".to_string()).unwrap()).unwrap();

    let config = parse_config(&opts, start).map_err(|e| e.to_string())?;
    let cluster = {
        let cluster = Cluster::from_config(&config).map_err(|e| e.to_string())?;
        cluster.spawn(tx.clone()).map_err(|e| e.to_string())?
    };

    println!("{}", rx.recv().unwrap());

    // TODO: print the fin-stats
    cluster.close_wait();

    Ok(())
}

fn parse_config(opts: &Opt, start: Start) -> Result<Config> {
    // Environment variables can be consumed here. Configuration parameters take
    // preference in the following order of decreasing preference:
    // a. Command line options.
    // b. Environment variables.
    // c. Toml configuration file.
    // d. System defaults.
    let mut config = match &opts.config_loc {
        Some(path) => {
            info!("config_location {:?}", path.to_str());
            Config::from_file(path).map_err(|e| e.to_string())?
        }
        None => {
            info!("Using default configuration for mqtt broker");
            Config::default()
        }
    };

    config = parse_cmd_opts(opts, start, parse_env(opts, config)?)?;

    Ok(config)
}

fn parse_cmd_opts(_opts: &Opt, start: Start, mut config: Config) -> Result<Config> {
    config.name = start.name.clone();
    config.port = start.port.clone();
    config.num_shards = start.num_shards.clone();

    Ok(config)
}

fn parse_env(_opts: &Opt, config: Config) -> Result<Config> {
    Ok(config)
}
