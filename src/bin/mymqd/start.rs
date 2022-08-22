use log::info;

use std::sync::mpsc;

use crate::{Opt, Result, SubCommand};
use mymq::broker::{Cluster, Config};

pub fn run(opts: Opt) -> Result<()> {
    let (tx, rx) = mpsc::sync_channel(2);
    let ctrlc_tx = tx.clone();
    ctrlc::set_handler(move || ctrlc_tx.send("ctrlc".to_string()).unwrap()).unwrap();

    let config = parse_config(&opts).map_err(|e| e.to_string())?;
    let cluster = {
        let cluster = Cluster::from_config(&config).map_err(|e| e.to_string())?;
        cluster.spawn(tx.clone()).map_err(|e| e.to_string())?
    };

    println!("{}", rx.recv().unwrap());

    // TODO: print the fin-stats
    cluster.close_wait();

    Ok(())
}

fn parse_config(opts: &Opt) -> Result<Config> {
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

    config = parse_cmd_opts(opts, parse_env(opts, config)?)?;

    Ok(config)
}

fn parse_cmd_opts(opts: &Opt, mut config: Config) -> Result<Config> {
    match &opts.subcmd {
        SubCommand::Start { name, port, num_shards } => {
            config.name = name.clone();
            config.port = port.clone();
            config.num_shards = num_shards.clone();
        }
        _ => unreachable!(),
    }

    Ok(config)
}

fn parse_env(_opts: &Opt, config: Config) -> Result<Config> {
    Ok(config)
}
