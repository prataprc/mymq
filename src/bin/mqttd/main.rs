use log::{error, info};
use structopt::StructOpt;

use std::{io, path, sync::mpsc};

use mqtr::broker::Config;
use mqtr::Result;

#[derive(Clone, StructOpt)]
pub struct Opt {
    #[structopt(long = "config")]
    config_loc: Option<path::PathBuf>,

    #[structopt(short = "v")]
    v: bool,

    #[structopt(long = "vv")]
    vv: bool,

    #[structopt(long = "log-mod", default_value = "")]
    log_mod: String,

    #[structopt(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clone, StructOpt)]
pub enum SubCommand {
    Start {
        #[structopt(long = "name", default_value = "mqttd")]
        name: String,

        #[structopt(long = "port", default_value = "1883")]
        port: u16,

        #[structopt(long = "num-shards", default_value = "1")]
        num_shards: u32,
    },
}

fn main() {
    use mqtr::broker::Cluster;

    let opts = parse_cmd_line();

    setup_logging(&opts);
    info!("verbosity level {:?}", opts.to_verbosity());

    let (tx, rx) = mpsc::sync_channel(2);
    let ctrlc_tx = tx.clone();
    ctrlc::set_handler(move || ctrlc_tx.send("ctrlc".to_string()).unwrap()).unwrap();

    let config = exit_on_error(parse_config(&opts), 1);
    let cluster = {
        let cluster = exit_on_error(Cluster::from_config(config.clone()), 2);
        exit_on_error(cluster.spawn(tx.clone()), 3)
    };

    println!("{}", rx.recv().unwrap());

    // TODO: print the fin-stats
    cluster.close_wait();
}

fn parse_cmd_line() -> Opt {
    Opt::from_args()
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
            Config::from_file(path)?
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
    }

    Ok(config)
}

fn parse_env(_opts: &Opt, config: Config) -> Result<Config> {
    Ok(config)
}

use env_logger::{fmt::Target, Builder, WriteStyle};
use log::Level;
use std::io::Write;
fn setup_logging(opts: &Opt) {
    let verbosity = opts.to_verbosity();
    let opts = opts.clone();
    Builder::from_default_env()
        .parse_default_env()
        .target(Target::Stdout)
        .format(move |f, r| {
            let file = r.file().clone().unwrap();
            if file.len() > 0 && !file.contains(&opts.log_mod) {
                return Ok(());
            }

            let target = r.target();
            match target {
                "0" | "1" | "2" if target <= verbosity.as_str() => log_format(f, r),
                "0" | "1" | "2" => Ok(()),
                _ => log_format(f, r),
            }
        })
        .write_style(WriteStyle::Auto)
        .init();
}

use env_logger::fmt::{Color, Formatter};
fn log_format(f: &mut Formatter, r: &log::Record<'_>) -> io::Result<()> {
    let mut level_style = f.style();
    match r.level() {
        Level::Error => level_style.set_color(Color::Red).set_bold(false),
        Level::Warn => level_style.set_color(Color::Yellow).set_bold(false),
        Level::Info => level_style.set_color(Color::Blue).set_bold(false),
        Level::Debug => level_style.set_color(Color::Magenta).set_bold(false),
        Level::Trace => level_style.set_color(Color::Cyan).set_bold(false),
    };
    let mut mod_style = f.style();
    mod_style.set_color(Color::Green).set_bold(false);
    let file = {
        let file = path::Path::new(r.file().clone().unwrap()).file_stem().unwrap();
        file.to_str().unwrap()
    };
    let loc = mod_style.value(format!("{}:{}", file, r.line().unwrap()));
    writeln!(
        f,
        "{} [{:5}] [{:>13}] {}",
        chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%Z"),
        level_style.value(r.level()),
        loc,
        r.args()
    )
}

fn exit_on_error<T>(res: Result<T>, code: i32) -> T {
    match res {
        Ok(val) => val,
        Err(err) => {
            error!("exit_on_error code:{} err:{}", code, err);
            std::process::exit(code)
        }
    }
}

impl Opt {
    fn to_verbosity(&self) -> String {
        if self.v {
            "2"
        } else if self.vv {
            "1"
        } else {
            "0"
        }
        .to_string()
    }
}
