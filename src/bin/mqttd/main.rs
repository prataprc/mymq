use log::info;
use structopt::StructOpt;

use std::{io, path};

use mqtr::Config;

#[derive(Clone, StructOpt)]
pub struct Opt {
    #[structopt(long = "config")]
    config_loc: Option<path::PathBuf>,

    #[structopt(short = "v")]
    v: bool,

    #[structopt(long = "vv")]
    vv: bool,

    #[structopt(long = "no-log")]
    no_log: bool,

    #[structopt(long = "error")]
    error: bool,

    #[structopt(long = "warn")]
    warn: bool,

    #[structopt(long = "info")]
    info: bool,

    #[structopt(long = "debug")]
    debug: bool,

    #[structopt(long = "trace")]
    trace: bool,

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
    },
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

    fn to_log_filter(&self) -> log::LevelFilter {
        if self.no_log {
            log::LevelFilter::Off
        } else if self.error {
            log::LevelFilter::Error
        } else if self.warn {
            log::LevelFilter::Warn
        } else if self.info {
            log::LevelFilter::Info
        } else if self.debug {
            log::LevelFilter::Debug
        } else if self.trace {
            log::LevelFilter::Trace
        } else {
            log::LevelFilter::Info
        }
    }
}

fn main() {
    let opts = parse_cmd_line();
    setup_logging(&opts);

    match &opts.config_loc {
        Some(path) => {
            info!("config_location {:?}", path.to_str());
        }
        None => info!("Using default configuration for mqtt broker"),
    }
    info!("verbosity level {:?}", opts.to_verbosity());

    Config::default();
}

fn parse_cmd_line() -> Opt {
    Opt::from_args()
}

//fn parse_config(mut config: Config, opts: &Opt) -> Config {
//    // TODO: environment variables can be consumed here. Configuration parameter values
//    // take preference in the following order of decreasing preference:
//    // a. Command line options.
//    // b. Environment variables.
//    // c. Toml configuration file.
//    // d. System defaults.
//    match &opts.subcmd {
//        SubCommand::Start { name, port } => {
//            if let Some(name) = name {
//                config.name = name.clone();
//            }
//            if let Some(port) = port {
//                config.port = Some(port.clone())
//            }
//        }
//    }
//
//    config
//}

use env_logger::{Builder, WriteStyle};
use log::Level;
use std::io::Write;
fn setup_logging(opts: &Opt) {
    let verbosity = opts.to_verbosity();
    let level_filter = opts.to_log_filter();
    Builder::from_default_env()
        .format(move |f, r| {
            let target = r.target();
            match target {
                "0" | "1" | "2" if target <= verbosity.as_str() => format(f, r),
                "0" | "1" | "2" => Ok(()),
                _ => format(f, r),
            }
        })
        .filter_level(level_filter)
        .write_style(WriteStyle::Auto)
        .init();
}

use env_logger::fmt::{Color, Formatter};
fn format(f: &mut Formatter, r: &log::Record<'_>) -> io::Result<()> {
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
        let file = path::Path::new(r.file().clone().unwrap()).file_name().unwrap();
        file.to_str().unwrap()
    };
    let loc = mod_style.value(format!("{}:{}", file, r.line().unwrap()));
    writeln!(
        f,
        "{} [{:5}] [{:>10}] {}",
        chrono::Local::now().format("%Y-%m-%dT%H:%M:%.3f%Z"),
        level_style.value(r.level()),
        loc,
        r.args()
    )
}
