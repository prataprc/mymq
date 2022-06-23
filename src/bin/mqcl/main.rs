use structopt::StructOpt;

use std::{path, process::exit};

use mqtr::{Config, Error, Result};

// TODO: Handle CTRL-C to exit the cluster.

#[derive(Clone, StructOpt)]
pub struct Opt {
    #[structopt(long = "config")]
    config_loc: Option<path::PathBuf>,

    #[structopt(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clone, StructOpt)]
pub enum SubCommand {
    Start {
        #[structopt(long = "name")]
        name: Option<String>,

        #[structopt(long = "port")]
        port: Option<u16>,
    },
}

fn main() {
    setup_logging();
    println!("{:?}", mqtr::v5::PropertyType::UserProp);

    let opts = Opt::from_args();

    let config = match &opts.config_loc {
        Some(loc) => match Config::from_file(loc) {
            Ok(config) => config,
            Err(err) => {
                println!("invalid config file {:?}: {}", loc, err);
                exit(1);
            }
        },
        None => Config::default(),
    };

    let res: Result<()> = match &opts.subcmd {
        SubCommand::Start { .. } => handle_start(&opts, config),
    };

    res.map_err(|err: Error| println!("unexpected error: {}", err)).ok();
}

#[allow(unreachable_code)]
fn handle_start(opts: &Opt, config: Config) -> Result<()> {
    use mqtr::Cluster;

    let config = setup_config(config, opts);
    let _cluster = Cluster::from_config(config.clone())?.spawn()?;

    loop {}
}

fn setup_config(mut config: Config, opts: &Opt) -> Config {
    // TODO: environment variables can be consumed here. Configuration parameter values
    // take preference in the following order of decreasing preference:
    // a. Command line options.
    // b. Environment variables.
    // c. Toml configuration file.
    // d. System defaults.
    match &opts.subcmd {
        SubCommand::Start { name, port } => {
            if let Some(name) = name {
                config.name = name.clone();
            }
            if let Some(port) = port {
                config.port = Some(port.clone())
            }
        }
    }

    config
}

fn setup_logging() {
    use env_logger::{fmt::Color, Builder, WriteStyle};
    use log::Level;
    use std::io::Write;

    Builder::from_default_env()
        .format(|f, r| {
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
            let file = path::Path::new(r.file().clone().unwrap()).file_name().unwrap();
            writeln!(
                f,
                "{} [{:5} {:15}] {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%.3f%Z"),
                level_style.value(r.level()),
                mod_style.value(format!(
                    "{}:{}",
                    file.to_str().unwrap(),
                    r.line().unwrap()
                )),
                r.args()
            )
        })
        .write_style(WriteStyle::Auto)
        .init();
}
