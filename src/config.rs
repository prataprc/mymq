use std::{fs, path};

use crate::v5;
use crate::{Error, ErrorKind, Result};

#[macro_export]
macro_rules! config_field {
    ($table:ident, $field:ident, $config:ident, $($args:tt)+) => {{
        let field = stringify!($field);
        if let Some(val) = $table.get(field) {
            $config.$field = match val.$($args)+ {
                Some(val) => val.parse()?,
                None => err!(
                    InvalidInput,
                    desc: "invalid config field {}, {}", field, val.to_string()
                )?,
            }
        }
    }};
    (opt: $table:ident, $field:ident, $config:ident, $($args:tt)+) => {{
        let field = stringify!($field);
        if let Some(val) = $table.get(field) {
            $config.$field = match val.$($args)+ {
                Some(val) => Some(val.parse()?),
                None => err!(
                    InvalidInput,
                    desc: "invalid config field {}, {}", field, val.to_string()
                )?,
            }
        }
    }};
}

/// Configuration for broker and supported protocols.
#[derive(Clone, Eq, PartialEq)]
pub struct Config {
    /// Broker configuration.
    #[cfg(feature = "broker")]
    pub broker: crate::broker::Config,

    /// Protocol configuration for MQTT V5.
    pub mqtt_v5: v5::Config,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            #[cfg(feature = "broker")]
            broker: crate::broker::Config::default(),
            mqtt_v5: v5::Config::default(),
        }
    }
}

impl TryFrom<toml::Value> for Config {
    type Error = Error;

    fn try_from(val: toml::Value) -> Result<Config> {
        let config = match val.as_table() {
            Some(map) => Config {
                #[cfg(feature = "broker")]
                broker: match map.get("broker") {
                    Some(value) => crate::broker::Config::try_from(value.clone())?,
                    None => crate::broker::Config::default(),
                },
                mqtt_v5: match map.get("mqtt_v5") {
                    Some(value) => v5::Config::try_from(value.clone())?,
                    None => v5::Config::default(),
                },
            },
            None => err!(InvalidInput, desc: "invalid toml configuration")?,
        };
        Ok(config)
    }
}

impl Config {
    /// Construct a new configuration from a file located by `loc`.
    pub fn from_file<P>(loc: P) -> Result<Config>
    where
        P: AsRef<path::Path>,
    {
        use std::str::from_utf8;

        let ploc: &path::Path = loc.as_ref();

        let value: toml::Value = {
            let data = err!(IOError, try: fs::read(ploc), "reading config {:?}", ploc)?;
            let txt = err!(InvalidInput, try: from_utf8(&data), "bad config {:?}", ploc)?;
            err!(InvalidInput, try: toml::from_str(txt), "config not toml {:?}", ploc)?
        };

        Config::try_from(value)
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        #[cfg(feature = "broker")]
        self.broker.validate()?;

        self.mqtt_v5.validate()?;
        Ok(())
    }
}
