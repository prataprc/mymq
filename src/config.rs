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
