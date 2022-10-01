#[cfg(any(feature = "fuzzy", test))]
use arbitrary::Arbitrary;

#[cfg(feature = "backtrace")]
use std::backtrace::Backtrace;

use std::{self, fmt, result};

use crate::Result;

/// Macro to compose Error values.
///
/// Here are few possible ways:
///
/// ```ignore
/// err!(InsufficientBytes, desc: "for payload {}", len);
/// ```
///
/// ```ignore
/// err!(MalformedPacket, code: QoSNotSupported, "qos={}", qos);
/// ```
///
/// ```ignore
/// err_at!(IOError, cause: err, "from addr:{}", ip_addr);
/// ```
///
/// ```ignore
/// err_at!(IOError, code: ServerShutdown, cause: err, "reason", reason);
/// ```
///
#[macro_export]
macro_rules! err {
    ($v:ident, code: $code:ident, cause: $cause:expr, $($args:expr),+) => {{
        let kind = ErrorKind::$v;
        let e = Error {
            kind,
            description: format!($($args),+),
            code: Some(ReasonCode::$code),
            cause: Some(Box::new($cause)),
            loc: format!("{}:{}", file!(), line!()),
            ..Error::default()
        };

        log_error!(e);
        Err(e)
    }};
    ($v:ident, try: $res:expr, $($args:expr),+) => {{
        match $res {
            Ok(val) => Ok(val),
            Err(err) => {
                let e = Error {
                    kind: ErrorKind::$v,
                    description: format!($($args),+),
                    cause: Some(Box::new(err)),
                    loc: format!("{}:{}", file!(), line!()),
                    ..Error::default()
                };
                log_error!(e);
                Err(e)
            }
        }
    }};
    ($v:ident, try: $res:expr) => {{
        match $res {
            Ok(val) => Ok(val),
            Err(err) => {
                let e = Error {
                    kind: ErrorKind::$v,
                    description: err.to_string(),
                    cause: Some(Box::new(err)),
                    loc: format!("{}:{}", file!(), line!()),
                    ..Error::default()
                };
                log_error!(e);
                Err(e)
            }
        }
    }};
    ($v:ident, code: $code:ident, $($args:expr),+) => {{
        let kind = ErrorKind::$v;
        let description = format!($($args),+);
        let e = Error {
            kind,
            description,
            code: Some(ReasonCode::$code),
            loc: format!("{}:{}", file!(), line!()),
            ..Error::default()
        };

        log_error!(e);
        Err(e)
    }};
    ($v:ident, cause: $cause:expr, $($args:expr),+) => {{
        let kind = ErrorKind::$v;
        let description = format!($($args),+);
        let e = Error {
            kind,
            description,
            cause: Some(Box::new($cause)),
            loc: format!("{}:{}", file!(), line!()),
            ..Error::default()
        };

        log_error!(e);
        Err(e)
    }};
    ($v:ident, desc: $($args:expr),+) => {{
        let kind = ErrorKind::$v;
        let description = format!($($args),+);
        let e = Error {
            kind,
            description,
            loc: format!("{}:{}", file!(), line!()),
            ..Error::default()
        };

        log_error!(e);
        Err(e)
    }};
}

/// Macro to log error. Suggest using the [err] macro.
#[cfg_attr(any(feature = "fuzzy", test), macro_export)]
macro_rules! log_error {
    ($e:ident) => {{
        log::error!("{}: {}", $e.kind, $e.description);

        match &$e.cause {
            Some(cause) => log::error!("cause:{}", cause.to_string()),
            None => (),
        }

        #[cfg(feature = "backtrace")]
        use std::backtrace::BacktraceStatus;
        #[cfg(feature = "backtrace")]
        match ($e.backtrace.status(), $e.kind()) {
            (BacktraceStatus::Unsupported, _) => log::error!("[BACKTRACE Unsupported]"),
            (BacktraceStatus::Disabled, _) => log::error!("[BACKTRACE Disabled]"),
            (BacktraceStatus::Captured, ErrorKind::Disconnected) => (),
            (BacktraceStatus::Captured, _) => {
                for f in $e.backtrace.frames().iter() {
                    println!("{:?}", f)
                }
            }
            _ => todo!(),
        }
    }};
}

/// This is a local macro used by mymq threads to send `fatal` message back to the
/// application. Typically `self` is a thread and this macro make assumptions about
/// the `self` argument.
#[cfg(feature = "broker")]
macro_rules! app_fatal {
    ($self:expr, $($args:expr),+) => {{
        match $($args),+ {
            Ok(_) => (),
            Err(err) => {
                $self.as_app_tx().send("fatal".to_string()).ok();
                log::error!("{} fatal error {} ", $self.prefix, err);
            }
        }
    }};
}

/// Type Error implement all possible error-values that can be returned by the
/// [Result] type.
pub struct Error {
    /// Kind of error, callers/application should interpret this for granular
    /// error handling.
    pub kind: ErrorKind,
    /// Human readable string.
    pub description: String,
    /// Reason code.
    pub code: Option<ReasonCode>,
    /// Chain of errors, that is, if another error lead to this error.
    pub cause: Option<Box<dyn std::error::Error + Send>>,
    /// Location at which the error happens
    pub loc: String,
    /// Call stack at the point where the error happened.
    #[cfg(feature = "backtrace")]
    pub backtrace: Backtrace,
}

impl Default for Error {
    fn default() -> Error {
        Error {
            kind: ErrorKind::InvalidInput,
            description: String::default(),
            code: None,
            cause: None,
            loc: String::default(),
            #[cfg(feature = "backtrace")]
            backtrace: Backtrace::force_capture(),
        }
    }
}

impl Clone for Error {
    fn clone(&self) -> Error {
        Error {
            kind: self.kind,
            description: self.description.clone(),
            code: self.code,
            cause: None,
            loc: self.loc.clone(),
            #[cfg(feature = "backtrace")]
            backtrace: Backtrace::disabled(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}:{}", self.kind, self.description)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "<{},{},{}>", self.kind, self.description, self.loc)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            Some(cause) => Some(cause.as_ref()),
            None => None,
        }
    }

    fn description(&self) -> &str {
        self.description.as_str()
    }

    fn cause(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            Some(cause) => Some(cause.as_ref()),
            None => None,
        }
    }

    #[cfg(feature = "backtrace")]
    fn backtrace(&self) -> Option<&Backtrace> {
        Some(&self.backtrace)
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(val: std::convert::Infallible) -> Self {
        let err: result::Result<(), Error> = err!(Infallible, cause: val, "{}", val);
        err.unwrap_err()
    }
}

impl From<std::str::ParseBoolError> for Error {
    fn from(val: std::str::ParseBoolError) -> Self {
        let err: result::Result<(), Error> = err!(ParseBoolError, cause: val, "{}", val);
        err.unwrap_err()
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(val: std::num::ParseFloatError) -> Self {
        let err: result::Result<(), Error> = err!(ParseFloatError, cause: val, "{}", val);
        err.unwrap_err()
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(val: std::num::ParseIntError) -> Self {
        let err: result::Result<(), Error> = err!(ParseIntError, cause: val, "{}", val);
        err.unwrap_err()
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(val: std::num::TryFromIntError) -> Self {
        let err: result::Result<(), Error> = err!(TryFromIntError, cause: val, "{}", val);
        err.unwrap_err()
    }
}

impl From<std::net::AddrParseError> for Error {
    fn from(val: std::net::AddrParseError) -> Self {
        let err: result::Result<(), Error> =
            err!(TryFromAddrError, cause: val, "{}", val);
        err.unwrap_err()
    }
}

impl From<uuid::Error> for Error {
    fn from(val: uuid::Error) -> Self {
        let err: result::Result<(), Error> = err!(UuidError, cause: val, "{}", val);
        err.unwrap_err()
    }
}

impl From<std::io::Error> for Error {
    fn from(val: std::io::Error) -> Self {
        let err: result::Result<(), Error> = err!(IOError, cause: val, "{}", val);
        err.unwrap_err()
    }
}

#[cfg(any(feature = "fuzzy", test))]
impl From<Error> for arbitrary::Error {
    fn from(_: Error) -> Self {
        arbitrary::Error::IncorrectFormat
    }
}

impl Error {
    /// Return the error kind, caller should know how to handle it.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Return error kinds from this error and all of the root causes.
    pub fn kinds(&self) -> Vec<ErrorKind> {
        let mut kinds = vec![self.kind];
        match &self.cause {
            Some(err) => {
                kinds.extend_from_slice(&err.downcast_ref::<Error>().unwrap().kinds());
                kinds
            }
            None => kinds,
        }
    }

    /// Return the reason-code. Default reason code shall be
    /// [ReasonCode::UnspecifiedError]
    pub fn code(&self) -> ReasonCode {
        self.code.unwrap_or(ReasonCode::UnspecifiedError)
    }

    pub fn has(&self, kind: ErrorKind) -> bool {
        if self.kind == kind {
            true
        } else {
            match &self.cause {
                Some(err) => err.downcast_ref::<Error>().unwrap().has(kind),
                None => false,
            }
        }
    }
}

/// Error kind expected to be handled by calling functions.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ErrorKind {
    NoError,
    // general error
    InvalidInput,
    Fatal,
    // protocol errors
    MalformedPacket,
    ProtocolError,
    UnsupportedProtocolVersion,
    InsufficientBytes,
    SessionTakenOver,
    // network error
    Disconnected,
    SlowClient,
    // thread / ipc error
    IPCFail,
    RxClosed,
    TxFinish,
    // chain of error
    Infallible,
    ParseBoolError,
    ParseFloatError,
    ParseIntError,
    TryFromIntError,
    TryFromAddrError,
    UuidError,
    IOError,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        use ErrorKind::*;

        match self {
            NoError => write!(f, "NoError"),
            // general error
            InvalidInput => write!(f, "InvalidInput"),
            Fatal => write!(f, "Fatal"),
            // protocol errors
            ProtocolError => write!(f, "ProtocolError"),
            UnsupportedProtocolVersion => write!(f, "UnsupportedProtocolVersion"),
            InsufficientBytes => write!(f, "InsufficientBytes"),
            MalformedPacket => write!(f, "MalformedPacket"),
            SessionTakenOver => write!(f, "SessionTakenOver"),
            // network error
            Disconnected => write!(f, "Disconnected"),
            SlowClient => write!(f, "SlowClient"),
            // thread / ipc error
            IPCFail => write!(f, "IPCFail"),
            RxClosed => write!(f, "RxClosed"),
            TxFinish => write!(f, "TxFinish"),
            // chain of error
            Infallible => write!(f, "Infallible"),
            ParseBoolError => write!(f, "ParseBoolError"),
            ParseFloatError => write!(f, "ParseFloatError"),
            ParseIntError => write!(f, "ParseIntError"),
            TryFromIntError => write!(f, "TryFromIntError"),
            TryFromAddrError => write!(f, "TryFromAddrError"),
            UuidError => write!(f, "UuidError"),
            IOError => write!(f, "IOError"),
        }
    }
}

/// ReasonCode defined by each variant defines error value.
#[cfg_attr(any(feature = "fuzzy", test), derive(Arbitrary))]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum ReasonCode {
    Success = 0x00, // NormalDisconnect, QoS0
    QoS1 = 0x1,
    QoS2 = 0x02,
    DiconnectWillMessage = 0x04,
    NoMatchingSubscribers = 0x10,
    NoSubscriptionExisted = 0x11,
    ContinueAuthentication = 0x18,
    ReAuthenticate = 0x19,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationError = 0x83,
    UnsupportedProtocolVersion = 0x84,
    InvalidClientID = 0x85,
    BadLogin = 0x86,
    NotAuthorized = 0x87,
    ServerNotAvailable = 0x88,
    ServerBusy = 0x89,
    Banned = 0x8A,
    ServerShutdown = 0x8B,
    BadAuthenticationMethod = 0x8C,
    KeepAliveTimeout = 0x8D,
    SessionTakenOver = 0x8E,
    InvalidTopicFilter = 0x8F,
    TopicNameInvalid = 0x90,
    PacketIdInuse = 0x91,
    PacketIdNotFound = 0x92,
    ExceededReceiveMaximum = 0x93,
    TopicAliasInvalid = 0x94,
    PacketTooLarge = 0x95,
    ExceedMessageRate = 0x96,
    QuotaExceeded = 0x97,
    AdminAction = 0x98,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9A,
    QoSNotSupported = 0x9B,
    UseAnotherServer = 0x9C,
    ServerMoved = 0x9D,
    UnsupportedSharedSubscription = 0x9E,
    ExceedConnectionRate = 0x9F,
    ExceedMaximumConnectTime = 0xA0,
    SubscriptionIdNotSupported = 0xA1,
    WildcardSubscriptionsNotSupported = 0xA2,
}

impl Default for ReasonCode {
    fn default() -> ReasonCode {
        ReasonCode::Success
    }
}

impl TryFrom<u8> for ReasonCode {
    type Error = Error;

    fn try_from(val: u8) -> Result<ReasonCode> {
        match val {
            0x00 => Ok(ReasonCode::Success),
            0x1 => Ok(ReasonCode::QoS1),
            0x02 => Ok(ReasonCode::QoS2),
            0x04 => Ok(ReasonCode::DiconnectWillMessage),
            0x10 => Ok(ReasonCode::NoMatchingSubscribers),
            0x11 => Ok(ReasonCode::NoSubscriptionExisted),
            0x18 => Ok(ReasonCode::ContinueAuthentication),
            0x19 => Ok(ReasonCode::ReAuthenticate),
            0x80 => Ok(ReasonCode::UnspecifiedError),
            0x81 => Ok(ReasonCode::MalformedPacket),
            0x82 => Ok(ReasonCode::ProtocolError),
            0x83 => Ok(ReasonCode::ImplementationError),
            0x84 => Ok(ReasonCode::UnsupportedProtocolVersion),
            0x85 => Ok(ReasonCode::InvalidClientID),
            0x86 => Ok(ReasonCode::BadLogin),
            0x87 => Ok(ReasonCode::NotAuthorized),
            0x88 => Ok(ReasonCode::ServerNotAvailable),
            0x89 => Ok(ReasonCode::ServerBusy),
            0x8A => Ok(ReasonCode::Banned),
            0x8B => Ok(ReasonCode::ServerShutdown),
            0x8C => Ok(ReasonCode::BadAuthenticationMethod),
            0x8D => Ok(ReasonCode::KeepAliveTimeout),
            0x8E => Ok(ReasonCode::SessionTakenOver),
            0x8F => Ok(ReasonCode::InvalidTopicFilter),
            0x90 => Ok(ReasonCode::TopicNameInvalid),
            0x91 => Ok(ReasonCode::PacketIdInuse),
            0x92 => Ok(ReasonCode::PacketIdNotFound),
            0x93 => Ok(ReasonCode::ExceededReceiveMaximum),
            0x94 => Ok(ReasonCode::TopicAliasInvalid),
            0x95 => Ok(ReasonCode::PacketTooLarge),
            0x96 => Ok(ReasonCode::ExceedMessageRate),
            0x97 => Ok(ReasonCode::QuotaExceeded),
            0x98 => Ok(ReasonCode::AdminAction),
            0x99 => Ok(ReasonCode::PayloadFormatInvalid),
            0x9A => Ok(ReasonCode::RetainNotSupported),
            0x9B => Ok(ReasonCode::QoSNotSupported),
            0x9C => Ok(ReasonCode::UseAnotherServer),
            0x9D => Ok(ReasonCode::ServerMoved),
            0x9E => Ok(ReasonCode::UnsupportedSharedSubscription),
            0x9F => Ok(ReasonCode::ExceedConnectionRate),
            0xA0 => Ok(ReasonCode::ExceedMaximumConnectTime),
            0xA1 => Ok(ReasonCode::SubscriptionIdNotSupported),
            0xA2 => Ok(ReasonCode::WildcardSubscriptionsNotSupported),
            val => err!(MalformedPacket, code: MalformedPacket, "reason-code {}", val),
        }
    }
}

impl fmt::Display for ReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        use ReasonCode::*;

        let s = match self {
            Success => "success",
            QoS1 => "granted qos 1",
            QoS2 => "granted qos 2",
            DiconnectWillMessage => "disconnect with will message",
            NoMatchingSubscribers => "no matching subscribers",
            NoSubscriptionExisted => "no subscription existed",
            ContinueAuthentication => "continue authentication",
            ReAuthenticate => "re-authenticate",
            UnspecifiedError => "unspecified error",
            MalformedPacket => "malformed packet",
            ProtocolError => "protocol error",
            ImplementationError => "implementation specific error",
            UnsupportedProtocolVersion => "unsupported protocol version",
            InvalidClientID => "client identifier not valid",
            BadLogin => "bad user name or password",
            NotAuthorized => "not authorized",
            ServerNotAvailable => "server unavailable",
            ServerBusy => "server busy",
            Banned => "banned",
            ServerShutdown => "server shutting down",
            BadAuthenticationMethod => "bad authentication method",
            KeepAliveTimeout => "keep alive timeout",
            SessionTakenOver => "session taken over",
            InvalidTopicFilter => "topic filter invalid",
            TopicNameInvalid => "topic name invalid",
            PacketIdInuse => "packet identifier in use",
            PacketIdNotFound => "packet identifier not found",
            ExceededReceiveMaximum => "receive maximum exceeded",
            TopicAliasInvalid => "topic alias invalid",
            PacketTooLarge => "packet too large",
            ExceedMessageRate => "message rate too high",
            QuotaExceeded => "quota exceeded",
            AdminAction => "administrative action",
            PayloadFormatInvalid => "payload format invalid",
            RetainNotSupported => "retain not supported",
            QoSNotSupported => "qos not supported",
            UseAnotherServer => "use another server",
            ServerMoved => "server moved",
            UnsupportedSharedSubscription => "shared subscriptions not supported",
            ExceedConnectionRate => "connection rate exceeded",
            ExceedMaximumConnectTime => "maximum connect time",
            SubscriptionIdNotSupported => "subscription identifiers not supported",
            WildcardSubscriptionsNotSupported => "wildcard subscriptions not supported",
        };

        write!(f, "{}", s)
    }
}
