use std::{self, fmt, result};

/// Short form to compose Error values.
///
/// Here are few possible ways:
///
/// ```ignore
/// err!(InsufficientBytes, desc: "for payload {}", len);
/// ```
///
/// ```ignore
/// err!(MalformedPacket, code: InvalidQoS, "qos={}", qos);
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
    ($v:ident, code: $code:ident, cause: $cause:expr, $($arg:expr),+) => {{
        let kind = ErrorKind::$v;
        let description = format!($($arg),+);
        let e = Error {
            kind,
            description,
            code: Some(ReasonCode::$code),
            cause: Some(Box::new($cause)),
            ..Error::default()
        };

        log_error!(e);
        Err(e)
    }};
    ($v:ident, code: $code:ident, $($arg:expr),+) => {{
        let kind = ErrorKind::$v;
        let description = format!($($arg),+);
        let e = Error {
            kind,
            description,
            code: Some(ReasonCode::$code),
            ..Error::default()
        };

        log_error!(e);
        Err(e)
    }};
    ($v:ident, cause: $cause:expr, $($arg:expr),+) => {{
        let kind = ErrorKind::$v;
        let description = format!($($arg),+);
        let e = Error {
            kind,
            description,
            cause: Some(Box::new($cause)),
            ..Error::default()
        };

        log_error!(e);
        Err(e)
    }};
    ($v:ident, desc: $($arg:expr),+) => {{
        let kind = ErrorKind::$v;
        let description = format!($($arg),+);
        let e = Error {
            kind,
            description,
            ..Error::default()
        };

        log_error!(e);
        Err(e)
    }};
}

#[macro_export]
macro_rules! log_error {
    ($e:ident) => {{
        use log::error;

        #[cfg(feature = "backtrace")]
        use std::backtrace::BacktraceStatus::*;

        error!("{}: {}", $e.kind, $e.description);

        #[cfg(feature = "backtrace")]
        match $e.backtrace.status() {
            Unsupported => error!("[BACKTRACE Unsupported]"),
            Disabled => error!("[BACKTRACE Disabled]"),
            Captured => $e.backtrace.frames().for_each(|f| error!("{:?}", f)),
        }
    }};
}

/// Error that is part of [Result] type.
pub struct Error {
    pub(crate) kind: ErrorKind,
    pub(crate) description: String,
    pub(crate) code: Option<ReasonCode>,
    pub(crate) cause: Option<Box<dyn std::error::Error>>,
    #[cfg(feature = "backtrace")]
    pub(crate) backtrace: Option<backtrace::Backtrace>,
}

impl Default for Error {
    fn default() -> Error {
        Error {
            kind: ErrorKind::InvalidInput,
            description: String::default(),
            code: None,
            cause: None,
            #[cfg(feature = "backtrace")]
            backtrace: std::backtrace::Backtrace::force_capture(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self.description)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        let code = self.code.map(|c| c.to_string()).unwrap_or("-".to_string());
        write!(f, "<{},{},{}>", self.kind, code, self.description)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.as_ref().map(|b| b.as_ref())
    }

    fn description(&self) -> &str {
        self.description.as_str()
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.cause.as_ref().map(|b| b.as_ref())
    }

    #[cfg(feature = "backtrace")]
    fn backtrace(&self) -> Option<&Backtrace> {
        self.backtrace.as_ref()
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(val: std::num::TryFromIntError) -> Self {
        let err: result::Result<(), Error> = err!(TryFromIntError, cause: val, "{}", val);
        err.unwrap_err()
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

    /// Reason code as defined by `MQTT-spec`.
    pub fn code(&self) -> Option<ReasonCode> {
        self.code
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
    UnsupportedProtocolVersion,
    InsufficientBytes,
    MalformedPacket,
    ProtocolError,
    IOError,
    InvalidInput,
    PayloadTooLong,
    TryFromIntError,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        use ErrorKind::*;

        match self {
            NoError => write!(f, "NoError"),
            UnsupportedProtocolVersion => write!(f, "UnsupportedProtocolVersion"),
            InsufficientBytes => write!(f, "InsufficientBytes"),
            MalformedPacket => write!(f, "MalformedPacket"),
            ProtocolError => write!(f, "ProtocolError"),
            IOError => write!(f, "IOError"),
            InvalidInput => write!(f, "InvalidInput"),
            PayloadTooLong => write!(f, "PayloadTooLong"),
            TryFromIntError => write!(f, "TryFromIntError"),
        }
    }
}

/// ReasonCode defined by `MQTT-spec`, each variant defines error value.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ReasonCode {
    Success = 0x00, // NormalDisconnet, QoS0
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
    SessionTakeOver = 0x8E,
    InvalidTopicFilter = 0x8F,
    InvalidTopicName = 0x90,
    PacketIdInuse = 0x91,
    PacketIdNotFound = 0x92,
    ExceededReceiveMaximum = 0x93,
    InvalidTopicAlias = 0x94,
    PacketTooLarge = 0x95,
    ExceedMessageRate = 0x96,
    QuotaExceeded = 0x97,
    AdminAction = 0x98,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9A,
    InvalidQoS = 0x9B,
    UseAnotherServer = 0x9C,
    ServerMoved = 0x9D,
    UnsupportedSharedSubscription = 0x9E,
    ExceedConnectionRate = 0x9F,
    ExceedMaximumConnectTime = 0xA0,
    SubscriptionIdNotSupported = 0xA1,
    WildcardSubscriptionsNotSupported = 0xA2,
}

impl fmt::Display for ReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        use ReasonCode::*;

        let s = match self {
            Success => "Success",
            QoS1 => "Granted QoS 1",
            QoS2 => "Granted QoS 2",
            DiconnectWillMessage => "Disconnect with Will Message",
            NoMatchingSubscribers => "No matching subscribers",
            NoSubscriptionExisted => "No subscription existed",
            ContinueAuthentication => "Continue authentication",
            ReAuthenticate => "Re-authenticate",
            UnspecifiedError => "Unspecified error",
            MalformedPacket => "Malformed Packet",
            ProtocolError => "Protocol Error",
            ImplementationError => "Implementation specific error",
            UnsupportedProtocolVersion => "Unsupported Protocol Version",
            InvalidClientID => "Client Identifier not valid",
            BadLogin => "Bad User Name or Password",
            NotAuthorized => "Not authorized",
            ServerNotAvailable => "Server unavailable",
            ServerBusy => "Server busy",
            Banned => "Banned",
            ServerShutdown => "Server shutting down",
            BadAuthenticationMethod => "Bad authentication method",
            KeepAliveTimeout => "Keep Alive timeout",
            SessionTakeOver => "Session taken over",
            InvalidTopicFilter => "Topic Filter invalid",
            InvalidTopicName => "Topic Name invalid",
            PacketIdInuse => "Packet Identifier in use",
            PacketIdNotFound => "Packet Identifier not found",
            ExceededReceiveMaximum => "Receive Maximum exceeded",
            InvalidTopicAlias => "Topic Alias invalid",
            PacketTooLarge => "Packet too large",
            ExceedMessageRate => "Message rate too high",
            QuotaExceeded => "Quota exceeded",
            AdminAction => "Administrative action",
            PayloadFormatInvalid => "Payload format invalid",
            RetainNotSupported => "Retain not supported",
            InvalidQoS => "QoS not supported",
            UseAnotherServer => "Use another server",
            ServerMoved => "Server moved",
            UnsupportedSharedSubscription => "Shared Subscriptions not supported",
            ExceedConnectionRate => "Connection rate exceeded",
            ExceedMaximumConnectTime => "Maximum connect time",
            SubscriptionIdNotSupported => "Subscription Identifiers not supported",
            WildcardSubscriptionsNotSupported => "Wildcard Subscriptions not supported",
        };

        write!(f, "{}", s)
    }
}
