use std::fmt::Display;

use serde::{Deserialize, Serialize};

/// Raft library errors.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    /// The operation was aborted and must be retried. This typically happens
    /// with e.g. Raft leader changes. This is used instead of implementing
    /// complex retry logic and replay protection in Raft.
    Abort,
    /// Invalid data, typically decoding errors or unexpected internal values.
    InvalidData(String),
    /// Invalid user input.
    InvalidInput(String),
    /// An IO error.
    IO(String),
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Abort => write!(f, "operation aborted"),
            Error::InvalidData(msg) => write!(f, "invalid data: {msg}"),
            Error::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
            Error::IO(msg) => write!(f, "io error: {msg}"),
        }
    }
}

impl Error {
    /// Returns whether the error is considered deterministic. Raft state
    /// machine application needs to know whether a command failure is
    /// deterministic on the input command -- if it is, the command can be
    /// considered applied and the error returned to the client, but otherwise
    /// the state machine must panic to prevent node divergence.
    pub fn is_deterministic(&self) -> bool {
        match self {
            // Aborts don't happen during application, only leader changes.
            Error::Abort => false,
            // Possible data corruption local to this node.
            Error::InvalidData(_) => false,
            // Input errors are (likely) deterministic.
            Error::InvalidInput(_) => true,
            // IO errors are typically local to the node (e.g. faulty disk).
            Error::IO(_) => false,
        }
    }
}

/// Constructs an Error::InvalidData for the given format string.
#[macro_export]
macro_rules! errdata {
    ($($args:tt)*) => { $crate::error::Error::InvalidData(format!($($args)*)).into() };
}

/// Constructs an Error::InvalidInput for the given format string.
#[macro_export]
macro_rules! errinput {
    ($($args:tt)*) => { $crate::error::Error::InvalidInput(format!($($args)*)).into() };
}

/// A Result returning Error.
pub type Result<T> = std::result::Result<T, Error>;

impl<T> From<Error> for Result<T> {
    fn from(error: Error) -> Self {
        Err(error)
    }
}

impl serde::de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::InvalidData(msg.to_string())
    }
}

impl serde::ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::InvalidData(msg.to_string())
    }
}

impl From<bincode::error::DecodeError> for Error {
    fn from(err: bincode::error::DecodeError) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<bincode::error::EncodeError> for Error {
    fn from(err: bincode::error::EncodeError) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<crossbeam::channel::RecvError> for Error {
    fn from(err: crossbeam::channel::RecvError) -> Self {
        Error::IO(err.to_string())
    }
}

impl<T> From<crossbeam::channel::SendError<T>> for Error {
    fn from(err: crossbeam::channel::SendError<T>) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<crossbeam::channel::TryRecvError> for Error {
    fn from(err: crossbeam::channel::TryRecvError) -> Self {
        Error::IO(err.to_string())
    }
}

impl<T> From<crossbeam::channel::TrySendError<T>> for Error {
    fn from(err: crossbeam::channel::TrySendError<T>) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(err: std::array::TryFromSliceError) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(err: std::num::TryFromIntError) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        // This only happens when a different thread panics while holding a
        // mutex. This should be fatal, so we panic here too.
        panic!("{err}")
    }
}
