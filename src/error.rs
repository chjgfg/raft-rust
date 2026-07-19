use std::fmt::Display;

use serde::{Deserialize, Serialize};

/// Raft 库错误类型。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    /// 操作被中止，必须重试。常见于 Raft 领导者变更等场景。
    /// 用此错误代替在 Raft 内实现复杂的重试与重放保护逻辑。
    Abort,
    /// 非法数据，通常是解码错误或意外的内部值。
    InvalidData(String),
    /// 非法用户输入。
    InvalidInput(String),
    /// IO 错误。
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
    /// 判断错误是否为确定性错误。
    ///
    /// Raft 状态机应用需要知道命令失败是否由输入命令本身决定：
    /// 若是，则该命令可视为已应用，错误可返回给客户端；
    /// 否则状态机必须 panic，以免节点状态分叉。
    pub fn is_deterministic(&self) -> bool {
        match self {
            // Abort 不会在应用阶段发生，只在领导者变更时出现。
            Error::Abort => false,
            // 可能是本节点本地数据损坏。
            Error::InvalidData(_) => false,
            // 输入错误（大概率）是确定性的。
            Error::InvalidInput(_) => true,
            // IO 错误通常是节点本地问题（例如磁盘故障）。
            Error::IO(_) => false,
        }
    }
}

/// 按格式字符串构造 `Error::InvalidData`。
#[macro_export]
macro_rules! errdata {
    ($($args:tt)*) => { $crate::error::Error::InvalidData(format!($($args)*)).into() };
}

/// 按格式字符串构造 `Error::InvalidInput`。
#[macro_export]
macro_rules! errinput {
    ($($args:tt)*) => { $crate::error::Error::InvalidInput(format!($($args)*)).into() };
}

/// 返回 [`Error`] 的 Result 别名。
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
        // 只有在其他线程持有互斥锁时 panic 才会发生。
        // 这种情况应视为致命错误，因此这里同样 panic。
        panic!("{err}")
    }
}
