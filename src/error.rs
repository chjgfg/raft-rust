// 用于实现 Display，向客户端/日志输出可读错误信息
use std::fmt::Display;

// 错误需可序列化，以便经网络作为 ClientReply 返回
use serde::{Deserialize, Serialize};

/// Raft 库错误类型。
// 可克隆/比较/序列化，便于在协议响应与测试断言中使用
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// 全库统一错误枚举：可经 ClientResponse 回传客户端
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
// Error 枚举定义结束
}

// 接入标准 Error trait，便于与 ? 及错误链生态互操作
impl std::error::Error for Error {}

// 将错误渲染为人类可读字符串（日志、CLI、ClientReply）
impl Display for Error {
    // 按变体拼出固定英文前缀，便于脚本/人类统一识别
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // 按错误类别输出固定英文前缀 + 细节
        match self {
            // 领导者变更等场景：提示操作被中止需重试
            Error::Abort => write!(f, "operation aborted"),
            // 协议/存储解码或内部不变量被破坏
            Error::InvalidData(msg) => write!(f, "invalid data: {msg}"),
            // 配置或客户端输入不合法
            Error::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
            // 磁盘、网络、channel 等本地 IO 故障
            Error::IO(msg) => write!(f, "io error: {msg}"),
        // match 分支穷尽
        }
    // fmt 结束
    }
// Display impl 结束
}

// 状态机确定性分类等与 Error 相关的方法
impl Error {
    /// 判断错误是否为确定性错误。
    ///
    /// Raft 状态机应用需要知道命令失败是否由输入命令本身决定：
    /// 若是，则该命令可视为已应用，错误可返回给客户端；
    /// 否则状态机必须 panic，以免节点状态分叉。
    pub fn is_deterministic(&self) -> bool {
        // 分类判断：只有输入类错误可安全回传且不导致状态分叉
        match self {
            // Abort 不会在应用阶段发生，只在领导者变更时出现。
            Error::Abort => false,
            // 可能是本节点本地数据损坏。
            Error::InvalidData(_) => false,
            // 输入错误（大概率）是确定性的。
            Error::InvalidInput(_) => true,
            // IO 错误通常是节点本地问题（例如磁盘故障）。
            Error::IO(_) => false,
        // is_deterministic 的 match 结束
        }
    // is_deterministic 结束
    }
// Error 方法 impl 结束
}

/// 按格式字符串构造 `Error::InvalidData`。
// 导出宏：在协议解码/日志损坏等路径快速构造 InvalidData
#[macro_export]
// 解码/内部不变量失败时的便捷构造入口
macro_rules! errdata {
    // 将 format! 参数包装为 InvalidData，并 .into() 成 Result
    ($($args:tt)*) => { $crate::error::Error::InvalidData(format!($($args)*)).into() };
// errdata 宏定义结束
}

/// 按格式字符串构造 `Error::InvalidInput`。
// 导出宏：在配置/客户端参数校验路径快速构造 InvalidInput
#[macro_export]
// 用户输入/配置校验失败时的便捷构造入口
macro_rules! errinput {
    // 将 format! 参数包装为 InvalidInput，并 .into() 成 Result
    ($($args:tt)*) => { $crate::error::Error::InvalidInput(format!($($args)*)).into() };
// errinput 宏定义结束
}

/// 返回 [`Error`] 的 Result 别名。
// 全库统一的 Result 类型，减少重复书写
pub type Result<T> = std::result::Result<T, Error>;

// 允许 `return error.into()` 直接得到 Err(error)
impl<T> From<Error> for Result<T> {
    // 配合 errdata!/errinput! 的 `.into()` 直接变成 Err
    fn from(error: Error) -> Self {
        // 将 Error 包装为 Result::Err，配合 errdata!/errinput! 宏使用
        Err(error)
    // From<Error> for Result 结束
    }
// Result 的 From impl 结束
}

// bincode 解码失败映射为 InvalidData（线路/日志载荷损坏）
impl From<bincode::error::DecodeError> for Error {
    // 线路或持久化载荷无法按约定格式解码
    fn from(err: bincode::error::DecodeError) -> Self {
        // 序列化格式错误视为数据不合法，而非本地 IO
        Error::InvalidData(err.to_string())
    // DecodeError 转换结束
    }
// From DecodeError 结束
}

// bincode 编码失败映射为 InvalidData（理论上少见，多为类型不兼容）
impl From<bincode::error::EncodeError> for Error {
    // 编码路径异常（类型变更/不兼容）同样归数据问题
    fn from(err: bincode::error::EncodeError) -> Self {
        // 编码失败同样归类为数据问题
        Error::InvalidData(err.to_string())
    // EncodeError 转换结束
    }
// From EncodeError 结束
}

// 阻塞接收 channel 断开 → IO（对端线程退出）
impl From<crossbeam::channel::RecvError> for Error {
    // 阻塞 recv 时对端已关闭通道
    fn from(err: crossbeam::channel::RecvError) -> Self {
        // 通道关闭视作节点内通信 IO 故障
        Error::IO(err.to_string())
    // RecvError 转换结束
    }
// From RecvError 结束
}

// 发送 channel 失败 → IO（接收端已丢弃）
impl<T> From<crossbeam::channel::SendError<T>> for Error {
    // 发送时接收端已 drop，消息无法投递
    fn from(err: crossbeam::channel::SendError<T>) -> Self {
        // 无法投递出站消息（如 Node 驱动线程退出）
        Error::IO(err.to_string())
    // SendError 转换结束
    }
// From SendError 结束
}

// 非阻塞接收错误（空或断开）→ IO
impl From<crossbeam::channel::TryRecvError> for Error {
    // try_recv 在空队列或断开时都会落到此映射
    fn from(err: crossbeam::channel::TryRecvError) -> Self {
        // try_recv 失败统一映射，调用方再按需区分
        Error::IO(err.to_string())
    // TryRecvError 转换结束
    }
// From TryRecvError 结束
}

// 非阻塞发送错误 → IO
impl<T> From<crossbeam::channel::TrySendError<T>> for Error {
    // 有界通道满或已关闭时的非阻塞发送失败
    fn from(err: crossbeam::channel::TrySendError<T>) -> Self {
        // 有界通道满或已关闭
        Error::IO(err.to_string())
    // TrySendError 转换结束
    }
// From TrySendError 结束
}

// 标准库 IO 错误（磁盘/网络）直接映射
impl From<std::io::Error> for Error {
    // 文件、套接字等系统调用失败统一为 IO 变体
    fn from(err: std::io::Error) -> Self {
        // 文件、socket 等系统调用失败
        Error::IO(err.to_string())
    // std::io::Error 转换结束
    }
// From std::io::Error 结束
}

// 切片转固定数组失败（长度不符）→ 数据损坏/格式错误
impl From<std::array::TryFromSliceError> for Error {
    // 长度前缀/定长字段截取时字节数不够
    fn from(err: std::array::TryFromSliceError) -> Self {
        // 例如长度前缀解析时字节数不足
        Error::InvalidData(err.to_string())
    // TryFromSliceError 转换结束
    }
// From TryFromSliceError 结束
}

// UTF-8 校验失败 → 数据不合法
impl From<std::string::FromUtf8Error> for Error {
    // 期望文本的二进制字段不是合法 UTF-8
    fn from(err: std::string::FromUtf8Error) -> Self {
        // 期望字符串的二进制字段非法
        Error::InvalidData(err.to_string())
    // FromUtf8Error 转换结束
    }
// From FromUtf8Error 结束
}

// 整数范围转换失败 → 数据/协议字段越界
impl From<std::num::TryFromIntError> for Error {
    // 协议索引/长度等字段无法安全窄化
    fn from(err: std::num::TryFromIntError) -> Self {
        // 索引/长度等字段无法安全转换
        Error::InvalidData(err.to_string())
    // TryFromIntError 转换结束
    }
// From TryFromIntError 结束
}

// 互斥锁中毒：另一线程在持锁时 panic，本节点状态已不可信
impl<T> From<std::sync::PoisonError<T>> for Error {
    // 锁中毒说明节点内不变量可能已破坏，直接致命退出
    fn from(err: std::sync::PoisonError<T>) -> Self {
        // 只有在其他线程持有互斥锁时 panic 才会发生。
        // 这种情况应视为致命错误，因此这里同样 panic。
        panic!("{err}")
    // PoisonError 处理结束
    }
// From PoisonError 结束
}
