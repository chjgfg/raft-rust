//! TCP + bincode 传输层（多进程节点）。

// 长度前缀帧编解码
mod codec;
// 监听、连接处理与 peer 出站
mod transport;

// 对外暴露线路消息类型与读写辅助
pub use codec::{read_msg, write_msg, WireMsg};
// 对外暴露入站事件、监听启动与客户端请求入口
pub use transport::{run_client_request, spawn_listener, Inbound, PeerOutbox};
