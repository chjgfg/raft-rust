//! TCP + bincode 传输层（多进程节点）。

mod codec;
mod transport;

pub use codec::{read_msg, write_msg, WireMsg};
pub use transport::{run_client_request, spawn_listener, Inbound, PeerOutbox};
