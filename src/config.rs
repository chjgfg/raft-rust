//! 单节点进程配置（YAML）。
//!
//! 多节点集群：分别为每个节点准备一份配置（如 `config/node1.yaml`），
//! 再各启动一个 `raft-node` 进程。

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::raft::{NodeID, Options, Ticks};

/// 节点进程配置文件。
#[derive(Clone, Debug, Deserialize)]
pub struct NodeFileConfig {
    pub node: NodeSection,
    /// 同伴列表。**为空表示单节点模式**（启动后立即成为领导者）。
    #[serde(default)]
    pub peers: Vec<PeerSection>,
    #[serde(default)]
    pub options: OptionsSection,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NodeSection {
    pub id: NodeID,
    /// 监听地址，如 `127.0.0.1:7001`。
    pub listen: String,
    /// 数据目录（Raft 日志 + 快照）。
    pub data_dir: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PeerSection {
    pub id: NodeID,
    pub addr: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OptionsSection {
    #[serde(default = "default_heartbeat")]
    pub heartbeat_interval: Ticks,
    #[serde(default = "default_election_min")]
    pub election_timeout_min: Ticks,
    #[serde(default = "default_election_max")]
    pub election_timeout_max: Ticks,
    #[serde(default = "default_max_append")]
    pub max_append_entries: usize,
    #[serde(default = "default_true")]
    pub pre_vote: bool,
    #[serde(default = "default_true")]
    pub check_quorum: bool,
    /// 距上次快照 apply 了多少条后触发本地快照；0 表示关闭。
    #[serde(default = "default_snapshot_threshold")]
    pub snapshot_threshold: u64,
}

impl Default for OptionsSection {
    fn default() -> Self {
        Self {
            heartbeat_interval: default_heartbeat(),
            election_timeout_min: default_election_min(),
            election_timeout_max: default_election_max(),
            max_append_entries: default_max_append(),
            pre_vote: true,
            check_quorum: true,
            snapshot_threshold: default_snapshot_threshold(),
        }
    }
}

impl OptionsSection {
    pub fn to_options(&self) -> Options {
        assert!(
            self.election_timeout_min < self.election_timeout_max,
            "election_timeout_min must be < election_timeout_max"
        );
        Options {
            heartbeat_interval: self.heartbeat_interval,
            election_timeout_range: self.election_timeout_min..self.election_timeout_max,
            max_append_entries: self.max_append_entries,
            pre_vote: self.pre_vote,
            check_quorum: self.check_quorum,
            snapshot_threshold: self.snapshot_threshold,
        }
    }
}

fn default_heartbeat() -> Ticks {
    2
}
fn default_election_min() -> Ticks {
    5
}
fn default_election_max() -> Ticks {
    10
}
fn default_max_append() -> usize {
    100
}
fn default_true() -> bool {
    true
}
fn default_snapshot_threshold() -> u64 {
    1000
}

impl NodeFileConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = fs::read_to_string(path)
            .map_err(|e| Error::IO(format!("read config {}: {e}", path.display())))?;
        let cfg: Self = serde_yaml::from_str(&text)
            .map_err(|e| Error::InvalidData(format!("parse config {}: {e}", path.display())))?;
        if cfg.peers.iter().any(|p| p.id == cfg.node.id) {
            return Err(Error::InvalidInput("node id must not appear in peers".into()));
        }
        Ok(cfg)
    }

    pub fn listen_addr(&self) -> Result<SocketAddr> {
        self.node
            .listen
            .parse()
            .map_err(|e| Error::InvalidInput(format!("bad listen addr: {e}")))
    }

    pub fn peer_addrs(&self) -> Result<Vec<(NodeID, SocketAddr)>> {
        self.peers
            .iter()
            .map(|p| {
                let addr: SocketAddr = p
                    .addr
                    .parse()
                    .map_err(|e| Error::InvalidInput(format!("bad peer {} addr: {e}", p.id)))?;
                Ok((p.id, addr))
            })
            .collect()
    }
}
