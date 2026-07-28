//! 单节点进程配置（YAML）。
//!
//! 多节点集群：分别为每个节点准备一份配置（如 `config/node1.yaml`），
//! 再各启动一个 `raft-node` 进程。

// 读取 YAML 配置文件内容
use std::fs;
// 将 listen/peer 字符串解析为 socket 地址
use std::net::SocketAddr;
// 配置路径与数据目录路径
use std::path::{Path, PathBuf};

// 从 YAML 反序列化到结构体
use serde::Deserialize;

// 配置加载失败映射为库错误
use crate::error::{Error, Result};
// 节点 ID、Raft 运行参数与 tick 单位
use crate::raft::{NodeID, Options, Ticks};

/// 节点进程配置文件。
// 整份 YAML 的根结构：本节点 + 同伴 + 可选调参
#[derive(Clone, Debug, Deserialize)]
// 一份 YAML 对应一个 raft-node 进程
pub struct NodeFileConfig {
    // 本节点身份与监听/数据目录
    pub node: NodeSection,
    /// 同伴列表。**为空表示单节点模式**（启动后立即成为领导者）。
    // 缺省为空向量，便于单机快速启动
    #[serde(default)]
    // 出站连接目标；空列表即单节点自举
    pub peers: Vec<PeerSection>,
    // Raft 心跳/选举/快照等可选项，缺省用 OptionsSection::default
    #[serde(default)]
    // 未写 options 时走 Default，避免强制用户填满调参
    pub options: OptionsSection,
// 根配置结构结束
}

// YAML 中 `node:` 段：标识本进程在集群中的位置
#[derive(Clone, Debug, Deserialize)]
// 本进程在集群中的固定身份与本地资源路径
pub struct NodeSection {
    // Raft 节点 ID，集群内唯一
    pub id: NodeID,
    /// 监听地址，如 `127.0.0.1:7001`。
    pub listen: String,
    /// 数据目录（Raft 日志 + 快照）。
    pub data_dir: PathBuf,
// 本节点段结束
}

// YAML 中单个 peer：用于出站连接与路由
#[derive(Clone, Debug, Deserialize)]
// 集群中一个远程投票节点的可达信息
pub struct PeerSection {
    // 同伴节点 ID
    pub id: NodeID,
    // 同伴 TCP 地址字符串
    pub addr: String,
// 单个 peer 描述结束
}

// YAML 中 `options:` 段：映射到运行时 `Options`
#[derive(Clone, Debug, Deserialize)]
// 与 Node 内部 Options 一一对应的可序列化调参
pub struct OptionsSection {
    // 领导者心跳间隔（tick 数）
    #[serde(default = "default_heartbeat")]
    // 越小越快探测故障，越大越省带宽
    pub heartbeat_interval: Ticks,
    // 选举超时下限（含）
    #[serde(default = "default_election_min")]
    // 随机超时下界，应明显大于心跳间隔
    pub election_timeout_min: Ticks,
    // 选举超时上限（不含），节点在区间内随机，降低平票
    #[serde(default = "default_election_max")]
    // 随机超时上界（半开），与 min 构成合法 Range
    pub election_timeout_max: Ticks,
    // 单次 Append 最多携带的日志条数，控制 RPC 体积
    #[serde(default = "default_max_append")]
    // 批量复制上限，过大易阻塞小消息
    pub max_append_entries: usize,
    // 是否启用 Pre-vote，减少分区节点抬升任期
    #[serde(default = "default_true")]
    // 生产建议保持开启，避免无谓选举抬 term
    pub pre_vote: bool,
    // 是否启用 CheckQuorum，领导者失去多数时主动下台
    #[serde(default = "default_true")]
    // 防止网络分区后旧领导者继续服务写
    pub check_quorum: bool,
    /// 距上次快照 apply 了多少条后触发本地快照；0 表示关闭。
    #[serde(default = "default_snapshot_threshold")]
    // 日志压缩触发阈值；0 关闭自动快照
    pub snapshot_threshold: u64,
// options 段结束
}

// 未写 options 段时的默认调参
impl Default for OptionsSection {
    // 与各字段 serde default 函数对齐的整段默认值
    fn default() -> Self {
        // 与 serde default 函数保持一致，避免两处默认值漂移
        Self {
            // 默认心跳间隔
            heartbeat_interval: default_heartbeat(),
            // 默认选举超时下界
            election_timeout_min: default_election_min(),
            // 默认选举超时上界
            election_timeout_max: default_election_max(),
            // 默认单次 Append 批量上限
            max_append_entries: default_max_append(),
            // 默认开启 Pre-vote 与 CheckQuorum，偏向生产安全
            pre_vote: true,
            // 默认开启失去多数时主动下台
            check_quorum: true,
            // 默认快照阈值
            snapshot_threshold: default_snapshot_threshold(),
        // 默认结构体字面量结束
        }
    // Default::default 结束
    }
// OptionsSection::Default 结束
}

// 配置段 → 运行时 Options 的转换实现
impl OptionsSection {
    // 将配置段转换为 Node 构造所需的 Options
    pub fn to_options(&self) -> Options {
        // 选举超时区间必须合法，否则随机超时无意义
        assert!(
            // min 必须严格小于 max，否则 Range 为空
            self.election_timeout_min < self.election_timeout_max,
            // 断言失败时的诊断信息
            "election_timeout_min must be < election_timeout_max"
        // 断言调用结束
        );
        // 组装运行时 Options（Range 为半开区间）
        Options {
            // 透传心跳间隔
            heartbeat_interval: self.heartbeat_interval,
            // min..max 半开区间，与论文中随机选举超时一致
            election_timeout_range: self.election_timeout_min..self.election_timeout_max,
            // 透传批量 Append 上限
            max_append_entries: self.max_append_entries,
            // 透传 Pre-vote 开关
            pre_vote: self.pre_vote,
            // 透传 CheckQuorum 开关
            check_quorum: self.check_quorum,
            // 透传快照阈值
            snapshot_threshold: self.snapshot_threshold,
        // Options 字面量结束
        }
    // to_options 结束
    }
// OptionsSection impl 结束
}

// 默认心跳：每 2 个 tick（配合 TICK_INTERVAL 约 200ms）
fn default_heartbeat() -> Ticks {
    // 2 tick ≈ 常见 100ms tick 下的 200ms 心跳
    2
// default_heartbeat 结束
}
// 默认选举超时下限 5 tick
fn default_election_min() -> Ticks {
    // 略大于数倍心跳，减少无谓选举
    5
// default_election_min 结束
}
// 默认选举超时上限 10 tick
fn default_election_max() -> Ticks {
    // 与 min 拉开区间，降低同时超时概率
    10
// default_election_max 结束
}
// 默认单次 Append 最多 100 条，平衡吞吐与包大小
fn default_max_append() -> usize {
    // 100 条是教学实现的折中默认
    100
// default_max_append 结束
}
// serde 布尔字段默认 true
fn default_true() -> bool {
    // 供 pre_vote/check_quorum 等字段的 serde default 复用
    true
// default_true 结束
}
// 默认每 apply 1000 条触发一次本地快照
fn default_snapshot_threshold() -> u64 {
    // 1000 条后压缩，避免日志无限增长
    1000
// default_snapshot_threshold 结束
}

// 根配置的加载与地址解析
impl NodeFileConfig {
    // 从磁盘路径加载并校验整份节点配置
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        // 规范化为 Path 引用
        let path = path.as_ref();
        // 读取 YAML 文本；失败包装为带路径的 IO 错误
        let text = fs::read_to_string(path)
            // 保留路径信息，便于运维定位缺文件/权限问题
            .map_err(|e| Error::IO(format!("read config {}: {e}", path.display())))?;
        // 反序列化；YAML 语法/字段错误 → InvalidData
        let cfg: Self = serde_yaml::from_str(&text)
            // 解析失败同样带上路径
            .map_err(|e| Error::InvalidData(format!("parse config {}: {e}", path.display())))?;
        // 本节点 ID 不得出现在 peers，避免自连与成员集合重复
        if cfg.peers.iter().any(|p| p.id == cfg.node.id) {
            // 配置自相矛盾：自己既是 node 又是 peer
            return Err(Error::InvalidInput("node id must not appear in peers".into()));
        // ID 冲突检查结束
        }
        // 校验通过，返回配置
        Ok(cfg)
    // load 结束
    }

    // 解析本节点监听地址，供 TcpListener 绑定
    pub fn listen_addr(&self) -> Result<SocketAddr> {
        // 将 "host:port" 解析为 SocketAddr
        self.node
            // 取出 YAML 中的 listen 字符串
            .listen
            // 解析为标准 SocketAddr
            .parse()
            // 非法地址 → 用户输入错误
            .map_err(|e| Error::InvalidInput(format!("bad listen addr: {e}")))
    // listen_addr 结束
    }

    // 解析全部 peer 地址，供 PeerOutbox 出站路由表使用
    pub fn peer_addrs(&self) -> Result<Vec<(NodeID, SocketAddr)>> {
        // 逐个 peer 解析；任一失败则整体失败
        self.peers
            // 遍历配置中的每个同伴
            .iter()
            // 将 PeerSection 转为 (NodeID, SocketAddr)
            .map(|p| {
                // 将 peer 的地址字符串解析为 SocketAddr
                let addr: SocketAddr = p
                    // 取出该 peer 的地址字段
                    .addr
                    // 解析 host:port
                    .parse()
                    // 失败时标明是哪个 peer id
                    .map_err(|e| Error::InvalidInput(format!("bad peer {} addr: {e}", p.id)))?;
                // 返回 (节点 ID, 地址) 二元组
                Ok((p.id, addr))
            // 单个 peer 映射闭包结束
            })
            // 收集为 Vec，传播第一个错误
            .collect()
    // peer_addrs 结束
    }
// NodeFileConfig impl 结束
}
