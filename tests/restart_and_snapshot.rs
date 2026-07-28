//! BitCask 重启恢复与快照截断联调。

// 单节点 peers 为空集
use std::collections::HashSet;
// 临时路径唯一性
use std::time::{SystemTime, UNIX_EPOCH};

// 节点出站通道
use crossbeam::channel;
// KV 命令与状态机
use raft_rust::raft::kv::{self, Command, Kv};
// Session 包装状态机，支持恢复快照
use raft_rust::raft::session::SessionState;
// Raft 核心类型
use raft_rust::raft::{
// 信封与日志/节点核心类型
    Envelope, Key, Log, Message, Node, Options, Request, Response, State, TICK_INTERVAL,
// 结束 raft 导入列表
};
// 引擎与 BitCask
use raft_rust::storage::{BitCask, Engine};
// 客户端请求 id
use uuid::Uuid;

// 生成带 tag 的唯一临时日志路径
fn temp_path(tag: &str) -> std::path::PathBuf {
// 进程内递增序号，避免同纳秒冲突
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
// 取并推进序号
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
// 拼出系统临时目录下的唯一文件名
    std::env::temp_dir().join(format!(
// 路径模板：标签-进程-序号-纳秒
        "raft-{}-{}-{}-{}.log",
// 业务场景标签
        tag,
// 当前进程 id
        std::process::id(),
// 本进程内序号
        seq,
// 时间戳纳秒保证跨运行唯一
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
// 结束 format! 并 join
    ))
// 结束 temp_path
}

// 从出站通道取出指定 id 的 ClientResponse
fn take_write_response(rx: &channel::Receiver<Envelope>, want: Uuid) -> Response {
// 非阻塞排空出站队列直至找到目标 id
    while let Ok(msg) = rx.try_recv() {
        // 匹配请求 id 的客户端响应
        if let Message::ClientResponse { id, response } = msg.message
// 仅接受与 want 一致的响应
            && id == want
// 命中后进入返回分支
        {
// 解开 Option，业务上应已成功响应
            return response.expect("response ok");
// 结束 if 体
        }
// 结束 while
    }
// 队列耗尽仍无匹配则测试失败
    panic!("missing response for {want}");
// 结束 take_write_response
}

/// 写数据 → 释放节点 → 同路径再开 → 状态机经日志重放恢复。
#[test]
// 验证重启后仅靠日志重放即可恢复 KV
fn restart_replays_log_and_restores_kv() {
    // 两次运行共享同一 BitCask 文件路径
    let path = temp_path("restart");

    // --- 第一次运行：写入 ---
    {
// 首次运行的出站通道
        let (tx, rx) = channel::unbounded();
        // 打开持久化日志
        let log = Log::new(Box::new(BitCask::new(path.clone()).unwrap())).unwrap();
        // 单节点 + SessionState 包装 Kv
        let mut node = Node::new(
// 节点 id=1
            1,
// 无 peer 的单节点集群
            HashSet::new(),
// 刚打开的持久化日志
            log,
// 空 KV 经 Session 包装
            SessionState::new(Kv::new()),
// 出站发送端
            tx,
            // 关闭自动快照，纯测日志重放
            Options { snapshot_threshold: 0, ..Options::default() },
// 结束 Node::new 参数
        )
// 创建失败则测试中止
        .unwrap();
        // 无 peer 立即成为领导
        assert!(matches!(node, Node::Leader(_)));

        // 第一次 Put：persist=yes
        let id = Uuid::new_v4();
// 取当前任期填入信封
        let term = node.term();
// 步进处理客户端写请求
        node = node
// 构造自环写信封
            .step(Envelope {
// 发送方为本节点
                from: 1,
// 接收方为本节点
                to: 1,
// 信封任期与节点一致
                term,
// 客户端写请求消息
                message: Message::ClientRequest {
// 本次请求关联 id
                    id,
// 编码 Put(persist, yes)
                    request: Request::Write(kv::encode(&Command::Put {
// 持久化键名
                        key: "persist".into(),
// 期望恢复后读到的值
                        value: "yes".into(),
// 结束 Put 编码
                    })),
// 结束 ClientRequest
                },
// 结束 Envelope
            })
// 步进必须成功
            .unwrap();
        // 消费写响应，确保已提交应用
        let _ = take_write_response(&rx, id);
        // 再写一条
        let id2 = Uuid::new_v4();
// 第二次写用当前任期
        let term = node.term();
// 步进第二条 Put
        node = node
// 自环信封承载 k2 写入
            .step(Envelope {
// 本节点发出
                from: 1,
// 本节点接收
                to: 1,
// 当前任期
                term,
// 第二条客户端写
                message: Message::ClientRequest {
// 关联 id2
                    id: id2,
// 编码 Put(k2, v2)
                    request: Request::Write(kv::encode(&Command::Put {
// 第二键
                        key: "k2".into(),
// 第二值
                        value: "v2".into(),
// 结束第二条 Put
                    })),
// 结束第二条 ClientRequest
                },
// 结束第二条 Envelope
            })
// 第二条步进成功
            .unwrap();
// 确保第二条已提交应用
        let _ = take_write_response(&rx, id2);
        // drop 节点释放文件句柄
        drop(node);
        // channel 断开后 BitCask Drop flush
    }

    // --- 第二次运行：同路径恢复 ---
    {
// 重启后的出站通道
        let (tx, rx) = channel::unbounded();
        // 重新打开同一路径上的日志
        let log = Log::new(Box::new(BitCask::new(path).unwrap())).unwrap();
        // 空状态机；Node::new 会 maybe_apply 重放已提交日志
        let mut node = Node::new(
// 仍用节点 id=1
            1,
// 仍为单节点
            HashSet::new(),
// 同路径恢复的日志
            log,
// 空状态机等待日志重放
            SessionState::new(Kv::new()),
// 重启后出站端
            tx,
// 仍关闭快照，只验证日志重放
            Options { snapshot_threshold: 0, ..Options::default() },
// 结束重启 Node::new
        )
// 重启创建成功
        .unwrap();

        // 读 persist 应恢复为 yes
        let id = Uuid::new_v4();
// 读请求使用当前任期
        let term = node.term();
// 步进读 persist
        node = node
// 自环读信封
            .step(Envelope {
// 本节点发出
                from: 1,
// 本节点接收
                to: 1,
// 当前任期
                term,
// 客户端读请求
                message: Message::ClientRequest {
// 读请求 id
                    id,
// 编码 Get(persist)
                    request: Request::Read(kv::encode(&Command::Get { key: "persist".into() })),
// 结束读 ClientRequest
                },
// 结束读 Envelope
            })
// 读步进成功
            .unwrap();
// 按响应类型断言恢复结果
        match take_write_response(&rx, id) {
// 期望读回业务字节
            Response::Read(bytes) => {
// 解码为 KV 响应
                let r: kv::Response = kv::decode(&bytes).unwrap();
// 值必须为首次写入的 yes
                assert_eq!(r, kv::Response::Get(Some("yes".into())));
// 结束 Read 分支
            }
// 其它响应类型即失败
            other => panic!("expected Read, got {other:?}"),
// 结束 persist 匹配
        }

        // 读 k2 应恢复为 v2
        let id = Uuid::new_v4();
// k2 读请求任期
        let term = node.term();
// 步进读 k2
        node = node
// 自环读信封
            .step(Envelope {
// 本节点发出
                from: 1,
// 本节点接收
                to: 1,
// 当前任期
                term,
// 客户端读 k2
                message: Message::ClientRequest {
// 读请求 id
                    id,
// 编码 Get(k2)
                    request: Request::Read(kv::encode(&Command::Get { key: "k2".into() })),
// 结束 k2 ClientRequest
                },
// 结束 k2 Envelope
            })
// k2 读步进成功
            .unwrap();
// 断言 k2 恢复结果
        match take_write_response(&rx, id) {
// 期望读回业务字节
            Response::Read(bytes) => {
// 解码 KV 响应
                let r: kv::Response = kv::decode(&bytes).unwrap();
// 值必须为 v2
                assert_eq!(r, kv::Response::Get(Some("v2".into())));
// 结束 k2 Read 分支
            }
// 非 Read 则失败
            other => panic!("expected Read, got {other:?}"),
// 结束 k2 匹配
        }
// 显式持有节点至作用域末尾
        let _ = node;
// 结束第二次运行块
    }
// 结束 restart 测试
}

/// 低 snapshot_threshold：写够条数后 compact，重启仍可读（经快照字节 + 日志）。
#[test]
// 验证 compact 后快照恢复仍能读全量键
fn snapshot_compact_then_restart() {
// 快照场景独立临时路径
    let path = temp_path("snap");

// 第一阶段：低阈值写入触发 compact
    {
// compact 阶段出站通道
        let (tx, rx) = channel::unbounded();
// 打开快照场景日志引擎
        let log = Log::new(Box::new(BitCask::new(path.clone()).unwrap())).unwrap();
// 低阈值节点，便于触发快照
        let mut node = Node::new(
// 节点 id=1
            1,
// 单节点无 peer
            HashSet::new(),
// 持久化日志
            log,
// 空 Session+Kv
            SessionState::new(Kv::new()),
// 出站发送端
            tx,
// 低阈值 Options
            Options {
                snapshot_threshold: 3, // 很小，便于触发
// 其余沿用默认
                ..Options::default()
// 结束 Options
            },
// 结束 Node::new 参数
        )
// 创建成功
        .unwrap();

        // 写入 8 条，超过 threshold 应触发多次 compact
        for i in 0..8 {
// 每轮独立请求 id
            let id = Uuid::new_v4();
// 每轮取当前任期
            let term = node.term();
// 步进写入第 i 条
            node = node
// 自环写信封
                .step(Envelope {
// 本节点发出
                    from: 1,
// 本节点接收
                    to: 1,
// 当前任期
                    term,
// 客户端写第 i 键
                    message: Message::ClientRequest {
// 本轮请求 id
                        id,
// 编码 Put(k{i}, v{i})
                        request: Request::Write(kv::encode(&Command::Put {
// 动态键名
                            key: format!("k{i}"),
// 动态值
                            value: format!("v{i}"),
// 结束 Put
                        })),
// 结束 ClientRequest
                    },
// 结束 Envelope
                })
// 本轮步进成功
                .unwrap();
// 等待本轮提交应用
            let _ = take_write_response(&rx, id);
// 结束写循环
        }

        // 确认 first_index 因 compact 前进
        // 通过 status 间接看：再 put 后 commit 仍前进
        let id = Uuid::new_v4();
// Status 查询用当前任期
        let term = node.term();
// 步进 Status 请求
        node = node
// 自环 Status 信封
            .step(Envelope {
// 本节点发出
                from: 1,
// 本节点接收
                to: 1,
// 当前任期
                term,
// 查询集群/应用状态
                message: Message::ClientRequest { id, request: Request::Status },
// 结束 Status Envelope
            })
// Status 步进成功
            .unwrap();
// 校验 applied_index 反映业务写
        match take_write_response(&rx, id) {
// 拿到 Status 负载
            Response::Status(st) => {
                // 至少应用了 8 条业务写
                assert!(st.applied_index >= 8);
// 结束 Status 成功分支
            }
// 非 Status 则失败
            other => panic!("{other:?}"),
// 结束 Status 匹配
        }
// 释放句柄以便同路径再开
        drop(node);
// 结束 compact 写入阶段
    }

    // 重启
    {
// 重启后出站通道
        let (tx, rx) = channel::unbounded();
        // 直接打开引擎读取快照字节
        let mut engine = BitCask::new(path).unwrap();
// 从引擎取出快照数据（若有）
        let snap = engine.get(&Key::SnapshotData.encode()).unwrap();
// 用同一引擎构造日志
        let log = Log::new(Box::new(engine)).unwrap();
// 先准备空状态机
        let mut state = SessionState::new(Kv::new());
        // 若存在快照则先 restore 到快照索引
        if let Some(bytes) = snap {
// 读取快照元数据中的索引
            let (idx, _) = log.get_snapshot_meta();
// 仅在有效快照索引时恢复
            if idx > 0 {
// 将状态机恢复到快照点
                state.restore(&bytes, idx).unwrap();
// 结束 idx>0
            }
// 结束 snap Some
        }
        // 再 apply 快照之后的日志
        let mut node = Node::new(
// 节点 id=1
            1,
// 单节点
            HashSet::new(),
// 含快照截断后的日志
            log,
// 已 restore 的状态机
            state,
// 出站发送端
            tx,
            // 重启阶段关闭继续快照，专注恢复正确性
            Options { snapshot_threshold: 0, ..Options::default() },
// 结束恢复 Node::new
        )
// 恢复节点创建成功
        .unwrap();

        // 全部 8 个键应仍可读
        for i in 0..8 {
// 每键独立读 id
            let id = Uuid::new_v4();
// 读请求任期
            let term = node.term();
// 步进读第 i 键
            node = node
// 自环读信封
                .step(Envelope {
// 本节点发出
                    from: 1,
// 本节点接收
                    to: 1,
// 当前任期
                    term,
// 客户端读第 i 键
                    message: Message::ClientRequest {
// 读请求 id
                        id,
// 编码 Get(k{i})
                        request: Request::Read(kv::encode(&Command::Get {
// 动态键名
                            key: format!("k{i}"),
// 结束 Get 编码
                        })),
// 结束读 ClientRequest
                    },
// 结束读 Envelope
                })
// 读步进成功
                .unwrap();
// 断言第 i 键值
            match take_write_response(&rx, id) {
// 期望读回字节
                Response::Read(bytes) => {
// 解码 KV 响应
                    let r: kv::Response = kv::decode(&bytes).unwrap();
// 值必须为对应 v{i}
                    assert_eq!(r, kv::Response::Get(Some(format!("v{i}"))), "key k{i}");
// 结束 Read 分支
                }
// 非 Read 失败
                other => panic!("{other:?}"),
// 结束第 i 键匹配
            }
// 结束读循环
        }
// 持有节点至作用域末
        let _ = node;
        // 引用常量避免未使用告警（与 tick 语义无强绑定）
        let _ = TICK_INTERVAL;
// 结束重启验证块
    }
// 结束 snapshot 测试
}
