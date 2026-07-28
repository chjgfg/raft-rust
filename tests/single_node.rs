//! 使用 BitCask 存储的单节点 Raft 冒烟测试。

// 空 peers 表示单节点
use std::collections::HashSet;

// 节点出站通道
use crossbeam::channel;
// 统一错误类型，冒烟测试以 Result 传播失败
use raft_rust::error::Result;
// KV 状态机
use raft_rust::raft::kv::{self, Kv};
// 核心 Node 与客户端协议
use raft_rust::raft::{Envelope, Log, Message, Node, Options, Request, Response};
// 持久化
use raft_rust::storage::BitCask;
// 客户端请求用 UUID 关联请求与响应
use uuid::Uuid;

// 创建唯一临时路径上的 BitCask 引擎
fn temp_engine() -> BitCask {
    // 拼出进程级唯一临时文件，避免并行测试互相踩踏
    let path = std::env::temp_dir().join(format!(
        // 文件名前缀标识单节点冒烟场景
        "raft-sn-{}-{}.log",
        // 进程号参与命名，隔离不同测试进程
        std::process::id(),
        // 纳秒时间戳进一步降低路径碰撞概率
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    // 结束多行调用
    ));
    // 打开（或新建）该路径上的 BitCask 引擎实例
    BitCask::new(path).expect("bitcask")
}

// 构造单节点 Node 及其出站接收端
fn make_node() -> Result<(Node, channel::Receiver<Envelope>)> {
    // 无界通道承接节点发出的 Envelope，测试侧可同步排空
    let (tx, rx) = channel::unbounded();
    // 用临时 BitCask 作为 Log 的底层 Engine
    let log = Log::new(Box::new(temp_engine()))?;
    // 单节点集群（无同伴）创建时立即成为领导者。
    let node = Node::new(1, HashSet::new(), log, Kv::new(), tx, Options::default())?;
    // 返回可变节点与出站接收端供断言
    Ok((node, rx))
}

/// 排空出站消息，并返回 `want_id` 对应的 ClientResponse。
fn take_response(rx: &channel::Receiver<Envelope>, want_id: Uuid) -> Result<Response> {
    // 非阻塞排空，直到匹配到目标客户端响应
    while let Ok(msg) = rx.try_recv() {
        // 仅关心匹配请求 id 的客户端响应
        if let Message::ClientResponse { id, response } = msg.message
            // 请求-响应通过同一 UUID 配对
            && id == want_id
        // 进入代码块
        {
            // 找到目标后直接返回内层业务结果
            return response;
        }
    }
    // 队列已空仍未匹配则视为测试失败
    panic!("missing ClientResponse for {want_id}");
}

// 验证单节点成为领导后 Put/Get/Status 全路径可用
#[test]
// 主路径：领导选举隐含完成 + KV 写读状态查询闭环
fn single_node_becomes_leader_and_serves_kv() -> Result<()> {
    // 启动无 peer 的单节点并拿到出站通道
    let (mut node, rx) = make_node()?;
    // 节点 id 为 1
    assert_eq!(node.id(), 1);
    // 无 peer 直接是 Leader
    assert!(matches!(node, Node::Leader(_)));

    // Put —— 单节点立即提交并应用。
    // 为本次写请求分配关联 id
    let put_id = Uuid::new_v4();
    // 构造 Put 命令：hello -> world
    let put_cmd = kv::Command::Put { key: "hello".into(), value: "world".into() };
    // 取当前任期，保证 Envelope 任期与节点一致
    let term = node.term();
    // 自发自收 ClientRequest 写入
    node = node.step(Envelope {
        // 单节点自发：from 即自身
        from: 1,
        // 目标也是自身，模拟本机客户端入口
        to: 1,
        // 携带当前任期避免被当作过期消息
        term,
        // 客户端写请求：序列化后的 Put 命令
        message: Message::ClientRequest {
            // 与后续 ClientResponse 配对
            id: put_id,
            // Write 路径走日志复制/提交/应用
            request: Request::Write(kv::encode(&put_cmd)),
        },
    // 结束闭包并传播错误
    })?;
    // 排空出站，取出 Put 对应响应
    match take_response(&rx, put_id)? {
        // 写路径应返回 Write 包装的状态机字节
        Response::Write(bytes) => {
            // 状态机返回 Put 确认
            assert!(matches!(kv::decode::<kv::Response>(&bytes)?, kv::Response::Put(_)));
        }
        // 其它变体说明写路径协议错位
        other => panic!("expected Write, got {other:?}"),
    }

    // Get —— 单节点立即以法定人数确认读。
    // 为读请求分配新的关联 id
    let get_id = Uuid::new_v4();
    // 读取刚写入的 hello 键
    let get_cmd = kv::Command::Get { key: "hello".into() };
    // 再次快照任期（写后任期通常不变，仍显式携带）
    let term = node.term();
    // 注入客户端读请求 Envelope
    node = node.step(Envelope {
        // 自发自收
        from: 1,
        // 目标自身
        to: 1,
        // 当前任期
        term,
        // ClientRequest 走 Read 路径
        message: Message::ClientRequest {
            // 读请求 id
            id: get_id,
            // 线性读：编码后的 Get 命令
            request: Request::Read(kv::encode(&get_cmd)),
        },
    // 结束闭包并传播错误
    })?;
    // 取出 Get 对应客户端响应
    match take_response(&rx, get_id)? {
        // 读路径应返回 Read 包装的状态机字节
        Response::Read(bytes) => {
            // 应读到刚写入的 world
            assert_eq!(
                // 解码状态机响应
                kv::decode::<kv::Response>(&bytes)?,
                // 期望值为 Some("world")
                kv::Response::Get(Some("world".into()))
            );
        }
        // 非 Read 变体视为失败
        other => panic!("expected Read, got {other:?}"),
    }

    // 状态查询
    // 为 Status 请求分配关联 id
    let status_id = Uuid::new_v4();
    // 快照当前任期
    let term = node.term();
    // 注入 Status 客户端请求
    node = node.step(Envelope {
        // 自发
        from: 1,
        // 自收
        to: 1,
        // 当前任期
        term,
        // 查询集群/领导/提交进度
        message: Message::ClientRequest { id: status_id, request: Request::Status },
    // 结束闭包并传播错误
    })?;
    // 取出 Status 响应
    match take_response(&rx, status_id)? {
        // 解包 Status 结构做字段断言
        Response::Status(s) => {
            // 自认为领导
            assert_eq!(s.leader, 1);
            // 至少提交了 noop/业务写之一
            assert!(s.commit_index >= 1);
        }
        // 非 Status 变体视为失败
        other => panic!("expected Status, got {other:?}"),
    }

    // 抑制未使用告警并保留节点至断言结束
    let _ = node;
    // 全路径通过
    Ok(())
}

// 验证 BitCask Engine 基本 set/get/delete/scan
#[test]
// 底层存储引擎独立冒烟：不经 Raft 直接验证 Engine 契约
fn bitcask_engine_roundtrip() -> Result<()> {
    // 引入 Engine trait 以调用 set/get/delete/scan
    use raft_rust::storage::Engine;
    // 新建临时引擎实例
    let mut eng = temp_engine();
    // 写入两键
    eng.set(b"a", b"1".to_vec())?;
    // 第二键 b=2，为后续 scan 留对照
    eng.set(b"b", b"2".to_vec())?;
    // 读回 a 应仍为 1
    assert_eq!(eng.get(b"a")?, Some(b"1".to_vec()));
    // 删除 a
    eng.delete(b"a")?;
    // 删除后 get 应返回 None
    assert_eq!(eng.get(b"a")?, None);
    // scan 应只剩 b
    // 全范围扫描收集剩余键
    let keys: Vec<_> = eng.scan(..).map(|r| r.unwrap().0).collect();
    // 仅剩 b 键
    assert_eq!(keys, vec![b"b".to_vec()]);
    // 引擎 roundtrip 完成
    Ok(())
}

// 验证 Log 层 append/commit/scan 与 term_vote 元数据
#[test]
// 日志层契约：任期投票、追加、提交索引与区间扫描
fn log_append_commit_scan() -> Result<()> {
    // 临时 BitCask 上构建 Raft Log
    let mut log = Log::new(Box::new(temp_engine()))?;
    // 设置当前任期与投票
    log.set_term_vote(1, Some(1))?;
    // 追加两条命令
    let i1 = log.append(Some(b"one".to_vec()))?;
    // 第二条业务命令 two
    let i2 = log.append(Some(b"two".to_vec()))?;
    // 索引从 1 起连续
    assert_eq!((i1, i2), (1, 2));
    // 提交到 2
    log.commit(2)?;
    // 扫描 [1,2] 应得到两条 command
    let entries: Vec<_> = log.scan(1..=2).map(|e| e.unwrap().command).collect();
    // 扫描结果顺序与内容与 append 一致
    assert_eq!(entries, vec![Some(b"one".to_vec()), Some(b"two".to_vec())]);
    // commit_index 与其 term
    assert_eq!(log.get_commit_index(), (2, 1));
    // 日志层契约验证通过
    Ok(())
}
