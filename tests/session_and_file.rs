//! Session 去重与 BitCask 冒烟。

// 单节点 peers
use std::collections::HashSet;
// 临时路径
use std::time::{SystemTime, UNIX_EPOCH};

// 节点出站通道
use crossbeam::channel;
// KV 与命令编解码
use raft_rust::raft::kv::{self, Command, Kv};
// Session 编码与状态机包装
use raft_rust::raft::session::{encode_session, SessionState};
// Raft 核心类型
use raft_rust::raft::{Envelope, Entry, Log, Message, Node, Options, Request, Response, State};
// 存储引擎
use raft_rust::storage::{BitCask, Engine};
// 会话/请求唯一标识
use uuid::Uuid;

// 生成带 tag 的临时 BitCask 路径
fn temp_path(tag: &str) -> std::path::PathBuf {
    // 拼出进程级唯一、可复现前缀的临时文件名
    std::env::temp_dir().join(format!(
        // 日志文件名模板：tag + pid + 纳秒时间戳
        "raft-{}-{}-{}.log",
        // 调用方传入的场景标签
        tag,
        // 当前进程号，避免多测并行冲突
        std::process::id(),
        // 纳秒时间戳保证同进程内多次调用也不撞名
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    // 结束 format! 与 join
    ))
// temp_path 结束
}

// 验证同一 client_id+seq 的重复 apply 幂等：返回相同结果且不覆盖状态
#[test]
// 同序重传应命中 Session 缓存而非再次执行写
fn session_dedup_same_seq() {
    // 空 Session 包装的 Kv
    let mut state = SessionState::new(Kv::new());
    // 固定客户端会话 id
    let cid = Uuid::new_v4();
    // 第一次：seq=1 写 k=v1
    let payload = kv::encode(&Command::Put { key: "k".into(), value: "v1".into() });
    // 将 client_id/seq 与业务 payload 封装为会话命令
    let cmd1 = encode_session(cid, 1, payload);
    // 构造首条日志条目供状态机 apply
    let e1 = Entry { index: 1, term: 1, command: Some(cmd1), membership: None };
    // 真正执行 Put
    let r1 = state.apply(e1).unwrap();

    // 第二次：同 cid、同 seq=1，但 payload 改为 v2（模拟重传）
    let cmd2 = encode_session(
        // 同一客户端会话
        cid,
        // 与首次相同的序号，触发去重
        1,
        // 故意换 value 以验证不会被二次执行
        kv::encode(&Command::Put { key: "k".into(), value: "v2".into() }),
    // 会话命令组装完毕
    );
    // 第二条日志：index 前进但 seq 重复
    let e2 = Entry { index: 2, term: 1, command: Some(cmd2), membership: None };
    // 应命中去重缓存，不执行第二次 Put
    let r2 = state.apply(e2).unwrap();
    // 响应字节应与首次相同（幂等）
    assert_eq!(r1, r2);

    // 状态机中 k 仍为 v1，未被 v2 覆盖
    let get = state.read(kv::encode(&Command::Get { key: "k".into() })).unwrap();
    // 解码只读响应，核对实际存储值
    let resp: kv::Response = kv::decode(&get).unwrap();
    // 确认仍是首次写入的 v1
    assert_eq!(resp, kv::Response::Get(Some("v1".into())));
// session_dedup_same_seq 结束
}

/// 更小的 seq 不得返回「较新写」的缓存结果。
#[test]
// 过期 seq 既不能重放新缓存，也不能改写状态
fn session_stale_seq_does_not_return_newer_cache() {
    // 干净 Session+Kv，无历史缓存
    let mut state = SessionState::new(Kv::new());
    // 同一客户端贯穿本用例
    let cid = Uuid::new_v4();

    // seq=1 写 a=1
    let e1 = Entry {
        // 日志下标从 1 起
        index: 1,
        // 任期固定为 1 即可
        term: 1,
        // 会话包装的 Put a=1
        command: Some(encode_session(
            // 客户端会话
            cid,
            // 首次序号
            1,
            // 业务：写入 a
            kv::encode(&Command::Put { key: "a".into(), value: "1".into() }),
        // encode_session 结束
        )),
        // 非成员变更条目
        membership: None,
    // Entry 构造结束
    };
    // 应用首条写，建立 session 缓存基线
    let _ = state.apply(e1).unwrap();

    // seq=2 写 b=2，缓存更新为较新响应
    let e2 = Entry {
        // 下一条日志
        index: 2,
        // 仍处同一任期
        term: 1,
        // 序号推进到 2 的会话写
        command: Some(encode_session(
            // 同一 client
            cid,
            // 更新会话最新 seq
            2,
            // 业务：写入 b
            kv::encode(&Command::Put { key: "b".into(), value: "2".into() }),
        // encode_session 结束
        )),
        // 无成员变更
        membership: None,
    // Entry 构造结束
    };
    // 保留 seq=2 的响应，供后续与过期请求对比
    let r2 = state.apply(e2).unwrap();

    // 过期 seq=1：不得返回 seq=2 的响应字节
    let e_stale = Entry {
        // 日志继续前进
        index: 3,
        // 任期不变
        term: 1,
        // 故意用已过期的 seq=1 重放
        command: Some(encode_session(
            // 同一会话
            cid,
            // 落后于当前最新 seq
            1,
            // 若误执行会把 a 改成 x
            kv::encode(&Command::Put { key: "a".into(), value: "x".into() }),
        // encode_session 结束
        )),
        // 非成员变更
        membership: None,
    // Entry 构造结束
    };
    // 过期 apply 应短路，不碰状态机
    let r_stale = state.apply(e_stale).unwrap();
    // 不得误返回 seq=2 的缓存
    assert_ne!(r_stale, r2);
    // 过期请求应返回空结果（不重放、不执行）
    assert!(r_stale.is_empty());

    // b 仍在，a 仍是第一次的值
    let get_b = state.read(kv::encode(&Command::Get { key: "b".into() })).unwrap();
    // 断言 b 仍为 seq=2 写入的值
    assert_eq!(
        // 解码只读结果
        kv::decode::<kv::Response>(&get_b).unwrap(),
        // 期望 Get(Some("2"))
        kv::Response::Get(Some("2".into()))
    // assert_eq 结束
    );
    // a 不得被过期重传改成 x
    let get_a = state.read(kv::encode(&Command::Get { key: "a".into() })).unwrap();
    // 断言 a 仍为首次写入的 1
    assert_eq!(
        // 解码 a 的 Get 响应
        kv::decode::<kv::Response>(&get_a).unwrap(),
        // 期望仍是 "1" 而非 "x"
        kv::Response::Get(Some("1".into()))
    // assert_eq 结束
    );
// session_stale_seq_does_not_return_newer_cache 结束
}

// 验证 BitCask 落盘后重开可读、删除后持久化生效
#[test]
// 写-关-开-删-再开，覆盖持久化与删除语义
fn bitcask_roundtrip() {
    // 本用例专用临时引擎路径
    let path = temp_path("bc");
    // 作用域结束后自动 drop 第一阶段引擎
    {
        // 第一阶段：写入并 flush
        let mut eng = BitCask::new(path.clone()).unwrap();
        // 写入键 a
        eng.set(b"a", b"1".to_vec()).unwrap();
        // 写入键 b
        eng.set(b"b", b"2".to_vec()).unwrap();
        // 强制刷盘，确保崩溃一致性语义可测
        eng.flush().unwrap();
    // 关闭第一阶段引擎
    }
    // 重开应看到 a、再删除 a
    let mut eng = BitCask::new(path.clone()).unwrap();
    // 校验 a 在重开后仍可读
    assert_eq!(eng.get(b"a").unwrap(), Some(b"1".to_vec()));
    // 删除 a，写入墓碑
    eng.delete(b"a").unwrap();
    // 将删除持久化
    eng.flush().unwrap();
    // 显式释放句柄，准备第三次打开
    drop(eng);
    // 第三次打开：a 已删，b 仍在
    let mut eng = BitCask::new(path).unwrap();
    // a 应已不可见
    assert_eq!(eng.get(b"a").unwrap(), None);
    // b 应仍保留
    assert_eq!(eng.get(b"b").unwrap(), Some(b"2".to_vec()));
// bitcask_roundtrip 结束
}

// 验证单节点经 WriteSession 路径提交带 client_id/seq 的写
#[test]
// 单节点 Leader 上走会话写并收到 ClientResponse
fn single_node_write_session() {
    // 捕获节点出站消息（含客户端响应）
    let (tx, rx) = channel::unbounded();
    // 临时日志
    let log = Log::new(Box::new(BitCask::new(temp_path("sess")).unwrap())).unwrap();
    // 单节点领导 + SessionState
    let node = Node::new(
        // 本节点 id
        1,
        // 无其他 peers：单节点即可成主
        HashSet::new(),
        // 持久化日志
        log,
        // Session 包装的业务状态机
        SessionState::new(Kv::new()),
        // 出站通道发送端
        tx,
        // 默认选举/心跳参数
        Options::default(),
    // Node::new 参数列表结束
    )
    // 构造失败则测试直接失败
    .unwrap();
    // 客户端请求 id（协议层）
    let id = Uuid::new_v4();
    // 会话 client_id（去重层）
    let cid = Uuid::new_v4();
    // 业务 Put x=y 的编码
    let cmd = kv::encode(&Command::Put { key: "x".into(), value: "y".into() });
    // 使用当前任期，避免消息因 term 过期被丢弃
    let term = node.term();
    // 走 WriteSession 而非裸 Write
    let _node = node
        // 自环投递客户端会话写请求
        .step(Envelope {
            // 来自本节点（测试驱动）
            from: 1,
            // 投递给本节点
            to: 1,
            // 与节点当前任期一致
            term,
            // 封装为 ClientRequest
            message: Message::ClientRequest {
                // 协议层请求关联 id
                id,
                // 会话写：携带 client_id 与 seq
                request: Request::WriteSession { client_id: cid, seq: 1, command: cmd },
            // ClientRequest 结束
            },
        // Envelope 结束
        })
        // step 失败则测试失败
        .unwrap();
    // 期望收到对应 Write 响应
    let mut got = false;
    // 排空出站队列，查找匹配 id 的响应
    while let Ok(msg) = rx.try_recv() {
        // 只关心客户端响应消息
        if let Message::ClientResponse { id: rid, response } = msg.message {
            // 匹配本次请求 id
            if rid == id {
                // 会话写成功应返回 Write
                assert!(matches!(response.unwrap(), Response::Write(_)));
                // 标记已收到期望响应
                got = true;
            // 匹配 id 分支结束
            }
        // ClientResponse 匹配结束
        }
    // 出站队列排空
    }
    // 必须至少收到一次成功的会话写响应
    assert!(got);
// single_node_write_session 结束
}
