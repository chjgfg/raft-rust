//! BitCask 重启恢复与快照截断联调。

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam::channel;
use raft_rust::raft::kv::{self, Command, Kv};
use raft_rust::raft::session::SessionState;
use raft_rust::raft::{
    Envelope, Key, Log, Message, Node, Options, Request, Response, State, TICK_INTERVAL,
};
use raft_rust::storage::{BitCask, Engine};
use uuid::Uuid;

fn temp_path(tag: &str) -> std::path::PathBuf {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "raft-{}-{}-{}-{}.log",
        tag,
        std::process::id(),
        seq,
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    ))
}

fn take_write_response(rx: &channel::Receiver<Envelope>, want: Uuid) -> Response {
    while let Ok(msg) = rx.try_recv() {
        if let Message::ClientResponse { id, response } = msg.message
            && id == want
        {
            return response.expect("response ok");
        }
    }
    panic!("missing response for {want}");
}

/// 写数据 → 释放节点 → 同路径再开 → 状态机经日志重放恢复。
#[test]
fn restart_replays_log_and_restores_kv() {
    let path = temp_path("restart");

    // --- 第一次运行：写入 ---
    {
        let (tx, rx) = channel::unbounded();
        let log = Log::new(Box::new(BitCask::new(path.clone()).unwrap())).unwrap();
        let mut node = Node::new(
            1,
            HashSet::new(),
            log,
            SessionState::new(Kv::new()),
            tx,
            Options { snapshot_threshold: 0, ..Options::default() },
        )
        .unwrap();
        assert!(matches!(node, Node::Leader(_)));

        let id = Uuid::new_v4();
        let term = node.term();
        node = node
            .step(Envelope {
                from: 1,
                to: 1,
                term,
                message: Message::ClientRequest {
                    id,
                    request: Request::Write(kv::encode(&Command::Put {
                        key: "persist".into(),
                        value: "yes".into(),
                    })),
                },
            })
            .unwrap();
        let _ = take_write_response(&rx, id);
        // 再写一条
        let id2 = Uuid::new_v4();
        let term = node.term();
        node = node
            .step(Envelope {
                from: 1,
                to: 1,
                term,
                message: Message::ClientRequest {
                    id: id2,
                    request: Request::Write(kv::encode(&Command::Put {
                        key: "k2".into(),
                        value: "v2".into(),
                    })),
                },
            })
            .unwrap();
        let _ = take_write_response(&rx, id2);
        drop(node);
        // channel 断开后 BitCask Drop flush
    }

    // --- 第二次运行：同路径恢复 ---
    {
        let (tx, rx) = channel::unbounded();
        let log = Log::new(Box::new(BitCask::new(path).unwrap())).unwrap();
        // 空状态机；Node::new 会 maybe_apply 重放已提交日志
        let mut node = Node::new(
            1,
            HashSet::new(),
            log,
            SessionState::new(Kv::new()),
            tx,
            Options { snapshot_threshold: 0, ..Options::default() },
        )
        .unwrap();

        let id = Uuid::new_v4();
        let term = node.term();
        node = node
            .step(Envelope {
                from: 1,
                to: 1,
                term,
                message: Message::ClientRequest {
                    id,
                    request: Request::Read(kv::encode(&Command::Get { key: "persist".into() })),
                },
            })
            .unwrap();
        match take_write_response(&rx, id) {
            Response::Read(bytes) => {
                let r: kv::Response = kv::decode(&bytes).unwrap();
                assert_eq!(r, kv::Response::Get(Some("yes".into())));
            }
            other => panic!("expected Read, got {other:?}"),
        }

        let id = Uuid::new_v4();
        let term = node.term();
        node = node
            .step(Envelope {
                from: 1,
                to: 1,
                term,
                message: Message::ClientRequest {
                    id,
                    request: Request::Read(kv::encode(&Command::Get { key: "k2".into() })),
                },
            })
            .unwrap();
        match take_write_response(&rx, id) {
            Response::Read(bytes) => {
                let r: kv::Response = kv::decode(&bytes).unwrap();
                assert_eq!(r, kv::Response::Get(Some("v2".into())));
            }
            other => panic!("expected Read, got {other:?}"),
        }
        let _ = node;
    }
}

/// 低 snapshot_threshold：写够条数后 compact，重启仍可读（经快照字节 + 日志）。
#[test]
fn snapshot_compact_then_restart() {
    let path = temp_path("snap");

    {
        let (tx, rx) = channel::unbounded();
        let log = Log::new(Box::new(BitCask::new(path.clone()).unwrap())).unwrap();
        let mut node = Node::new(
            1,
            HashSet::new(),
            log,
            SessionState::new(Kv::new()),
            tx,
            Options {
                snapshot_threshold: 3, // 很小，便于触发
                ..Options::default()
            },
        )
        .unwrap();

        for i in 0..8 {
            let id = Uuid::new_v4();
            let term = node.term();
            node = node
                .step(Envelope {
                    from: 1,
                    to: 1,
                    term,
                    message: Message::ClientRequest {
                        id,
                        request: Request::Write(kv::encode(&Command::Put {
                            key: format!("k{i}"),
                            value: format!("v{i}"),
                        })),
                    },
                })
                .unwrap();
            let _ = take_write_response(&rx, id);
        }

        // 确认 first_index 因 compact 前进
        // 通过 status 间接看：再 put 后 commit 仍前进
        let id = Uuid::new_v4();
        let term = node.term();
        node = node
            .step(Envelope {
                from: 1,
                to: 1,
                term,
                message: Message::ClientRequest { id, request: Request::Status },
            })
            .unwrap();
        match take_write_response(&rx, id) {
            Response::Status(st) => {
                assert!(st.applied_index >= 8);
            }
            other => panic!("{other:?}"),
        }
        drop(node);
    }

    // 重启
    {
        let (tx, rx) = channel::unbounded();
        let mut engine = BitCask::new(path).unwrap();
        let snap = engine.get(&Key::SnapshotData.encode()).unwrap();
        let log = Log::new(Box::new(engine)).unwrap();
        let mut state = SessionState::new(Kv::new());
        if let Some(bytes) = snap {
            let (idx, _) = log.get_snapshot_meta();
            if idx > 0 {
                state.restore(&bytes, idx).unwrap();
            }
        }
        // 再 apply 快照之后的日志
        let mut node = Node::new(
            1,
            HashSet::new(),
            log,
            state,
            tx,
            Options { snapshot_threshold: 0, ..Options::default() },
        )
        .unwrap();

        for i in 0..8 {
            let id = Uuid::new_v4();
            let term = node.term();
            node = node
                .step(Envelope {
                    from: 1,
                    to: 1,
                    term,
                    message: Message::ClientRequest {
                        id,
                        request: Request::Read(kv::encode(&Command::Get {
                            key: format!("k{i}"),
                        })),
                    },
                })
                .unwrap();
            match take_write_response(&rx, id) {
                Response::Read(bytes) => {
                    let r: kv::Response = kv::decode(&bytes).unwrap();
                    assert_eq!(r, kv::Response::Get(Some(format!("v{i}"))), "key k{i}");
                }
                other => panic!("{other:?}"),
            }
        }
        let _ = node;
        let _ = TICK_INTERVAL;
    }
}
