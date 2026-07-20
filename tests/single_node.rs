//! 使用 Memory 存储的单节点 Raft 冒烟测试。

use std::collections::HashSet;

use crossbeam::channel;
use raft_rust::error::Result;
use raft_rust::raft::kv::{self, Kv};
use raft_rust::raft::{Envelope, Log, Message, Node, Options, Request, Response};
use raft_rust::storage::Memory;
use uuid::Uuid;

fn make_node() -> Result<(Node, channel::Receiver<Envelope>)> {
    let (tx, rx) = channel::unbounded();
    let log = Log::new(Box::new(Memory::new()))?;
    // 单节点集群（无同伴）创建时立即成为领导者。
    let node = Node::new(1, HashSet::new(), log, Kv::new(), tx, Options::default())?;
    Ok((node, rx))
}

/// 排空出站消息，并返回 `want_id` 对应的 ClientResponse。
fn take_response(rx: &channel::Receiver<Envelope>, want_id: Uuid) -> Result<Response> {
    while let Ok(msg) = rx.try_recv() {
        if let Message::ClientResponse { id, response } = msg.message
            && id == want_id
        {
            return response;
        }
    }
    panic!("missing ClientResponse for {want_id}");
}

#[test]
fn single_node_becomes_leader_and_serves_kv() -> Result<()> {
    let (mut node, rx) = make_node()?;
    assert_eq!(node.id(), 1);
    assert!(matches!(node, Node::Leader(_)));

    // Put —— 单节点立即提交并应用。
    let put_id = Uuid::new_v4();
    let put_cmd = kv::Command::Put { key: "hello".into(), value: "world".into() };
    let term = node.term();
    node = node.step(Envelope {
        from: 1,
        to: 1,
        term,
        message: Message::ClientRequest {
            id: put_id,
            request: Request::Write(kv::encode(&put_cmd)),
        },
    })?;
    match take_response(&rx, put_id)? {
        Response::Write(bytes) => {
            assert!(matches!(kv::decode::<kv::Response>(&bytes)?, kv::Response::Put(_)));
        }
        other => panic!("expected Write, got {other:?}"),
    }

    // Get —— 单节点立即以法定人数确认读。
    let get_id = Uuid::new_v4();
    let get_cmd = kv::Command::Get { key: "hello".into() };
    let term = node.term();
    node = node.step(Envelope {
        from: 1,
        to: 1,
        term,
        message: Message::ClientRequest {
            id: get_id,
            request: Request::Read(kv::encode(&get_cmd)),
        },
    })?;
    match take_response(&rx, get_id)? {
        Response::Read(bytes) => {
            assert_eq!(
                kv::decode::<kv::Response>(&bytes)?,
                kv::Response::Get(Some("world".into()))
            );
        }
        other => panic!("expected Read, got {other:?}"),
    }

    // 状态查询
    let status_id = Uuid::new_v4();
    let term = node.term();
    node = node.step(Envelope {
        from: 1,
        to: 1,
        term,
        message: Message::ClientRequest { id: status_id, request: Request::Status },
    })?;
    match take_response(&rx, status_id)? {
        Response::Status(s) => {
            assert_eq!(s.leader, 1);
            assert!(s.commit_index >= 1);
        }
        other => panic!("expected Status, got {other:?}"),
    }

    let _ = node;
    Ok(())
}

#[test]
fn memory_engine_roundtrip() -> Result<()> {
    use raft_rust::storage::Engine;
    let mut eng = Memory::new();
    eng.set(b"a", b"1".to_vec())?;
    eng.set(b"b", b"2".to_vec())?;
    assert_eq!(eng.get(b"a")?, Some(b"1".to_vec()));
    eng.delete(b"a")?;
    assert_eq!(eng.get(b"a")?, None);
    let keys: Vec<_> = eng.scan(..).map(|r| r.unwrap().0).collect();
    assert_eq!(keys, vec![b"b".to_vec()]);
    Ok(())
}

#[test]
fn log_append_commit_scan() -> Result<()> {
    let mut log = Log::new(Box::new(Memory::new()))?;
    log.set_term_vote(1, Some(1))?;
    let i1 = log.append(Some(b"one".to_vec()))?;
    let i2 = log.append(Some(b"two".to_vec()))?;
    assert_eq!((i1, i2), (1, 2));
    log.commit(2)?;
    let entries: Vec<_> = log.scan(1..=2).map(|e| e.unwrap().command).collect();
    assert_eq!(entries, vec![Some(b"one".to_vec()), Some(b"two".to_vec())]);
    assert_eq!(log.get_commit_index(), (2, 1));
    Ok(())
}
