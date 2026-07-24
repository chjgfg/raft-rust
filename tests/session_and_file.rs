//! Session 去重与 BitCask 冒烟。

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam::channel;
use raft_rust::raft::kv::{self, Command, Kv};
use raft_rust::raft::session::{encode_session, SessionState};
use raft_rust::raft::{Envelope, Entry, Log, Message, Node, Options, Request, Response, State};
use raft_rust::storage::{BitCask, Engine};
use uuid::Uuid;

fn temp_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "raft-{}-{}-{}.log",
        tag,
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    ))
}

#[test]
fn session_dedup_same_seq() {
    let mut state = SessionState::new(Kv::new());
    let cid = Uuid::new_v4();
    let payload = kv::encode(&Command::Put { key: "k".into(), value: "v1".into() });
    let cmd1 = encode_session(cid, 1, payload);
    let e1 = Entry { index: 1, term: 1, command: Some(cmd1), membership: None };
    let r1 = state.apply(e1).unwrap();

    let cmd2 = encode_session(
        cid,
        1,
        kv::encode(&Command::Put { key: "k".into(), value: "v2".into() }),
    );
    let e2 = Entry { index: 2, term: 1, command: Some(cmd2), membership: None };
    let r2 = state.apply(e2).unwrap();
    assert_eq!(r1, r2);

    let get = state.read(kv::encode(&Command::Get { key: "k".into() })).unwrap();
    let resp: kv::Response = kv::decode(&get).unwrap();
    assert_eq!(resp, kv::Response::Get(Some("v1".into())));
}

/// 更小的 seq 不得返回「较新写」的缓存结果。
#[test]
fn session_stale_seq_does_not_return_newer_cache() {
    let mut state = SessionState::new(Kv::new());
    let cid = Uuid::new_v4();

    let e1 = Entry {
        index: 1,
        term: 1,
        command: Some(encode_session(
            cid,
            1,
            kv::encode(&Command::Put { key: "a".into(), value: "1".into() }),
        )),
        membership: None,
    };
    let _ = state.apply(e1).unwrap();

    let e2 = Entry {
        index: 2,
        term: 1,
        command: Some(encode_session(
            cid,
            2,
            kv::encode(&Command::Put { key: "b".into(), value: "2".into() }),
        )),
        membership: None,
    };
    let r2 = state.apply(e2).unwrap();

    // 过期 seq=1：不得返回 seq=2 的响应字节
    let e_stale = Entry {
        index: 3,
        term: 1,
        command: Some(encode_session(
            cid,
            1,
            kv::encode(&Command::Put { key: "a".into(), value: "x".into() }),
        )),
        membership: None,
    };
    let r_stale = state.apply(e_stale).unwrap();
    assert_ne!(r_stale, r2);
    assert!(r_stale.is_empty());

    // b 仍在，a 仍是第一次的值
    let get_b = state.read(kv::encode(&Command::Get { key: "b".into() })).unwrap();
    assert_eq!(
        kv::decode::<kv::Response>(&get_b).unwrap(),
        kv::Response::Get(Some("2".into()))
    );
    let get_a = state.read(kv::encode(&Command::Get { key: "a".into() })).unwrap();
    assert_eq!(
        kv::decode::<kv::Response>(&get_a).unwrap(),
        kv::Response::Get(Some("1".into()))
    );
}

#[test]
fn bitcask_roundtrip() {
    let path = temp_path("bc");
    {
        let mut eng = BitCask::new(path.clone()).unwrap();
        eng.set(b"a", b"1".to_vec()).unwrap();
        eng.set(b"b", b"2".to_vec()).unwrap();
        eng.flush().unwrap();
    }
    let mut eng = BitCask::new(path.clone()).unwrap();
    assert_eq!(eng.get(b"a").unwrap(), Some(b"1".to_vec()));
    eng.delete(b"a").unwrap();
    eng.flush().unwrap();
    drop(eng);
    let mut eng = BitCask::new(path).unwrap();
    assert_eq!(eng.get(b"a").unwrap(), None);
    assert_eq!(eng.get(b"b").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn single_node_write_session() {
    let (tx, rx) = channel::unbounded();
    let log = Log::new(Box::new(BitCask::new(temp_path("sess")).unwrap())).unwrap();
    let node = Node::new(
        1,
        HashSet::new(),
        log,
        SessionState::new(Kv::new()),
        tx,
        Options::default(),
    )
    .unwrap();
    let id = Uuid::new_v4();
    let cid = Uuid::new_v4();
    let cmd = kv::encode(&Command::Put { key: "x".into(), value: "y".into() });
    let term = node.term();
    let _node = node
        .step(Envelope {
            from: 1,
            to: 1,
            term,
            message: Message::ClientRequest {
                id,
                request: Request::WriteSession { client_id: cid, seq: 1, command: cmd },
            },
        })
        .unwrap();
    let mut got = false;
    while let Ok(msg) = rx.try_recv() {
        if let Message::ClientResponse { id: rid, response } = msg.message {
            if rid == id {
                assert!(matches!(response.unwrap(), Response::Write(_)));
                got = true;
            }
        }
    }
    assert!(got);
}
