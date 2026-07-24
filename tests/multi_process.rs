//! 真多进程联调：拉起多个 `raft-node` 子进程，经 TCP 读写。
//!
//! 需要已编译的二进制：`cargo build --bin raft-node`。

use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use raft_rust::error::{Error, Result};
use raft_rust::net::run_client_request;
use raft_rust::raft::kv::{self, Command as KvCommand};
use raft_rust::raft::{Request, Response};

fn unique_dir() -> PathBuf {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "raft-mp-{}-{}-{}",
        std::process::id(),
        seq,
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    ))
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn bin_path(name: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target");
    let profile = if cfg!(debug_assertions) { "debug" } else { "release" };
    path.push(profile);
    if cfg!(windows) {
        path.push(format!("{name}.exe"));
    } else {
        path.push(name);
    }
    path
}

struct NodeProc {
    child: Child,
    addr: SocketAddr,
    _dir: PathBuf,
}

impl Drop for NodeProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn write_node_config(dir: &PathBuf, id: u8, port: u16, peers: &[(u8, u16)]) -> PathBuf {
    fs::create_dir_all(dir).unwrap();
    let data_dir = dir.join(format!("data{id}"));
    fs::create_dir_all(&data_dir).unwrap();
    let mut body = String::new();
    body.push_str(&format!("node:\n  id: {id}\n  listen: \"127.0.0.1:{port}\"\n"));
    let data = data_dir.display().to_string().replace('\\', "/");
    body.push_str(&format!("  data_dir: \"{data}\"\n\n"));
    if peers.is_empty() {
        body.push_str("peers: []\n\n");
    } else {
        body.push_str("peers:\n");
        for (pid, pport) in peers {
            body.push_str(&format!(
                "  - {{ id: {pid}, addr: \"127.0.0.1:{pport}\" }}\n"
            ));
        }
        body.push('\n');
    }
    body.push_str(
        "options:\n  heartbeat_interval: 2\n  election_timeout_min: 5\n  election_timeout_max: 10\n  max_append_entries: 100\n  pre_vote: true\n  check_quorum: true\n  snapshot_threshold: 0\n",
    );
    let cfg_path = dir.join(format!("node{id}.yaml"));
    fs::write(&cfg_path, body).unwrap();
    cfg_path
}

fn spawn_node(cfg: &PathBuf) -> Child {
    let bin = bin_path("raft-node");
    assert!(
        bin.exists(),
        "raft-node binary missing at {bin:?}; run cargo build --bin raft-node first"
    );
    ProcessCommand::new(bin)
        .arg("--config")
        .arg(cfg)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn raft-node")
}

fn wait_ready(peers: &[SocketAddr], timeout: Duration) -> Result<Response> {
    let deadline = Instant::now() + timeout;
    let mut last = Error::Abort;
    while Instant::now() < deadline {
        for addr in peers {
            match run_client_request(*addr, Request::Status, Duration::from_millis(500)) {
                Ok(r) => return Ok(r),
                Err(e) => last = e,
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(last)
}

fn request(peers: &[SocketAddr], req: Request) -> Result<Response> {
    let mut last = Error::Abort;
    for _ in 0..40 {
        for addr in peers {
            match run_client_request(*addr, req.clone(), Duration::from_secs(2)) {
                Ok(r) => return Ok(r),
                Err(Error::Abort) => last = Error::Abort,
                Err(Error::IO(e)) => last = Error::IO(e),
                Err(e) => return Err(e),
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(last)
}

#[test]
fn multi_process_single_node_put_get() {
    let dir = unique_dir();
    let port = free_port();
    let cfg = write_node_config(&dir, 1, port, &[]);
    let child = spawn_node(&cfg);
    let mut proc = NodeProc {
        child,
        addr: format!("127.0.0.1:{port}").parse().unwrap(),
        _dir: dir,
    };

    let peers = [proc.addr];
    let st = wait_ready(&peers, Duration::from_secs(10)).expect("status");
    match st {
        Response::Status(s) => assert_eq!(s.leader, 1),
        other => panic!("{other:?}"),
    }

    let put = Request::Write(kv::encode(&KvCommand::Put {
        key: "mp".into(),
        value: "ok".into(),
    }));
    assert!(matches!(request(&peers, put).expect("put"), Response::Write(_)));

    let get = Request::Read(kv::encode(&KvCommand::Get { key: "mp".into() }));
    match request(&peers, get).expect("get") {
        Response::Read(bytes) => {
            let r: kv::Response = kv::decode(&bytes).unwrap();
            assert_eq!(r, kv::Response::Get(Some("ok".into())));
        }
        other => panic!("{other:?}"),
    }

    let _ = proc.child.kill();
}

#[test]
fn multi_process_three_nodes_elect_and_write() {
    let dir = unique_dir();
    let p1 = free_port();
    let p2 = free_port();
    let p3 = free_port();

    let c1 = write_node_config(&dir, 1, p1, &[(2, p2), (3, p3)]);
    let c2 = write_node_config(&dir, 2, p2, &[(1, p1), (3, p3)]);
    let c3 = write_node_config(&dir, 3, p3, &[(1, p1), (2, p2)]);

    let mut nodes = vec![
        NodeProc {
            child: spawn_node(&c1),
            addr: format!("127.0.0.1:{p1}").parse().unwrap(),
            _dir: dir.clone(),
        },
        NodeProc {
            child: spawn_node(&c2),
            addr: format!("127.0.0.1:{p2}").parse().unwrap(),
            _dir: dir.clone(),
        },
        NodeProc {
            child: spawn_node(&c3),
            addr: format!("127.0.0.1:{p3}").parse().unwrap(),
            _dir: dir,
        },
    ];

    let peers: Vec<SocketAddr> = nodes.iter().map(|n| n.addr).collect();
    let st = wait_ready(&peers, Duration::from_secs(15)).expect("cluster ready");
    match st {
        Response::Status(s) => {
            assert!((1..=3).contains(&s.leader));
            assert_eq!(s.voters.len(), 3);
        }
        other => panic!("{other:?}"),
    }

    let put = Request::Write(kv::encode(&KvCommand::Put {
        key: "cluster".into(),
        value: "3nodes".into(),
    }));
    request(&peers, put).expect("put");

    let get = Request::Read(kv::encode(&KvCommand::Get { key: "cluster".into() }));
    match request(&peers, get).expect("get") {
        Response::Read(bytes) => {
            let r: kv::Response = kv::decode(&bytes).unwrap();
            assert_eq!(r, kv::Response::Get(Some("3nodes".into())));
        }
        other => panic!("{other:?}"),
    }

    for n in &mut nodes {
        let _ = n.child.kill();
    }
}
