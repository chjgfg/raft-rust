//! Raft 集群 CLI 客户端。
//!
//! ```text
//! cargo run --bin raft-cli -- --peers 127.0.0.1:7001 put a apple
//! cargo run --bin raft-cli -- --peers 127.0.0.1:7001,127.0.0.1:7002 get a
//! ```
//!
//! Session：默认把 `client_id` 与单调 `seq` 存在当前目录 `.raft-cli-session`，
//! 保证写重试幂等。可用 `--client-id` 覆盖 id（仍使用文件中的 seq）。

use std::collections::HashSet;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use raft_rust::error::{Error, Result};
use raft_rust::net::run_client_request;
use raft_rust::raft::kv::{self, Command};
use raft_rust::raft::{NodeID, Request, Response};
use uuid::Uuid;

const SESSION_FILE: &str = ".raft-cli-session";

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut args: Vec<String> = env::args().skip(1).collect();
    let mut peers: Vec<SocketAddr> = Vec::new();
    let mut client_id_override: Option<Uuid> = None;

    let mut i = 0;
    while i < args.len() {
        if args[i] == "--peers" && i + 1 < args.len() {
            peers = parse_peers(&args[i + 1])?;
            args.drain(i..=i + 1);
            continue;
        }
        if args[i] == "--client-id" && i + 1 < args.len() {
            client_id_override = Some(
                Uuid::parse_str(&args[i + 1])
                    .map_err(|e| Error::InvalidInput(format!("bad client-id: {e}")))?,
            );
            args.drain(i..=i + 1);
            continue;
        }
        i += 1;
    }

    if peers.is_empty() {
        return Err(Error::InvalidInput(
            "usage: raft-cli --peers host:port[,host:port...] [--client-id UUID] \
             <put|get|scan|status|members> ..."
                .into(),
        ));
    }

    if args.is_empty() {
        return Err(Error::InvalidInput("missing command".into()));
    }

    let mut session = SessionStore::load(session_path());
    if let Some(id) = client_id_override {
        session.client_id = id;
    }

    match args[0].as_str() {
        "put" => {
            if args.len() < 3 {
                return Err(Error::InvalidInput("put <key> <value>".into()));
            }
            let seq = session.next_seq();
            session.save()?;
            let cmd = kv::encode(&Command::Put {
                key: args[1].clone(),
                value: args[2].clone(),
            });
            let req = Request::WriteSession {
                client_id: session.client_id,
                seq,
                command: cmd,
            };
            // 重试必须带同一 (client_id, seq)
            let resp = request_with_retry(&peers, req)?;
            match resp {
                Response::Write(bytes) => {
                    let r: kv::Response = kv::decode(&bytes)?;
                    println!("{r}");
                }
                other => println!("{other:?}"),
            }
        }
        "get" => {
            if args.len() < 2 {
                return Err(Error::InvalidInput("get <key>".into()));
            }
            let cmd = kv::encode(&Command::Get { key: args[1].clone() });
            let resp = request_with_retry(&peers, Request::Read(cmd))?;
            match resp {
                Response::Read(bytes) => {
                    let r: kv::Response = kv::decode(&bytes)?;
                    println!("{r}");
                }
                other => println!("{other:?}"),
            }
        }
        "scan" => {
            let cmd = kv::encode(&Command::Scan);
            let resp = request_with_retry(&peers, Request::Read(cmd))?;
            match resp {
                Response::Read(bytes) => {
                    let r: kv::Response = kv::decode(&bytes)?;
                    println!("{r}");
                }
                other => println!("{other:?}"),
            }
        }
        "status" => {
            let resp = request_with_retry(&peers, Request::Status)?;
            match resp {
                Response::Status(s) => {
                    println!(
                        "leader={} term={} commit={} applied={} voters={:?}",
                        s.leader, s.term, s.commit_index, s.applied_index, s.voters
                    );
                    println!("match_index={:?}", s.match_index);
                }
                other => println!("{other:?}"),
            }
        }
        "members" => {
            if args.len() < 2 {
                return Err(Error::InvalidInput("members <id,id,...>".into()));
            }
            let voters: HashSet<NodeID> = args[1]
                .split(',')
                .map(|s| {
                    s.trim()
                        .parse::<NodeID>()
                        .map_err(|e| Error::InvalidInput(format!("bad id: {e}")))
                })
                .collect::<Result<_>>()?;
            let resp = request_with_retry(&peers, Request::ChangeMembership { voters })?;
            match resp {
                Response::ChangeMembership { index } => println!("membership proposed @ {index}"),
                other => println!("{other:?}"),
            }
        }
        other => return Err(Error::InvalidInput(format!("unknown command {other}"))),
    }
    Ok(())
}

fn parse_peers(s: &str) -> Result<Vec<SocketAddr>> {
    s.split(',')
        .map(|p| {
            p.trim()
                .parse()
                .map_err(|e| Error::InvalidInput(format!("bad peer addr {p}: {e}")))
        })
        .collect()
}

fn request_with_retry(peers: &[SocketAddr], request: Request) -> Result<Response> {
    let mut last = Error::Abort;
    for _ in 0..40 {
        for addr in peers {
            match run_client_request(*addr, request.clone(), Duration::from_secs(2)) {
                Ok(r) => return Ok(r),
                Err(Error::Abort) => last = Error::Abort,
                Err(Error::IO(e)) => last = Error::IO(e),
                Err(e) => return Err(e),
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(last)
}

fn session_path() -> PathBuf {
    env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join(SESSION_FILE)
}

/// 持久化 client_id + 已用最大 seq。
struct SessionStore {
    path: PathBuf,
    client_id: Uuid,
    last_seq: u64,
}

impl SessionStore {
    fn load(path: PathBuf) -> Self {
        if let Ok(text) = fs::read_to_string(&path) {
            let mut client_id = Uuid::new_v4();
            let mut last_seq = 0u64;
            for line in text.lines() {
                if let Some(v) = line.strip_prefix("client_id=") {
                    if let Ok(id) = Uuid::parse_str(v.trim()) {
                        client_id = id;
                    }
                } else if let Some(v) = line.strip_prefix("last_seq=") {
                    if let Ok(n) = v.trim().parse() {
                        last_seq = n;
                    }
                }
            }
            return Self { path, client_id, last_seq };
        }
        Self { path, client_id: Uuid::new_v4(), last_seq: 0 }
    }

    fn next_seq(&mut self) -> u64 {
        self.last_seq = self.last_seq.saturating_add(1);
        self.last_seq
    }

    fn save(&self) -> Result<()> {
        let body = format!("client_id={}\nlast_seq={}\n", self.client_id, self.last_seq);
        fs::write(&self.path, body).map_err(|e| Error::IO(format!("write session: {e}")))?;
        Ok(())
    }
}
