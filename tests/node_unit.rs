//! 确定性、无线程的节点驱动。

use std::collections::{HashMap, HashSet, VecDeque};

use crossbeam::channel;
use raft_rust::raft::kv::Kv;
use raft_rust::raft::{Envelope, Log, Node, NodeID, Options};
use raft_rust::storage::BitCask;

struct Harness {
    nodes: HashMap<NodeID, Node>,
    rxs: HashMap<NodeID, channel::Receiver<Envelope>>,
    mailboxes: HashMap<NodeID, VecDeque<Envelope>>,
}

impl Harness {
    fn new(ids: &[NodeID], opts: Options) -> Self {
        let mut nodes = HashMap::new();
        let mut rxs = HashMap::new();
        let mut mailboxes = HashMap::new();
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        for &id in ids {
            let peers: HashSet<NodeID> = ids.iter().copied().filter(|&p| p != id).collect();
            let (tx, rx) = channel::unbounded();
            let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "raft-nu-{}-{}-{}-{}.log",
                std::process::id(),
                id,
                seq,
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
            ));
            let log = Log::new(Box::new(BitCask::new(path).unwrap())).unwrap();
            let node = Node::new(id, peers, log, Kv::new(), tx, opts.clone()).unwrap();
            nodes.insert(id, node);
            rxs.insert(id, rx);
            mailboxes.insert(id, VecDeque::new());
        }
        Self { nodes, rxs, mailboxes }
    }

    fn drain_outboxes(&mut self) {
        for (&id, rx) in &self.rxs {
            while let Ok(msg) = rx.try_recv() {
                if msg.to == id {
                    continue;
                }
                self.mailboxes.get_mut(&msg.to).unwrap().push_back(msg);
            }
        }
    }

    fn deliver_one(&mut self) -> bool {
        for id in self.mailboxes.keys().copied().collect::<Vec<_>>() {
            if let Some(msg) = self.mailboxes.get_mut(&id).unwrap().pop_front() {
                let node = self.nodes.remove(&id).unwrap();
                let node = match node.step(msg) {
                    Ok(n) => n,
                    Err(e) => panic!("step error on {id}: {e}"),
                };
                self.nodes.insert(id, node);
                self.drain_outboxes();
                return true;
            }
        }
        false
    }

    fn deliver_all(&mut self) {
        while self.deliver_one() {}
    }

    fn tick(&mut self, id: NodeID) {
        let node = self.nodes.remove(&id).unwrap();
        let node = node.tick().unwrap();
        self.nodes.insert(id, node);
        self.drain_outboxes();
        self.deliver_all();
    }

    fn tick_all(&mut self) {
        for id in self.nodes.keys().copied().collect::<Vec<_>>() {
            let node = self.nodes.remove(&id).unwrap();
            let node = node.tick().unwrap();
            self.nodes.insert(id, node);
        }
        self.drain_outboxes();
        self.deliver_all();
    }

    fn leaders(&self) -> Vec<NodeID> {
        self.nodes
            .iter()
            .filter_map(|(id, n)| match n {
                Node::Leader(_) => Some(*id),
                _ => None,
            })
            .collect()
    }

    fn role(&self, id: NodeID) -> &'static str {
        match &self.nodes[&id] {
            Node::Follower(_) => "follower",
            Node::Candidate(_) => "candidate",
            Node::Leader(_) => "leader",
        }
    }
}

fn opts(prevote: bool) -> Options {
    Options {
        heartbeat_interval: 2,
        // 宽范围，配合错开 tick
        election_timeout_range: 5..6, // 固定 5
        max_append_entries: 100,
        pre_vote: prevote,
        check_quorum: false,
        snapshot_threshold: 0,
    }
}

#[test]
fn staggered_election_without_prevote() {
    let mut h = Harness::new(&[1, 2, 3], opts(false));
    // 只让节点 1 超时并竞选；2/3 仍是 follower 会投票
    for _ in 0..5 {
        h.tick(1);
    }
    assert!(
        h.leaders().contains(&1),
        "node1 should be leader, roles: 1={} 2={} 3={} terms: {} {} {}",
        h.role(1), h.role(2), h.role(3),
        h.nodes[&1].term(), h.nodes[&2].term(), h.nodes[&3].term()
    );
}

#[test]
fn staggered_election_with_prevote() {
    let mut h = Harness::new(&[1, 2, 3], opts(true));
    for _ in 0..5 {
        h.tick(1);
    }
    // pre-vote + real election 可能需要多一轮 deliver；再补几 tick
    for _ in 0..5 {
        h.tick_all();
        if h.leaders().contains(&1) || !h.leaders().is_empty() {
            break;
        }
    }
    assert!(
        !h.leaders().is_empty(),
        "should elect a leader, roles: 1={} 2={} 3={} terms: {} {} {}",
        h.role(1), h.role(2), h.role(3),
        h.nodes[&1].term(), h.nodes[&2].term(), h.nodes[&3].term()
    );
}

#[test]
fn single_node_leader() {
    let opts = Options {
        pre_vote: true,
        check_quorum: true,
        snapshot_threshold: 0,
        ..Options::default()
    };
    let mut h = Harness::new(&[1], opts);
    assert_eq!(h.leaders(), vec![1]);
    h.tick(1);
    assert_eq!(h.leaders(), vec![1]);
}
