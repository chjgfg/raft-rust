//! 使用共享 cluster 模块的 3 节点演示（薄封装）。

use raft_rust::cluster::{wait_for_leader, Cluster};
use raft_rust::error::Result;

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    println!("Starting 3-node Raft cluster (example)...");
    let cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();

    let status = wait_for_leader(&mut client)?;
    println!(
        "Leader={} term={} commit={} voters={:?}",
        status.leader, status.term, status.commit_index, status.voters
    );

    for (k, v) in [("a", "apple"), ("b", "banana"), ("c", "cherry")] {
        let index = client.put(k, v)?;
        println!("  put {k}={v} @ {index}");
    }
    for k in ["a", "b", "c", "missing"] {
        println!("  get {k} => {:?}", client.get(k)?);
    }
    println!("scan => {:?}", client.scan()?);
    Ok(())
}
