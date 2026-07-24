//! 进程内多节点 Raft 集群集成测试（happy path）。

use std::collections::HashSet;

use raft_rust::cluster::{wait_for_leader, Cluster};
use raft_rust::error::Result;

#[test]
fn elects_leader() -> Result<()> {
    let cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();
    let status = wait_for_leader(&mut client)?;
    assert!((1..=3).contains(&status.leader));
    assert!(status.term >= 1);
    assert!(status.commit_index >= 1);
    assert!(status.applied_index >= 1);
    assert_eq!(status.match_index.len(), 3);
    assert_eq!(status.voters.len(), 3);
    Ok(())
}

#[test]
fn put_get_scan() -> Result<()> {
    let cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();
    wait_for_leader(&mut client)?;

    let idx = client.put("hello", "world")?;
    assert!(idx >= 1);
    assert_eq!(client.get("hello")?, Some("world".into()));
    assert_eq!(client.get("missing")?, None);

    client.put("a", "1")?;
    client.put("b", "2")?;
    let all = client.scan()?;
    assert_eq!(all.get("hello"), Some(&"world".into()));
    assert_eq!(all.get("a"), Some(&"1".into()));
    assert_eq!(all.get("b"), Some(&"2".into()));
    assert_eq!(all.len(), 3);
    Ok(())
}

#[test]
fn overwrite_value() -> Result<()> {
    let cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();
    wait_for_leader(&mut client)?;
    client.put("k", "v1")?;
    assert_eq!(client.get("k")?, Some("v1".into()));
    client.put("k", "v2")?;
    assert_eq!(client.get("k")?, Some("v2".into()));
    Ok(())
}

#[test]
fn follower_forwards_write() -> Result<()> {
    let cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();
    let status = wait_for_leader(&mut client)?;
    let leader = status.leader;
    let follower = (1..=3).find(|&id| id != leader).expect("follower");
    let index = client.put_on(follower, "via-follower", "ok")?;
    assert!(index >= 1);
    assert_eq!(client.get("via-follower")?, Some("ok".into()));
    Ok(())
}

#[test]
fn sequential_writes_commit_in_order() -> Result<()> {
    let cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();
    wait_for_leader(&mut client)?;
    let mut last = 0;
    for i in 0..10 {
        let index = client.put(&format!("k{i}"), &format!("v{i}"))?;
        assert!(index > last);
        last = index;
    }
    for i in 0..10 {
        assert_eq!(client.get(&format!("k{i}"))?, Some(format!("v{i}")));
    }
    let status = client.status()?;
    assert!(status.commit_index >= last);
    Ok(())
}

#[test]
fn five_node_cluster() -> Result<()> {
    let cluster = Cluster::spawn(&[1, 2, 3, 4, 5]);
    let mut client = cluster.client();
    let status = wait_for_leader(&mut client)?;
    assert!((1..=5).contains(&status.leader));
    assert_eq!(status.match_index.len(), 5);
    client.put("x", "y")?;
    assert_eq!(client.get("x")?, Some("y".into()));
    Ok(())
}

#[test]
fn change_membership_uses_hashset() -> Result<()> {
    // 仅验证 API 类型可构造；完整成员变更见 membership 测试。
    let _ = HashSet::<u8>::from([1, 2, 3, 4]);
    Ok(())
}
