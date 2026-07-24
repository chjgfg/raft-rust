//! 联合共识成员变更。

use std::collections::HashSet;
use std::thread;
use std::time::Duration;

use raft_rust::cluster::{wait_for_leader, Cluster};
use raft_rust::error::{Error, Result};

#[test]
fn add_voter_joint() -> Result<()> {
    let mut cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();
    wait_for_leader(&mut client)?;
    client.put("before", "1")?;

    // 拉起节点 4（空日志），再变更成员。
    cluster.start(4);
    let voters: HashSet<u8> = [1, 2, 3, 4].into_iter().collect();
    let idx = client.change_membership(voters.clone())?;
    assert!(idx >= 1);

    // 等待 simple 配置落地。
    for _ in 0..40 {
        if let Ok(st) = client.status() {
            if st.voters.len() == 4 {
                break;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    let st = client.status()?;
    assert_eq!(st.voters.len(), 4, "voters={:?}", st.voters);

    client.put("after", "2")?;
    assert_eq!(client.get("before")?, Some("1".into()));
    assert_eq!(client.get("after")?, Some("2".into()));
    Ok(())
}

#[test]
fn remove_voter_joint() -> Result<()> {
    let cluster = Cluster::spawn(&[1, 2, 3, 4]);
    let mut client = cluster.client();
    wait_for_leader(&mut client)?;
    client.put("k", "v")?;

    // 移除非领导节点。
    let st = client.status()?;
    let remove = if st.leader == 4 { 3 } else { 4 };
    let mut voters: HashSet<u8> = [1, 2, 3, 4].into_iter().collect();
    voters.remove(&remove);

    let idx = client.change_membership(voters.clone())?;
    assert!(idx >= 1);

    for _ in 0..40 {
        if let Ok(st) = client.status() {
            if st.voters.len() == 3 && !st.voters.contains(&remove) {
                break;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    let st = client.status()?;
    assert_eq!(st.voters.len(), 3);
    assert!(!st.voters.contains(&remove));
    assert_eq!(client.get("k")?, Some("v".into()));
    client.put("k2", "v2")?;
    assert_eq!(client.get("k2")?, Some("v2".into()));
    Ok(())
}

/// 移除当前领导者：Simple 配置提交后旧领导 step down，集群仍可读写。
#[test]
fn remove_leader_steps_down() -> Result<()> {
    let mut cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();
    let st = wait_for_leader(&mut client)?;
    let old_leader = st.leader;
    client.put("before", "1")?;

    let voters: HashSet<u8> = (1..=3u8).filter(|&id| id != old_leader).collect();
    client.change_membership(voters.clone())?;

    // 等待新主（不能是旧领导）。
    let mut new_leader = None;
    for _ in 0..80 {
        match client.status() {
            Ok(s) if s.leader != old_leader && s.voters.len() == 2 => {
                new_leader = Some(s.leader);
                break;
            }
            _ => thread::sleep(Duration::from_millis(50)),
        }
    }
    let new_leader = new_leader.expect("new leader after removing old");
    assert_ne!(new_leader, old_leader);
    assert_eq!(client.get("before")?, Some("1".into()));
    client.put("after", "2")?;
    assert_eq!(client.get("after")?, Some("2".into()));
    Ok(())
}

#[test]
fn membership_change_preserves_data() -> Result<()> {
    let mut cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();
    wait_for_leader(&mut client)?;
    for i in 0..5 {
        client.put(&format!("k{i}"), &format!("v{i}"))?;
    }
    cluster.start(4);
    client.change_membership([1, 2, 3, 4].into_iter().collect())?;
    thread::sleep(Duration::from_millis(200));
    for i in 0..5 {
        assert_eq!(client.get(&format!("k{i}"))?, Some(format!("v{i}")));
    }
    Ok(())
}

#[test]
fn overlapping_change_rejected() -> Result<()> {
    let mut cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();
    wait_for_leader(&mut client)?;
    cluster.start(4);
    cluster.start(5);

    // 第一次变更。
    let r1 = client.change_membership([1, 2, 3, 4].into_iter().collect());
    assert!(r1.is_ok(), "first change should succeed: {r1:?}");

    // 立即第二次（可能仍在 joint/pending）——允许成功（若第一次已完成）或 InvalidInput/Abort。
    let r2 = client.change_membership([1, 2, 3, 4, 5].into_iter().collect());
    match r2 {
        Ok(_) => {} // 第一次已完成则允许
        Err(Error::InvalidInput(_)) | Err(Error::Abort) => {}
        Err(e) => return Err(e),
    }
    Ok(())
}
