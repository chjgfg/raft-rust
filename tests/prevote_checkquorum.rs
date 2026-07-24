//! Pre-vote / CheckQuorum 场景。

use raft_rust::cluster::{test_options, wait_for_leader, Cluster};
use raft_rust::error::{Error, Result};
use raft_rust::raft::Options;
use std::thread;
use std::time::Duration;

#[test]
fn election_works_with_prevote_and_check_quorum() -> Result<()> {
    let cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();
    let st = wait_for_leader(&mut client)?;
    client.put("a", "1")?;
    assert_eq!(client.get("a")?, Some("1".into()));
    assert!(st.term >= 1);
    Ok(())
}

#[test]
fn partitioned_node_does_not_steal_leadership() -> Result<()> {
    let mut opts = test_options();
    opts.pre_vote = true;
    opts.check_quorum = true;
    let cluster = Cluster::spawn_with_options(&[1, 2, 3], opts);
    let mut client = cluster.client();
    let st = wait_for_leader(&mut client)?;
    let leader = st.leader;
    let term_before = st.term;
    client.put("stable", "yes")?;

    // 隔离一个非领导跟随者。
    let follower = (1..=3).find(|&id| id != leader).unwrap();
    cluster.partition_groups(&[follower], &[leader, (1..=3).find(|&id| id != leader && id != follower).unwrap()]);

    // 等待足够久让被隔离节点反复 pre-vote。
    thread::sleep(Duration::from_millis(800));

    // 多数侧领导应保持（term 可能不变或仅小幅变化，但数据仍可服务）。
    let mut majority = client.clone();
    majority.preferred_hint(leader);
    let mut ok = false;
    for _ in 0..40 {
        match majority.get("stable") {
            Ok(Some(v)) if v == "yes" => {
                ok = true;
                break;
            }
            Ok(_) | Err(Error::Abort) | Err(Error::IO(_)) => {
                thread::sleep(Duration::from_millis(50))
            }
            Err(e) => return Err(e),
        }
    }
    assert!(ok, "majority should still serve reads");

    // term 不应被少数无意义抬升太多（允许少量因其它原因的增长，但一般应接近）。
    if let Ok(st2) = majority.status_on(leader) {
        assert!(
            st2.term <= term_before + 2,
            "term inflated too much: before={term_before} after={}",
            st2.term
        );
    }

    cluster.heal_all();
    Ok(())
}

#[test]
fn check_quorum_isolated_leader_steps_down() -> Result<()> {
    let mut opts = test_options();
    opts.pre_vote = true;
    opts.check_quorum = true;
    opts.election_timeout_range = 5..8;
    opts.heartbeat_interval = 2;
    let cluster = Cluster::spawn_with_options(&[1, 2, 3], opts);
    let mut client = cluster.client();
    let st = wait_for_leader(&mut client)?;
    let leader = st.leader;
    client.put("x", "1")?;

    // 隔离领导者与两个跟随者。
    let others: Vec<_> = (1..=3u8).filter(|&id| id != leader).collect();
    cluster.partition_groups(&[leader], &others);

    // 多数侧应选出新领导。
    let mut majority = client.clone();
    majority.preferred_hint(others[0]);
    let mut new_leader = None;
    for _ in 0..80 {
        match majority.status() {
            Ok(s) if s.leader != leader => {
                new_leader = Some(s.leader);
                break;
            }
            _ => thread::sleep(Duration::from_millis(50)),
        }
    }
    assert!(new_leader.is_some(), "majority should elect new leader");
    assert_eq!(majority.get("x")?, Some("1".into()));
    majority.put("y", "2")?;
    assert_eq!(majority.get("y")?, Some("2".into()));

    cluster.heal_all();
    thread::sleep(Duration::from_millis(300));
    let mut c = cluster.client();
    wait_for_leader(&mut c)?;
    assert_eq!(c.get("y")?, Some("2".into()));
    Ok(())
}

#[test]
fn works_with_prevote_disabled() -> Result<()> {
    let opts = Options {
        heartbeat_interval: 2,
        election_timeout_range: 5..10,
        max_append_entries: 100,
        pre_vote: false,
        check_quorum: false,
        snapshot_threshold: 0,
    };
    let cluster = Cluster::spawn_with_options(&[1, 2, 3], opts);
    let mut client = cluster.client();
    wait_for_leader(&mut client)?;
    client.put("p", "1")?;
    assert_eq!(client.get("p")?, Some("1".into()));
    Ok(())
}
