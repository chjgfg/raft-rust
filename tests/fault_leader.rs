//! 领导崩溃与重新选举。

use raft_rust::cluster::{wait_for_leader, Cluster};
use raft_rust::error::Result;
use std::thread;
use std::time::Duration;

#[test]
fn leader_crash_reelects_and_keeps_data() -> Result<()> {
    let mut cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();
    let st = wait_for_leader(&mut client)?;
    client.put("k", "v1")?;
    assert_eq!(client.get("k")?, Some("v1".into()));

    let old_leader = st.leader;
    cluster.stop(old_leader);

    // 向剩余节点重试直至新主选出。
    let mut new_status = None;
    for _ in 0..80 {
        match client.status() {
            Ok(s) if s.leader != old_leader => {
                new_status = Some(s);
                break;
            }
            _ => thread::sleep(Duration::from_millis(50)),
        }
    }
    let st2 = new_status.expect("new leader should be elected");
    assert_ne!(st2.leader, old_leader);
    assert_eq!(client.get("k")?, Some("v1".into()));

    client.put("k", "v2")?;
    assert_eq!(client.get("k")?, Some("v2".into()));
    Ok(())
}
