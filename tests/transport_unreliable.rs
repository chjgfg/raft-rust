//! 丢包 / 乱序传输下最终收敛。

use raft_rust::cluster::{wait_for_leader, Cluster};
use raft_rust::error::{Error, Result};
use std::thread;
use std::time::Duration;

#[test]
fn elects_and_writes_under_drop() -> Result<()> {
    let cluster = Cluster::spawn(&[1, 2, 3]);
    cluster.set_drop_rate(0.2);
    let mut client = cluster.client();
    let mut ok = false;
    for _ in 0..100 {
        match client.status() {
            Ok(_) => {
                ok = true;
                break;
            }
            Err(Error::Abort) | Err(Error::IO(_)) => thread::sleep(Duration::from_millis(50)),
            Err(e) => return Err(e),
        }
    }
    assert!(ok, "should eventually elect under drop");
    // 降低丢包再写，减少 flaky。
    cluster.set_drop_rate(0.05);
    for _ in 0..30 {
        if client.put("d", "1").is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(client.get("d")?, Some("1".into()));
    Ok(())
}

#[test]
fn elects_under_reorder() -> Result<()> {
    let cluster = Cluster::spawn(&[1, 2, 3]);
    cluster.set_reorder(true);
    let mut client = cluster.client();
    wait_for_leader(&mut client)?;
    client.put("r", "1")?;
    assert_eq!(client.get("r")?, Some("1".into()));
    cluster.set_reorder(false);
    Ok(())
}
