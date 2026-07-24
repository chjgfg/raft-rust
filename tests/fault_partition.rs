//! 网络分区：少数不能提交，多数可以；恢复后一致。

use raft_rust::cluster::{wait_for_leader, Cluster};
use raft_rust::error::{Error, Result};
use std::thread;
use std::time::Duration;

#[test]
fn minority_partition_cannot_commit() -> Result<()> {
    let cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();
    let st = wait_for_leader(&mut client)?;
    client.put("before", "ok")?;

    // 把节点 1 从 2、3 隔开。
    cluster.partition_groups(&[1], &[2, 3]);

    // 多数侧（2,3）仍应能写（若领导在多数侧）或重选后能写。
    let mut majority_client = client.clone();
    // 偏好打到 2/3
    majority_client.preferred_hint(2);

    let mut wrote = false;
    for _ in 0..60 {
        match majority_client.put("maj", "1") {
            Ok(_) => {
                wrote = true;
                break;
            }
            Err(Error::Abort) | Err(Error::IO(_)) => thread::sleep(Duration::from_millis(50)),
            Err(e) => return Err(e),
        }
    }
    assert!(wrote, "majority should commit");

    // 少数侧单独写应失败/超时。
    let mut minority = client.clone();
    minority.preferred_hint(1);
    let r = minority.put_on(1, "min", "x");
    assert!(r.is_err(), "minority write should fail, got {r:?}");

    cluster.heal_all();
    thread::sleep(Duration::from_millis(300));

    // 恢复后能读到多数侧的值，读不到少数未提交的值。
    let mut c = cluster.client();
    wait_for_leader(&mut c)?;
    assert_eq!(c.get("before")?, Some("ok".into()));
    assert_eq!(c.get("maj")?, Some("1".into()));
    assert_eq!(c.get("min")?, None);
    let _ = st;
    Ok(())
}
