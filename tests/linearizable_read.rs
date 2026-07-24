//! 写后读线性一致（允许 Abort 重试）。

use raft_rust::cluster::{wait_for_leader, Cluster};
use raft_rust::error::Result;

#[test]
fn write_then_read_sees_value() -> Result<()> {
    let cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();
    wait_for_leader(&mut client)?;
    for i in 0..20 {
        let k = format!("k{i}");
        let v = format!("v{i}");
        client.put(&k, &v)?;
        assert_eq!(client.get(&k)?, Some(v));
    }
    Ok(())
}

#[test]
fn read_after_leader_change() -> Result<()> {
    let mut cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();
    let st = wait_for_leader(&mut client)?;
    client.put("stable", "1")?;
    cluster.stop(st.leader);

    let mut seen = None;
    for _ in 0..80 {
        match client.get("stable") {
            Ok(v) => {
                seen = Some(v);
                break;
            }
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(50)),
        }
    }
    assert_eq!(seen, Some(Some("1".into())));
    Ok(())
}
