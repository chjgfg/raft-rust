//! 多客户端并发写。

use std::sync::{Arc, Barrier};
use std::thread;

use raft_rust::cluster::{wait_for_leader, Cluster};
use raft_rust::error::Result;

#[test]
fn concurrent_puts() -> Result<()> {
    let cluster = Cluster::spawn(&[1, 2, 3]);
    let mut client = cluster.client();
    wait_for_leader(&mut client)?;

    let barrier = Arc::new(Barrier::new(4));
    let mut handles = vec![];
    for t in 0..4 {
        let mut c = cluster.client();
        let b = barrier.clone();
        handles.push(thread::spawn(move || {
            b.wait();
            for i in 0..10 {
                let key = format!("t{t}-k{i}");
                let val = format!("v{i}");
                // 允许短暂 Abort 重试。
                for _ in 0..30 {
                    if c.put(&key, &val).is_ok() {
                        break;
                    }
                    thread::sleep(std::time::Duration::from_millis(20));
                }
            }
        }));
    }
    for h in handles {
        h.join().expect("thread panicked");
    }

    let mut c = cluster.client();
    for t in 0..4 {
        for i in 0..10 {
            let key = format!("t{t}-k{i}");
            assert_eq!(c.get(&key)?, Some(format!("v{i}")), "missing {key}");
        }
    }
    Ok(())
}
