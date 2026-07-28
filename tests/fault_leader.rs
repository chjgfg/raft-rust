//! 领导崩溃与重新选举。

// 集群控制与等领导者
use raft_rust::cluster::{wait_for_leader, Cluster};
// 引入依赖
use raft_rust::error::Result;
// 轮询退避
use std::thread;
// 引入依赖
use std::time::Duration;

// 验证领导者宕机后集群能重选，已提交数据不丢，新主可继续写
#[test]
// 定义函数
fn leader_crash_reelects_and_keeps_data() -> Result<()> {
    // mut：需要 stop 旧领导者
    let mut cluster = Cluster::spawn(&[1, 2, 3]);
    // 可变绑定
    let mut client = cluster.client();
    // 记录崩溃前的领导者与任期相关状态
    let st = wait_for_leader(&mut client)?;
    // 崩溃前写入应已多数提交
    client.put("k", "v1")?;
    // 断言业务不变量
    assert_eq!(client.get("k")?, Some("v1".into()));

    // 故障注入：停掉当前领导者进程内节点
    let old_leader = st.leader;
    // 业务逻辑步骤
    cluster.stop(old_leader);

    // 向剩余节点重试直至新主选出。
    let mut new_status = None;
    // 最多约 4s 等待新任期领导者
    for _ in 0..80 {
        // 按结果分支处理
        match client.status() {
            // 出现非旧领导者的 Status 即视为重选成功
            Ok(s) if s.leader != old_leader => {
                // 业务逻辑步骤
                new_status = Some(s);
                // 业务逻辑步骤
                break;
            // 结束当前作用域
            }
            // 仍无主或连不上则退避
            _ => thread::sleep(Duration::from_millis(50)),
        // 结束当前作用域
        }
    // 结束当前作用域
    }
    // 必须选出新主，否则故障恢复失败
    let st2 = new_status.expect("new leader should be elected");
    // 新主 id 不能仍是已 stop 的节点
    assert_ne!(st2.leader, old_leader);
    // 已提交的 v1 在新主下仍可读（持久化 + 复制）
    assert_eq!(client.get("k")?, Some("v1".into()));

    // 新主应能继续接受写并覆盖
    client.put("k", "v2")?;
    // 断言业务不变量
    assert_eq!(client.get("k")?, Some("v2".into()));
    // 成功返回
    Ok(())
// 结束当前作用域
}
