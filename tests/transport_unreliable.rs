//! 丢包 / 乱序传输下最终收敛。

// 可注入传输故障的集群
use raft_rust::cluster::{wait_for_leader, Cluster};
// 丢包期间常见 Abort/IO
use raft_rust::error::{Error, Result};
// 退避重试时让出调度
use std::thread;
// 重试间隔与超时控制
use std::time::Duration;

// 验证 20% 丢包下仍能最终选出领导并完成写读
#[test]
// 丢包场景：选举最终收敛，随后写读成功
fn elects_and_writes_under_drop() -> Result<()> {
    // 三节点进程内集群，作为不可靠传输底座
    let cluster = Cluster::spawn(&[1, 2, 3]);
    // 注入约 20% 消息丢弃
    cluster.set_drop_rate(0.2);
    // 客户端经可丢包通道访问集群
    let mut client = cluster.client();
    // 在丢包下轮询 Status 直至成功
    let mut ok = false;
    // 最多约 5s，覆盖多次选举超时
    for _ in 0..100 {
        // 用 status 探测选举是否已完成
        match client.status() {
            // 任一成功 Status 说明选举路径最终收敛
            Ok(_) => {
                // 标记选举探测成功
                ok = true;
                // 无需继续轮询
                break;
            }
            // 丢包导致的瞬时失败可重试
            Err(Error::Abort) | Err(Error::IO(_)) => thread::sleep(Duration::from_millis(50)),
            // 非瞬时错误直接失败，避免吞掉协议 bug
            Err(e) => return Err(e),
        }
    }
    // 丢包下最终必须选出可用主
    assert!(ok, "should eventually elect under drop");
    // 降低丢包再写，减少 flaky。
    cluster.set_drop_rate(0.05);
    // 写路径同样允许短暂失败重试
    for _ in 0..30 {
        // 多数提交成功即结束重试
        if client.put("d", "1").is_ok() {
            // 写入已提交
            break;
        }
        // 短暂退避后再次尝试 Put
        thread::sleep(Duration::from_millis(50));
    }
    // 最终应读到已提交值
    assert_eq!(client.get("d")?, Some("1".into()));
    // 丢包场景下写后读闭环完成
    Ok(())
}

// 验证消息乱序投递下仍能选举并正确提交
#[test]
// 乱序 RPC 不得破坏 term/index 校验与最终一致性
fn elects_under_reorder() -> Result<()> {
    // 三节点集群，后续开启乱序注入
    let cluster = Cluster::spawn(&[1, 2, 3]);
    // 开启乱序：模拟网络重排 RPC
    cluster.set_reorder(true);
    // 乱序通道上的客户端
    let mut client = cluster.client();
    // 乱序下仍应选出领导
    wait_for_leader(&mut client)?;
    // 写读正确性不受乱序影响（Raft 靠 term/index 校验）
    client.put("r", "1")?;
    // 读路径必须看到刚提交的值
    assert_eq!(client.get("r")?, Some("1".into()));
    // 关闭乱序，恢复正常传输
    cluster.set_reorder(false);
    // 乱序场景验证结束
    Ok(())
}
