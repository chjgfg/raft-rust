//! 网络分区：少数不能提交，多数可以；恢复后一致。

// 集群与等领导者
use raft_rust::cluster::{wait_for_leader, Cluster};
// Abort/IO 在分区期间属预期瞬时错误
use raft_rust::error::{Error, Result};
// 引入依赖
use std::thread;
// 引入依赖
use std::time::Duration;

// 验证脑裂场景：多数分区可提交，少数分区写失败；愈合后只保留多数侧数据
#[test]
// 定义函数
fn minority_partition_cannot_commit() -> Result<()> {
    // 绑定中间结果
    let cluster = Cluster::spawn(&[1, 2, 3]);
    // 可变绑定
    let mut client = cluster.client();
    // 记录分区前领导者（末尾用 _ 抑制未使用告警）
    let st = wait_for_leader(&mut client)?;
    // 分区前基线数据，愈合后必须仍在
    client.put("before", "ok")?;

    // 把节点 1 从 2、3 隔开。
    cluster.partition_groups(&[1], &[2, 3]);

    // 多数侧（2,3）仍应能写（若领导在多数侧）或重选后能写。
    let mut majority_client = client.clone();
    // 偏好打到 2/3
    majority_client.preferred_hint(2);

    // 轮询直至多数侧 Put 成功
    let mut wrote = false;
    // 循环推进
    for _ in 0..60 {
        // 按结果分支处理
        match majority_client.put("maj", "1") {
            // 多数拥有法定人数，应能提交
            Ok(_) => {
                // 业务逻辑步骤
                wrote = true;
                // 业务逻辑步骤
                break;
            // 结束当前作用域
            }
            // 重选或网络抖动时允许 Abort/IO 重试
            Err(Error::Abort) | Err(Error::IO(_)) => thread::sleep(Duration::from_millis(50)),
            // 其它错误视为测试失败
            Err(e) => return Err(e),
        // 结束当前作用域
        }
    // 结束当前作用域
    }
    // 断言业务不变量
    assert!(wrote, "majority should commit");

    // 少数侧单独写应失败/超时。
    let mut minority = client.clone();
    // 强制走被隔离的节点 1
    minority.preferred_hint(1);
    // 直接打到少数节点，无法形成多数则不得提交
    let r = minority.put_on(1, "min", "x");
    // 断言业务不变量
    assert!(r.is_err(), "minority write should fail, got {r:?}");

    // 解除分区，恢复全网连通
    cluster.heal_all();
    // 给日志追赶与可能的领导切换留时间
    thread::sleep(Duration::from_millis(300));

    // 恢复后能读到多数侧的值，读不到少数未提交的值。
    let mut c = cluster.client();
    // 等待选出领导
    wait_for_leader(&mut c)?;
    // 分区前已提交数据仍在
    assert_eq!(c.get("before")?, Some("ok".into()));
    // 多数侧写入已进入全局状态
    assert_eq!(c.get("maj")?, Some("1".into()));
    // 少数侧未提交写不得出现（防脑裂脏写）
    assert_eq!(c.get("min")?, None);
    // 保留 st 引用以满足编译器对绑定的使用
    let _ = st;
    // 成功返回
    Ok(())
// 结束当前作用域
}
