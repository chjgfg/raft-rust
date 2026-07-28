//! 写后读线性一致（允许 Abort 重试）。

// 集群与等领导者
use raft_rust::cluster::{wait_for_leader, Cluster};
// 测试内统一用 Result 传播失败
use raft_rust::error::Result;

// 验证同一客户端写后立即读必见自己的写（线性一致写后读）
#[test]
// 稳定任期内反复 Put+Get，断言读侧不落后于自己的写
fn write_then_read_sees_value() -> Result<()> {
    // 三节点集群提供读写服务
    let cluster = Cluster::spawn(&[1, 2, 3]);
    // 单客户端会话贯穿写后读
    let mut client = cluster.client();
    // 先等到稳定领导者，避免冷启动 Abort 干扰断言
    wait_for_leader(&mut client)?;
    // 多轮 Put+Get，放大任期稳定期间的读路径正确性
    for i in 0..20 {
        // 每轮独立键，避免跨轮覆盖干扰
        let k = format!("k{i}");
        // 期望写后立即可见的值
        let v = format!("v{i}");
        // 写必须多数提交后返回
        client.put(&k, &v)?;
        // 紧接着读必须看到刚写的值，不得读到旧/空
        assert_eq!(client.get(&k)?, Some(v));
    }
    // 全部轮次写后读均满足线性一致
    Ok(())
}

// 验证旧领导者宕机后，新任期内读仍能看到崩溃前已提交的值
#[test]
// 领导切换后读路径不得丢失崩溃前已提交数据
fn read_after_leader_change() -> Result<()> {
    // mut：需要 stop 领导者
    let mut cluster = Cluster::spawn(&[1, 2, 3]);
    // 崩溃前后共用同一客户端做读写
    let mut client = cluster.client();
    // 记录写时领导者，供后续故障注入
    let st = wait_for_leader(&mut client)?;
    // 崩溃前写入稳定键
    client.put("stable", "1")?;
    // 故障注入：停掉写时的领导者，触发重选
    cluster.stop(st.leader);

    // 轮询读，直到新主可服务或超时
    let mut seen = None;
    // 约 4s 内等待新主可服务读
    for _ in 0..80 {
        // 对崩溃前键做线性读探测
        match client.get("stable") {
            // 任一成功读到的值即为恢复后的线性读结果
            Ok(v) => {
                // 记录恢复后读到的值
                seen = Some(v);
                // 读成功即可结束轮询
                break;
            }
            // 无主/Abort 期间继续等
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(50)),
        }
    }
    // 重选后必须仍读到崩溃前已提交的 "1"
    assert_eq!(seen, Some(Some("1".into())));
    // 领导切换场景下数据与读语义均成立
    Ok(())
}
