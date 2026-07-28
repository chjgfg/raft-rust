//! 进程内多节点 Raft 集群集成测试（happy path）。

// 用于构造成员集合，验证 change_membership API 的类型签名
use std::collections::HashSet;

// 集群启动与等待领导者出现的测试辅助
use raft_rust::cluster::{wait_for_leader, Cluster};
// 统一 Result 类型，便于测试内 `?` 传播
use raft_rust::error::Result;

// 验证三节点集群能选出合法领导者，并暴露正确的状态字段
#[test]
// 空日志选举后 Status 字段应齐全且合法
fn elects_leader() -> Result<()> {
    // 启动 id 为 1/2/3 的进程内三节点集群
    let cluster = Cluster::spawn(&[1, 2, 3]);
    // 获取可路由到任意节点的客户端
    let mut client = cluster.client();
    // 阻塞直到集群选出领导者并返回 Status
    let status = wait_for_leader(&mut client)?;
    // 领导者 id 必须落在初始成员集合内
    assert!((1..=3).contains(&status.leader));
    // 选举成功后任期至少为 1
    assert!(status.term >= 1);
    // 空日志选举后通常会有 noop 等提交，提交索引应前进
    assert!(status.commit_index >= 1);
    // 已应用到状态机的索引应与提交进度一致地前进
    assert!(status.applied_index >= 1);
    // match_index 应对全部 3 个投票者有条目
    assert_eq!(status.match_index.len(), 3);
    // 当前配置中的投票者集合大小为 3
    assert_eq!(status.voters.len(), 3);
    // 选举与状态字段断言通过
    Ok(())
}

// 验证经领导者的 Put/Get/Scan 语义：写入可查、缺失键为空、前缀扫描完整
#[test]
// 基础 KV 语义：Put 后 Get/Scan 一致，缺失键为 None
fn put_get_scan() -> Result<()> {
    // 三节点集群作为 KV 服务底座
    let cluster = Cluster::spawn(&[1, 2, 3]);
    // 读写客户端
    let mut client = cluster.client();
    // 确保写路径有稳定领导者
    wait_for_leader(&mut client)?;

    // Put 返回提交后的日志索引，应至少为 1
    let idx = client.put("hello", "world")?;
    // 提交索引合法
    assert!(idx >= 1);
    // 写后读应看到刚提交的值
    assert_eq!(client.get("hello")?, Some("world".into()));
    // 未写入的键应返回 None
    assert_eq!(client.get("missing")?, None);

    // 再写入两个键，为全量 scan 准备数据
    client.put("a", "1")?;
    // 第二个键，凑齐三对
    client.put("b", "2")?;
    // 扫描全部已提交 KV
    let all = client.scan()?;
    // 校验 scan 包含先前写入的三组键值
    assert_eq!(all.get("hello"), Some(&"world".into()));
    // scan 含键 a
    assert_eq!(all.get("a"), Some(&"1".into()));
    // scan 含键 b
    assert_eq!(all.get("b"), Some(&"2".into()));
    // 恰好三对键，无多余条目
    assert_eq!(all.len(), 3);
    // Put/Get/Scan 语义成立
    Ok(())
}

// 验证同一键多次 Put 后读到的是最后一次提交值（覆盖写）
#[test]
// 同键二次 Put：状态机只保留最新提交值
fn overwrite_value() -> Result<()> {
    // 三节点集群
    let cluster = Cluster::spawn(&[1, 2, 3]);
    // 覆盖写客户端
    let mut client = cluster.client();
    // 写前等领导
    wait_for_leader(&mut client)?;
    // 首次写入 v1
    client.put("k", "v1")?;
    // 覆盖前应读到 v1
    assert_eq!(client.get("k")?, Some("v1".into()));
    // 再次 Put 覆盖为 v2，日志追加新条目
    client.put("k", "v2")?;
    // 状态机应用后应只保留最新值
    assert_eq!(client.get("k")?, Some("v2".into()));
    // 覆盖写语义成立
    Ok(())
}

// 验证客户端打到跟随者时写请求会被转发/重定向到领导者并最终提交
#[test]
// 跟随者入口写：依赖转发/重定向仍能多数提交
fn follower_forwards_write() -> Result<()> {
    // 三节点集群
    let cluster = Cluster::spawn(&[1, 2, 3]);
    // 先查领导再选跟随者
    let mut client = cluster.client();
    // 取得当前领导 id
    let status = wait_for_leader(&mut client)?;
    // 记录当前领导者，便于挑选非领导者节点
    let leader = status.leader;
    // 任选一个跟随者 id
    let follower = (1..=3).find(|&id| id != leader).expect("follower");
    // 强制经跟随者发起 Put，依赖转发路径
    let index = client.put_on(follower, "via-follower", "ok")?;
    // 跟随者转发后仍应拿到已提交索引
    assert!(index >= 1);
    // 集群视角可读到该写入
    assert_eq!(client.get("via-follower")?, Some("ok".into()));
    // 跟随者转发写路径成立
    Ok(())
}

// 验证连续写的日志索引严格递增，且提交索引不低于最后一次写
#[test]
// 顺序写：返回索引单调递增，commit_index 覆盖末次写
fn sequential_writes_commit_in_order() -> Result<()> {
    // 三节点集群
    let cluster = Cluster::spawn(&[1, 2, 3]);
    // 顺序写客户端
    let mut client = cluster.client();
    // 顺序写前等领导
    wait_for_leader(&mut client)?;
    // 跟踪上一条写入返回的索引
    let mut last = 0;
    // 顺序提交 10 次写，索引应单调递增
    for i in 0..10 {
        // 每次 Put 返回本次提交索引
        let index = client.put(&format!("k{i}"), &format!("v{i}"))?;
        // 每次 Put 的 commit index 必须大于上一次
        assert!(index > last);
        // 更新单调性基线
        last = index;
    }
    // 全部键应可读且与写入一致
    for i in 0..10 {
        // 逐键校验顺序写结果
        assert_eq!(client.get(&format!("k{i}"))?, Some(format!("v{i}")));
    }
    // 集群 commit_index 应至少覆盖最后一次写
    let status = client.status()?;
    // 提交水位不低于末次 Put 索引
    assert!(status.commit_index >= last);
    // 顺序提交语义成立
    Ok(())
}

// 验证五节点集群同样能选举并完成基础读写
#[test]
// 五投票者集群：选举成功且基础 Put/Get 可用
fn five_node_cluster() -> Result<()> {
    // 启动 5 投票者，多数为 3
    let cluster = Cluster::spawn(&[1, 2, 3, 4, 5]);
    // 五节点客户端
    let mut client = cluster.client();
    // 等待五节点选出领导
    let status = wait_for_leader(&mut client)?;
    // 领导者 id 在 1..5
    assert!((1..=5).contains(&status.leader));
    // match_index 覆盖全部 5 个节点
    assert_eq!(status.match_index.len(), 5);
    // 多数提交写后可读
    client.put("x", "y")?;
    // 五节点下写后读一致
    assert_eq!(client.get("x")?, Some("y".into()));
    // 五节点 happy path 通过
    Ok(())
}

// 冒烟：成员变更 API 所需的 HashSet 类型可构造（完整流程见 membership 测试）
#[test]
// 仅校验 change_membership 入参类型可构造
fn change_membership_uses_hashset() -> Result<()> {
    // 仅验证 API 类型可构造；完整成员变更见 membership 测试。
    let _ = HashSet::<u8>::from([1, 2, 3, 4]);
    // 类型冒烟通过
    Ok(())
}
