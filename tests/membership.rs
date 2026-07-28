//! 联合共识成员变更。

// 成员集合类型
use std::collections::HashSet;
// 轮询配置落地
use std::thread;
// 配置落地轮询间隔
use std::time::Duration;

// 集群与等领导者
use raft_rust::cluster::{wait_for_leader, Cluster};
// 重叠变更时可能返回 InvalidInput/Abort
use raft_rust::error::{Error, Result};

// 验证经联合共识向集群添加第 4 个投票者，配置落地后新旧数据均可读写
#[test]
// 三节点扩为四节点：joint 完成后新旧键均可读写
fn add_voter_joint() -> Result<()> {
    // mut：后续 start 新节点
    let mut cluster = Cluster::spawn(&[1, 2, 3]);
    // 经当前领导发起成员变更与 KV 操作
    let mut client = cluster.client();
    // 扩容前必须已有稳定领导者
    wait_for_leader(&mut client)?;
    // 扩容前写入，扩容后应仍可读
    client.put("before", "1")?;

    // 拉起节点 4（空日志），再变更成员。
    cluster.start(4);
    // 目标配置：四投票者
    let voters: HashSet<u8> = [1, 2, 3, 4].into_iter().collect();
    // 发起 joint consensus 成员变更，返回配置条目索引
    let idx = client.change_membership(voters.clone())?;
    // 配置条目应进入日志并返回合法索引
    assert!(idx >= 1);

    // 等待 simple 配置落地。
    for _ in 0..40 {
        // 用 status 观察 voters 集合是否已变为 4
        if let Ok(st) = client.status() {
            // voters 变为 4 表示 C_new 已提交并应用
            if st.voters.len() == 4 {
                // C_new 已可见，结束等待
                break;
            }
        }
        // 配置尚未落地，短暂退避
        thread::sleep(Duration::from_millis(50));
    }
    // 最终配置必须包含 4 个投票者
    let st = client.status()?;
    // 断言投票者数量与扩容目标一致
    assert_eq!(st.voters.len(), 4, "voters={:?}", st.voters);

    // 新配置下写应成功
    client.put("after", "2")?;
    // 扩容前数据保留
    assert_eq!(client.get("before")?, Some("1".into()));
    // 扩容后写入可见
    assert_eq!(client.get("after")?, Some("2".into()));
    // 扩容场景读写闭环完成
    Ok(())
}

// 验证经联合共识安全移除一个非领导投票者，缩容后仍可读写
#[test]
// 四节点缩为三节点：移除非领导后数据与写能力保留
fn remove_voter_joint() -> Result<()> {
    // 初始四节点，多数为 3
    let cluster = Cluster::spawn(&[1, 2, 3, 4]);
    // 缩容与读写共用客户端
    let mut client = cluster.client();
    // 缩容前需稳定领导者
    wait_for_leader(&mut client)?;
    // 缩容前基线数据
    client.put("k", "v")?;

    // 移除非领导节点。
    let st = client.status()?;
    // 若 4 是领导则移 3，否则移 4，避免本用例同时测领导下台
    let remove = if st.leader == 4 { 3 } else { 4 };
    // 从四人配置出发构造目标三人集合
    let mut voters: HashSet<u8> = [1, 2, 3, 4].into_iter().collect();
    // 从目标配置中删掉选定节点
    voters.remove(&remove);

    // 提交到三人配置
    let idx = client.change_membership(voters.clone())?;
    // 成员变更日志索引应合法
    assert!(idx >= 1);

    // 等待 C_new 应用：投票者 3 且不含被移除 id
    for _ in 0..40 {
        // 轮询 status 直到配置规模与成员符合预期
        if let Ok(st) = client.status() {
            // 三人配置且已不含被移除节点
            if st.voters.len() == 3 && !st.voters.contains(&remove) {
                // 缩容配置已落地
                break;
            }
        }
        // 配置传播中，继续等待
        thread::sleep(Duration::from_millis(50));
    }
    // 再取一次 status 做硬断言
    let st = client.status()?;
    // 配置规模变为 3
    assert_eq!(st.voters.len(), 3);
    // 被移除节点不再是投票者
    assert!(!st.voters.contains(&remove));
    // 缩容不丢已提交数据
    assert_eq!(client.get("k")?, Some("v".into()));
    // 新配置下仍可写
    client.put("k2", "v2")?;
    // 缩容后新写可读
    assert_eq!(client.get("k2")?, Some("v2".into()));
    // 缩容场景验证完成
    Ok(())
}

/// 移除当前领导者：Simple 配置提交后旧领导 step down，集群仍可读写。
#[test]
// 目标配置不含旧主：旧主 step down，新主接管后数据与写能力保留
fn remove_leader_steps_down() -> Result<()> {
    // mut：成员变更过程中可能涉及节点集合变化
    let mut cluster = Cluster::spawn(&[1, 2, 3]);
    // 成员变更与后续读写客户端
    let mut client = cluster.client();
    // 记录即将被移出配置的领导者
    let st = wait_for_leader(&mut client)?;
    // 即将被移出配置的领导者
    let old_leader = st.leader;
    // 领导下台前写入基线数据
    client.put("before", "1")?;

    // 目标配置：仅剩除旧领导外的两个节点
    let voters: HashSet<u8> = (1..=3u8).filter(|&id| id != old_leader).collect();
    // 移除领导后 C_new 提交应触发旧主 step down
    client.change_membership(voters.clone())?;

    // 等待新主（不能是旧领导）。
    let mut new_leader = None;
    // 约 4s 等待领导切换与配置落地
    for _ in 0..80 {
        // 观察领导者是否已换人且配置为 2
        match client.status() {
            // 配置为 2 且领导者已换人
            Ok(s) if s.leader != old_leader && s.voters.len() == 2 => {
                // 记录新主 id
                new_leader = Some(s.leader);
                // 切换完成
                break;
            }
            // 尚未切换完成，退避重试
            _ => thread::sleep(Duration::from_millis(50)),
        }
    }
    // 必须出现新主
    let new_leader = new_leader.expect("new leader after removing old");
    // 新主不能是已移出配置的旧领导
    assert_ne!(new_leader, old_leader);
    // 旧数据仍在
    assert_eq!(client.get("before")?, Some("1".into()));
    // 新主可继续写
    client.put("after", "2")?;
    // 新主写入对读可见
    assert_eq!(client.get("after")?, Some("2".into()));
    // 移除领导场景验证完成
    Ok(())
}

// 验证多次写后扩容，历史键值在新配置下完整保留
#[test]
// 批量历史写 + 扩容：新配置下历史键全部可读
fn membership_change_preserves_data() -> Result<()> {
    // 三节点起步，后续扩到四节点
    let mut cluster = Cluster::spawn(&[1, 2, 3]);
    // 扩容前后读写客户端
    let mut client = cluster.client();
    // 批量写前先稳定选举
    wait_for_leader(&mut client)?;
    // 扩容前批量写入
    for i in 0..5 {
        // 历史键值对，扩容后应完整保留
        client.put(&format!("k{i}"), &format!("v{i}"))?;
    }
    // 加入节点 4 并变更配置
    cluster.start(4);
    // 提交含节点 4 的新投票者集合
    client.change_membership([1, 2, 3, 4].into_iter().collect())?;
    // 给 joint→simple 与日志追赶留时间
    thread::sleep(Duration::from_millis(200));
    // 全部历史键值必须仍可读
    for i in 0..5 {
        // 逐键校验扩容不丢历史状态
        assert_eq!(client.get(&format!("k{i}"))?, Some(format!("v{i}")));
    }
    // 数据保留断言全部通过
    Ok(())
}

// 验证重叠/进行中的成员变更：第二次要么成功（第一次已结束）要么被拒绝，不得崩溃
#[test]
// 连续两次变更：允许串行成功或第二次被拒，禁止非预期错误
fn overlapping_change_rejected() -> Result<()> {
    // 三节点起步，再拉起 4/5 作为候选成员
    let mut cluster = Cluster::spawn(&[1, 2, 3]);
    // 两次变更共用客户端
    let mut client = cluster.client();
    // 变更前先有领导
    wait_for_leader(&mut client)?;
    // 预先拉起候选新成员，避免变更时节点不存在
    cluster.start(4);
    // 第二次变更目标再含节点 5
    cluster.start(5);

    // 第一次变更。
    let r1 = client.change_membership([1, 2, 3, 4].into_iter().collect());
    // 首轮变更必须成功提交
    assert!(r1.is_ok(), "first change should succeed: {r1:?}");

    // 立即第二次（可能仍在 joint/pending）——允许成功（若第一次已完成）或 InvalidInput/Abort。
    let r2 = client.change_membership([1, 2, 3, 4, 5].into_iter().collect());
    // 按协议分类处理第二次结果
    match r2 {
        Ok(_) => {} // 第一次已完成则允许
        // 配置变更进行中应被明确拒绝或可重试 Abort
        Err(Error::InvalidInput(_)) | Err(Error::Abort) => {}
        // 其它错误类型不符合协议预期
        Err(e) => return Err(e),
    }
    // 重叠变更未导致崩溃或非法错误
    Ok(())
}
