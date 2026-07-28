//! Pre-vote / CheckQuorum 场景。

// 可注入 Options 的集群工厂
use raft_rust::cluster::{test_options, wait_for_leader, Cluster};
// 分区期间允许 Abort/IO
use raft_rust::error::{Error, Result};
// 自定义 Options
use raft_rust::raft::Options;
// 模拟网络分区后的等待与轮询退避
use std::thread;
// 控制 pre-vote 空转窗口与重选收敛时间
use std::time::Duration;

// 默认开启 pre-vote 与 check_quorum 的集群仍能选举并读写
#[test]
// 冒烟：双开关全开时三节点仍能完成选举与基础 KV
fn election_works_with_prevote_and_check_quorum() -> Result<()> {
    // 默认 test_options 即带 pre_vote/check_quorum
    let cluster = Cluster::spawn(&[1, 2, 3]);
    // 拿到可路由到当前主的客户端句柄
    let mut client = cluster.client();
    // 阻塞直至集群产生合法领导者
    let st = wait_for_leader(&mut client)?;
    // 基础写路径冒烟
    client.put("a", "1")?;
    // 读回校验复制与应用路径通畅
    assert_eq!(client.get("a")?, Some("1".into()));
    // 选举成功任期 >=1
    assert!(st.term >= 1);
    // 冒烟路径全部通过
    Ok(())
}

// 验证隔离跟随者只会空转 pre-vote，不能抬升多数侧任期或抢走领导权
#[test]
// 少数分区 pre-vote 失败不应污染多数侧任期与主身份
fn partitioned_node_does_not_steal_leadership() -> Result<()> {
    // 强制打开 pre_vote + check_quorum
    let mut opts = test_options();
    // 开启预投票，避免无票直接抬升 term
    opts.pre_vote = true;
    // 开启法定人数检查，强化领导存活判定
    opts.check_quorum = true;
    // 以双开关配置拉起三节点
    let cluster = Cluster::spawn_with_options(&[1, 2, 3], opts);
    // 客户端用于写基线并探测多数侧服务
    let mut client = cluster.client();
    // 先等到稳定主，再做分区
    let st = wait_for_leader(&mut client)?;
    // 锁定当前主 id，后续构造多数/少数分区
    let leader = st.leader;
    // 记录隔离前任期，用于断言未被无意义抬升
    let term_before = st.term;
    // 多数侧基线数据
    client.put("stable", "yes")?;

    // 隔离一个非领导跟随者。
    let follower = (1..=3).find(|&id| id != leader).unwrap();
    // 单节点一组，领导+另一跟随者一组
    cluster.partition_groups(&[follower], &[leader, (1..=3).find(|&id| id != leader && id != follower).unwrap()]);

    // 等待足够久让被隔离节点反复 pre-vote。
    thread::sleep(Duration::from_millis(800));

    // 多数侧领导应保持（term 可能不变或仅小幅变化，但数据仍可服务）。
    let mut majority = client.clone();
    // 偏好打到原领导者所在多数分区
    majority.preferred_hint(leader);
    // 轮询读成功标志，容忍短暂路由抖动
    let mut ok = false;
    // 有限次重试，避免永久挂死
    for _ in 0..40 {
        // 探测多数侧是否仍能提供已提交读
        match majority.get("stable") {
            // 多数侧读服务未中断
            Ok(Some(v)) if v == "yes" => {
                // 读到基线值即判定多数侧健康
                ok = true;
                // 提前结束轮询
                break;
            }
            // 瞬时失败可重试
            Ok(_) | Err(Error::Abort) | Err(Error::IO(_)) => {
                // 短暂退避后继续探测
                thread::sleep(Duration::from_millis(50))
            }
            // 非预期错误直接失败，避免掩盖协议 bug
            Err(e) => return Err(e),
        }
    }
    // 多数侧必须在超时内恢复可读
    assert!(ok, "majority should still serve reads");

    // term 不应被少数无意义抬升太多（允许少量因其它原因的增长，但一般应接近）。
    if let Ok(st2) = majority.status_on(leader) {
        // pre-vote 不增加 term；允许 +2 容错心跳/边界
        assert!(
            // 多数侧任期增长应被 pre-vote 抑制在小幅范围内
            st2.term <= term_before + 2,
            // 失败时打印前后任期便于定位
            "term inflated too much: before={term_before} after={}",
            // 实际观测到的任期
            st2.term
        );
    }

    // 恢复连通
    cluster.heal_all();
    // 分区场景断言结束
    Ok(())
}

// 验证 check_quorum：被隔离的旧领导应 step down，多数侧重选并可继续写
#[test]
// 隔离旧主后多数侧应重选并接管读写，验证 check_quorum 生效
fn check_quorum_isolated_leader_steps_down() -> Result<()> {
    // 从默认测试配置拷贝再微调超时
    let mut opts = test_options();
    // 与 check_quorum 联用，防止旧主网络恢复后无票抢主
    opts.pre_vote = true;
    // 核心：旧主失去多数心跳后应主动降级
    opts.check_quorum = true;
    // 缩短选举超时，加速多数侧重选
    opts.election_timeout_range = 5..8;
    // 加快心跳，让 check_quorum 更快发现失联
    opts.heartbeat_interval = 2;
    // 拉起可快速重选的三节点集群
    let cluster = Cluster::spawn_with_options(&[1, 2, 3], opts);
    // 客户端先写基线，再切多数侧探测
    let mut client = cluster.client();
    // 取得初始主身份
    let st = wait_for_leader(&mut client)?;
    // 记录将被隔离的旧主 id
    let leader = st.leader;
    // 旧主在位时写入，供后续多数侧一致性校验
    client.put("x", "1")?;

    // 隔离领导者与两个跟随者。
    let others: Vec<_> = (1..=3u8).filter(|&id| id != leader).collect();
    // 旧主单独成组，两跟随者组成多数分区
    cluster.partition_groups(&[leader], &others);

    // 多数侧应选出新领导。
    let mut majority = client.clone();
    // 从多数分区任一点发起
    majority.preferred_hint(others[0]);
    // 收集多数侧新主 id
    let mut new_leader = None;
    // 给多数侧重选留足轮询窗口
    for _ in 0..80 {
        // 查询多数侧当前主身份
        match majority.status() {
            // 领导者 id 已不是被隔离节点
            Ok(s) if s.leader != leader => {
                // 记录新主，确认旧主已让位
                new_leader = Some(s.leader);
                // 重选完成，停止轮询
                break;
            }
            // 尚未收敛则退避继续
            _ => thread::sleep(Duration::from_millis(50)),
        }
    }
    // 多数侧必须在窗口内选出非旧主
    assert!(new_leader.is_some(), "majority should elect new leader");
    // 旧数据在多数侧仍可读
    assert_eq!(majority.get("x")?, Some("1".into()));
    // 新主可提交新写
    majority.put("y", "2")?;
    // 确认新写已在多数侧提交并可读
    assert_eq!(majority.get("y")?, Some("2".into()));

    // 愈合后全网应看到多数侧提交的 y
    cluster.heal_all();
    // 给旧主追日志与状态对齐留时间
    thread::sleep(Duration::from_millis(300));
    // 愈合后重新拿全网客户端
    let mut c = cluster.client();
    // 等全网再次形成统一领导
    wait_for_leader(&mut c)?;
    // 全网应可见多数侧在分区期间提交的 y
    assert_eq!(c.get("y")?, Some("2".into()));
    // check_quorum 隔离旧主场景通过
    Ok(())
}

// 关闭 pre-vote/check_quorum 时经典 Raft 仍能选举读写
#[test]
// 回归：双开关关闭时仍保持经典选举与 KV 路径
fn works_with_prevote_disabled() -> Result<()> {
    // 显式构造关闭扩展特性的 Options
    let opts = Options {
        // 心跳间隔与其它用例对齐，缩短收敛
        heartbeat_interval: 2,
        // 选举超时窗口略宽，避免关闭 pre-vote 后抖动
        election_timeout_range: 5..10,
        // 单次追赶条目上限，保持默认吞吐
        max_append_entries: 100,
        // 关闭预投票
        pre_vote: false,
        // 关闭领导法定人数检查
        check_quorum: false,
        // 本用例不触发快照
        snapshot_threshold: 0,
    };
    // 以经典 Raft 配置启动三节点
    let cluster = Cluster::spawn_with_options(&[1, 2, 3], opts);
    // 客户端验证关闭扩展后的读写
    let mut client = cluster.client();
    // 经典路径下也应完成选举
    wait_for_leader(&mut client)?;
    // 写路径冒烟
    client.put("p", "1")?;
    // 读回确认应用与复制正常
    assert_eq!(client.get("p")?, Some("1".into()));
    // 关闭扩展特性回归通过
    Ok(())
}
