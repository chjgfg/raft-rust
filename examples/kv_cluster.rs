//! 使用共享 cluster 模块的 3 节点演示（薄封装）。

// 进程内集群与等主辅助
use raft_rust::cluster::{wait_for_leader, Cluster};
// 统一 Error/Result
use raft_rust::error::Result;

// 程序入口
fn main() -> Result<()> {
    // 默认 info 日志便于观察选举
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // 向用户打印业务结果
    println!("Starting 3-node Raft cluster (example)...");
    // 进程内集群实例
    let cluster = Cluster::spawn(&[1, 2, 3]);
    // 进程内集群实例
    let mut client = cluster.client();

    // 轮询直到选出可服务领导者
    let status = wait_for_leader(&mut client)?;
    // 向用户打印业务结果
    println!(
        // 格式化打印集群状态关键字段
        "Leader={} term={} commit={} voters={:?}",
        // 填入领导者、任期、commit 与投票人
        status.leader, status.term, status.commit_index, status.voters
    // 语句/调用结束
    );

    // 遍历集合或重试轮次
    for (k, v) in [("a", "apple"), ("b", "banana"), ("c", "cherry")] {
        // 写提交日志索引
        let index = client.put(k, v)?;
        // 向用户打印业务结果
        println!("  put {k}={v} @ {index}");
    // 当前作用域结束
    }
    // 遍历集合或重试轮次
    for k in ["a", "b", "c", "missing"] {
        // 向用户打印业务结果
        println!("  get {k} => {:?}", client.get(k)?);
    // match 分支结束
    }
    // 向用户打印业务结果
    println!("scan => {:?}", client.scan()?);
    // 成功返回空结果
    Ok(())
// 结束领导者状态的汇总打印
}
