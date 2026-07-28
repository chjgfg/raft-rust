//! 多客户端并发写。

// 线程同步：Barrier 保证多客户端同时开闸写
use std::sync::{Arc, Barrier};
// 多线程并发客户端
use std::thread;

// 集群与等领导者辅助
use raft_rust::cluster::{wait_for_leader, Cluster};
// 测试结果统一 Result 传播
use raft_rust::error::Result;

// 验证多客户端并发 Put 最终都能提交，且键值互不覆盖丢失
#[test]
// 四客户端齐开闸并发写，最终状态机含全部独立键
fn concurrent_puts() -> Result<()> {
    // 三节点集群提供并发写入口
    let cluster = Cluster::spawn(&[1, 2, 3]);
    // 主线程客户端：先等领导再开闸
    let mut client = cluster.client();
    // 先等领导者稳定，避免冷启动阶段全部 Abort
    wait_for_leader(&mut client)?;

    // 4 个工作线程 + 主线程同步开闸（new(4) 只等 4 个 worker）
    let barrier = Arc::new(Barrier::new(4));
    // 收集各 worker 句柄以便 join
    let mut handles = vec![];
    // 启动 4 个并发客户端线程
    for t in 0..4 {
        // 每个线程独立 client，模拟多会话
        let mut c = cluster.client();
        // 共享 barrier，保证四线程同时开闸
        let b = barrier.clone();
        // 将 worker 迁入独立线程
        handles.push(thread::spawn(move || {
            // 四线程齐备后再开始写，放大并发冲突
            b.wait();
            // 每线程写 10 个独立键，避免业务层互相覆盖
            for i in 0..10 {
                // 线程 id 前缀保证键空间隔离
                let key = format!("t{t}-k{i}");
                // 与键一一对应的期望值
                let val = format!("v{i}");
                // 允许短暂 Abort 重试。
                for _ in 0..30 {
                    // 领导切换/短暂拒绝时重试直至 Put 成功
                    if c.put(&key, &val).is_ok() {
                        // 本键已提交，进入下一键
                        break;
                    // 结束当前作用域
                    }
                    // 退避避免打爆集群
                    thread::sleep(std::time::Duration::from_millis(20));
                // 结束当前作用域
                }
            // 结束当前作用域
            }
        // 业务逻辑步骤
        }));
    // 结束当前作用域
    }
    // 等待全部 worker 完成，任一 panic 则测试失败
    for h in handles {
        // join 失败说明 worker 内断言/panic
        h.join().expect("thread panicked");
    // 结束当前作用域
    }

    // 主线程再开 client 校验最终状态机
    let mut c = cluster.client();
    // 逐键校验 4×10 次写入均已提交且值正确
    for t in 0..4 {
        // 校验该线程写出的 10 个键
        for i in 0..10 {
            // 重建期望键名
            let key = format!("t{t}-k{i}");
            // 并发写不得丢键或写错值
            assert_eq!(c.get(&key)?, Some(format!("v{i}")), "missing {key}");
        // 结束当前作用域
        }
    // 结束当前作用域
    }
    // 并发写最终一致性成立
    Ok(())
// 结束当前作用域
}
