//! Raft 集群 CLI 客户端。
//!
//! ```text
//! cargo run --bin raft-cli -- --peers 127.0.0.1:7001 put a apple
//! cargo run --bin raft-cli -- --peers 127.0.0.1:7001,127.0.0.1:7002 get a
//! ```
//!
//! Session：默认把 `client_id` 与单调 `seq` 存在当前目录 `.raft-cli-session`，
//! 保证写重试幂等。可用 `--client-id` 覆盖 id（仍使用文件中的 seq）。

// 哈希集合：投票人/成员/分区边
use std::collections::HashSet;
// CLI 参数与工作目录
use std::env;
// 会话文件读写
use std::fs;
// 对端 TCP 地址
use std::net::SocketAddr;
// 会话文件路径
use std::path::PathBuf;
// 超时与退避间隔
use std::time::Duration;

// 统一 Error/Result
use raft_rust::error::{Error, Result};
// 向对端发起一次客户端 RPC
use raft_rust::net::run_client_request;
// KV 命令与状态机
use raft_rust::raft::kv::{self, Command};
// Raft 协议核心类型
use raft_rust::raft::{NodeID, Request, Response};
// 会话/请求唯一 ID
use uuid::Uuid;

// 默认会话文件名，持久化 client_id 与 seq
const SESSION_FILE: &str = ".raft-cli-session";

// 程序入口
fn main() {
    // 错误分支
    if let Err(e) = run() {
        // 向 stderr 打印错误供脚本捕获
        eprintln!("error: {e}");
        // 非零退出表示 CLI 失败
        std::process::exit(1);
    // main 入口错误处理后结束
    }
// 当前作用域结束
}

// 解析参数、加载会话并分发子命令
fn run() -> Result<()> {
    // 收集 argv，后续 drain 选项
    let mut args: Vec<String> = env::args().skip(1).collect();
    // 对端地址或去掉自己的 peers
    let mut peers: Vec<SocketAddr> = Vec::new();
    // 可选覆盖会话 client_id
    let mut client_id_override: Option<Uuid> = None;

    // 绑定局部变量
    let mut i = 0;
    // 循环解析或等待条件
    while i < args.len() {
        // 解析 --peers 集群入口
        if args[i] == "--peers" && i + 1 < args.len() {
            // 完成当前语句
            peers = parse_peers(&args[i + 1])?;
            // 语句/调用结束
            args.drain(i..=i + 1);
            // 继续下一轮
            continue;
        // 当前作用域结束
        }
        // 解析 --client-id 覆盖身份
        if args[i] == "--client-id" && i + 1 < args.len() {
            // 错误分支处理完毕，进程即将以失败码退出
            client_id_override = Some(
                // CLI 进程入口函数结束
                Uuid::parse_str(&args[i + 1])
                    // 解析转换或错误映射
                    .map_err(|e| Error::InvalidInput(format!("bad client-id: {e}")))?,
            // 语句/调用结束
            );
            // 语句/调用结束
            args.drain(i..=i + 1);
            // 继续下一轮
            continue;
        // 当前作用域结束
        }
        // 完成当前语句
        i += 1;
    // 当前作用域结束
    }

    // 未提供 peers 则报用法错误
    if peers.is_empty() {
        // 返回业务结果或错误
        return Err(Error::InvalidInput(
            // CLI 用法说明字符串
            "usage: raft-cli --peers host:port[,host:port...] [--client-id UUID] \
             // 业务：<put|get|scan|status|members> ...'
             <put|get|scan|status|members> ..."
                // 解析转换或错误映射
                .into(),
        // 语句/调用结束
        ));
    // 当前作用域结束
    }

    // 缺少子命令名
    if args.is_empty() {
        // 返回业务结果或错误
        return Err(Error::InvalidInput("missing command".into()));
    // 当前作用域结束
    }

    // 加载或构造写会话
    let mut session = SessionStore::load(session_path());
    // 可选值解构分支
    if let Some(id) = client_id_override {
        // 处理完 --peers 选项，继续扫描后续参数
        session.client_id = id;
    // 当前作用域结束
    }

    // 按响应/结果分支处理
    match args[0].as_str() {
        // 幂等写：WriteSession + 单调 seq
        "put" => {
            // 校验子命令参数个数
            if args.len() < 3 {
                // 返回业务结果或错误
                return Err(Error::InvalidInput("put <key> <value>".into()));
            // 当前作用域结束
            }
            // 加载或构造写会话
            let seq = session.next_seq();
            // client_id 覆盖值组装为 Some 完成
            session.save()?;
            // 编码后的状态机命令字节
            let cmd = kv::encode(&Command::Put {
                // 填充键字段
                key: args[1].clone(),
                // 填充值字段
                value: args[2].clone(),
            // 语句/调用结束
            });
            // 处理完 --client-id 选项，继续扫描
            let req = Request::WriteSession {
                // 写幂等会话身份
                client_id: session.client_id,
                // 业务：seq,
                seq,
                // 选项扫描循环结束，args 仅剩子命令
                command: cmd,
            // 当前作用域结束
            };
            // 重试必须带同一 (client_id, seq)
            let resp = request_with_retry(&peers, req)?;
            // 按响应/结果分支处理
            match resp {
                // 写回包：解码 KV 层响应
                Response::Write(bytes) => {
                    // 绑定局部变量
                    let r: kv::Response = kv::decode(&bytes)?;
                    // 向用户打印业务结果
                    println!("{r}");
                // 当前作用域结束
                }
                // 未知命令或非预期响应
                other => println!("{other:?}"),
            // match 分支结束
            }
        // 当前作用域结束
        }
        // 构造并返回缺少 peers 的用法错误
        "get" => {
            // peers 非空校验通过
            if args.len() < 2 {
                // 返回业务结果或错误
                return Err(Error::InvalidInput("get <key>".into()));
            // 当前作用域结束
            }
            // 收集 argv，后续 drain 选项
            let cmd = kv::encode(&Command::Get { key: args[1].clone() });
            // 对端地址或去掉自己的 peers
            let resp = request_with_retry(&peers, Request::Read(cmd))?;
            // 按响应/结果分支处理
            match resp {
                // 子命令存在性校验通过
                Response::Read(bytes) => {
                    // 绑定局部变量
                    let r: kv::Response = kv::decode(&bytes)?;
                    // 向用户打印业务结果
                    println!("{r}");
                // 当前作用域结束
                }
                // 未知命令或非预期响应
                other => println!("{other:?}"),
            // match 分支结束
            }
        // 当前作用域结束
        }
        // 全表 Scan
        "scan" => {
            // 可选 client_id 覆盖应用完毕
            let cmd = kv::encode(&Command::Scan);
            // 对端地址或去掉自己的 peers
            let resp = request_with_retry(&peers, Request::Read(cmd))?;
            // 按响应/结果分支处理
            match resp {
                // 读回包：解码 KV 层响应
                Response::Read(bytes) => {
                    // 绑定局部变量
                    let r: kv::Response = kv::decode(&bytes)?;
                    // 向用户打印业务结果
                    println!("{r}");
                // 当前作用域结束
                }
                // 未知命令或非预期响应
                other => println!("{other:?}"),
            // match 分支结束
            }
        // 当前作用域结束
        }
        // put 参数不足时的错误分支结束
        "status" => {
            // 对端地址或去掉自己的 peers
            let resp = request_with_retry(&peers, Request::Status)?;
            // 按响应/结果分支处理
            match resp {
                // 状态回包：展示主从与位点
                Response::Status(s) => {
                    // 向用户打印业务结果
                    println!(
                        // 格式化打印集群状态关键字段
                        "leader={} term={} commit={} applied={} voters={:?}",
                        // 填入领导者、任期、commit 与投票人
                        s.leader, s.term, s.commit_index, s.applied_index, s.voters
                    // 语句/调用结束
                    );
                    // 向用户打印业务结果
                    println!("match_index={:?}", s.match_index);
                // 当前作用域结束
                }
                // 未知命令或非预期响应
                other => println!("{other:?}"),
            // Put 命令字面量编码结束
            }
        // 当前作用域结束
        }
        // 提交 ChangeMembership
        "members" => {
            // 校验子命令参数个数
            if args.len() < 2 {
                // 返回业务结果或错误
                return Err(Error::InvalidInput("members <id,id,...>".into()));
            // 当前作用域结束
            }
            // 收集 argv，后续 drain 选项
            let voters: HashSet<NodeID> = args[1]
                // 解析转换或错误映射
                .split(',')
                // 解析转换或错误映射
                .map(|s| {
                    // WriteSession 请求字段组装完毕
                    s.trim()
                        // 解析转换或错误映射
                        .parse::<NodeID>()
                        // 解析转换或错误映射
                        .map_err(|e| Error::InvalidInput(format!("bad id: {e}")))
                // 业务：})
                })
                // 解析转换或错误映射
                .collect::<Result<_>>()?;
            // 对端地址或去掉自己的 peers
            let resp = request_with_retry(&peers, Request::ChangeMembership { voters })?;
            // 按响应/结果分支处理
            match resp {
                // 成员变更已提议，打印日志索引
                Response::ChangeMembership { index } => println!("membership proposed @ {index}"),
                // 未知命令或非预期响应
                other => println!("{other:?}"),
            // match 分支结束
            }
        // 当前作用域结束
        }
        // Write 成功分支：已向用户打印 KV 响应
        other => return Err(Error::InvalidInput(format!("unknown command {other}"))),
    // match 分支结束
    }
    // 成功返回空结果
    Ok(())
// 写响应 match 结束
}

// 将 host:port 列表解析为地址
fn parse_peers(s: &str) -> Result<Vec<SocketAddr>> {
    // 业务：s.split(',')
    s.split(',')
        // 解析转换或错误映射
        .map(|p| {
            // 业务：p.trim()
            p.trim()
                // 解析转换或错误映射
                .parse()
                // 解析转换或错误映射
                .map_err(|e| Error::InvalidInput(format!("bad peer addr {p}: {e}")))
        // get 参数不足时的错误分支结束
        })
        // 解析转换或错误映射
        .collect()
// 当前作用域结束
}

// 多 peer 轮询，Abort/IO 可重试
fn request_with_retry(peers: &[SocketAddr], request: Request) -> Result<Response> {
    // 可重试的最后错误
    let mut last = Error::Abort;
    // 遍历集合或重试轮次
    for _ in 0..40 {
        // 遍历集合或重试轮次
        for addr in peers {
            // 按响应/结果分支处理
            match run_client_request(*addr, request.clone(), Duration::from_secs(2)) {
                // 成功路径：推进状态或返回
                Ok(r) => return Ok(r),
                // 无主/转发失败，可换节点重试
                Err(Error::Abort) => last = Error::Abort,
                // 语句/调用结束
                Err(Error::IO(e)) => last = Error::IO(e),
                // 超时或通道错误处理
                Err(e) => return Err(e),
            // Read 成功分支：已打印 Get 结果
            }
        // 当前作用域结束
        }
        // 失败后短暂退避，等待选主稳定
        std::thread::sleep(Duration::from_millis(100));
    // 读响应 match 结束
    }
    // get 子命令处理结束
    Err(last)
// 当前作用域结束
}

// cwd 下会话文件路径
fn session_path() -> PathBuf {
    // 读取环境或工作目录
    env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join(SESSION_FILE)
// 当前作用域结束
}

/// 持久化 client_id + 已用最大 seq。
struct SessionStore {
    // 会话文件落盘路径
    path: PathBuf,
    // 写幂等会话身份
    client_id: Uuid,
    // 已使用的最大写序号
    last_seq: u64,
// 当前作用域结束
}

// 实现该类型的方法
impl SessionStore {
    // Scan 成功分支：已打印全表
    fn load(path: PathBuf) -> Self {
        // Result 成功解构分支
        if let Ok(text) = fs::read_to_string(&path) {
            // 会话或解析得到的客户端身份
            let mut client_id = Uuid::new_v4();
            // scan 响应 match 结束
            let mut last_seq = 0u64;
            // scan 子命令处理结束
            for line in text.lines() {
                // 可选值解构分支
                if let Some(v) = line.strip_prefix("client_id=") {
                    // Result 成功解构分支
                    if let Ok(id) = Uuid::parse_str(v.trim()) {
                        // 完成当前语句
                        client_id = id;
                    // 当前作用域结束
                    }
                // 进入代码块
                } else if let Some(v) = line.strip_prefix("last_seq=") {
                    // Result 成功解构分支
                    if let Ok(n) = v.trim().parse() {
                        // 完成当前语句
                        last_seq = n;
                    // 当前作用域结束
                    }
                // 当前作用域结束
                }
            // 当前作用域结束
            }
            // 写幂等会话身份
            return Self { path, client_id, last_seq };
        // 当前作用域结束
        }
        // 写幂等会话身份
        Self { path, client_id: Uuid::new_v4(), last_seq: 0 }
    // 当前作用域结束
    }

    // 分配下一个单调写序号
    fn next_seq(&mut self) -> u64 {
        // 更新自身状态字段
        self.last_seq = self.last_seq.saturating_add(1);
        // Status 成功分支结束
        self.last_seq
    // 当前作用域结束
    }

    // status 响应 match 结束
    fn save(&self) -> Result<()> {
        // status 子命令处理结束
        let body = format!("client_id={}\nlast_seq={}\n", self.client_id, self.last_seq);
        // 会话文件 IO
        fs::write(&self.path, body).map_err(|e| Error::IO(format!("write session: {e}")))?;
        // 成功返回空结果
        Ok(())
    // 当前作用域结束
    }
// 当前作用域结束
}
