//! 真多进程联调：拉起多个 `raft-node` 子进程，经 TCP 读写。
//!
//! 需要已编译的二进制：`cargo build --bin raft-node`。

// 写节点配置与数据目录
use std::fs;
// 子进程监听地址
use std::net::SocketAddr;
// 临时目录与二进制路径
use std::path::PathBuf;
// 拉起/杀掉 raft-node 子进程
use std::process::{Child, Command as ProcessCommand, Stdio};
// 退避重试时的短暂休眠
use std::thread;
// 超时轮询与唯一目录名
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// 客户端请求错误与 Result
use raft_rust::error::{Error, Result};
// TCP 客户端发 Request
use raft_rust::net::run_client_request;
// KV 命令编解码
use raft_rust::raft::kv::{self, Command as KvCommand};
// Raft 客户端协议消息
use raft_rust::raft::{Request, Response};

// 生成互不冲突的临时工作目录，避免并行测试撞路径
fn unique_dir() -> PathBuf {
    // 进程内递增序号，配合 pid 与纳秒时间戳
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    // 原子取号保证同进程内多次调用也不重名
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // 拼出 temp/raft-mp-<pid>-<seq>-<nanos> 路径
    std::env::temp_dir().join(format!(
        // 目录名前缀标识多进程集成测试
        "raft-mp-{}-{}-{}",
        // 当前测试进程 pid，隔离不同 cargo test 进程
        std::process::id(),
        // 同进程内序号
        seq,
        // 纳秒时间戳进一步降低碰撞概率
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    // 结束多行表达式
    ))
// 返回唯一临时根目录
}

// 绑定 :0 拿一个空闲端口，避免多进程测例端口冲突
fn free_port() -> u16 {
    // 让 OS 分配临时端口后立即释放，仅借用端口号
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    // 取出实际分配的端口供配置写入
    listener.local_addr().unwrap().port()
// free_port 结束
}

// 解析当前 profile 下的目标二进制路径（debug/release，含 Windows .exe）
fn bin_path(name: &str) -> PathBuf {
    // 从 crate 根定位 target/
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // 进入 cargo 产物目录
    path.push("target");
    // 测试通常在 debug 构建下跑
    let profile = if cfg!(debug_assertions) { "debug" } else { "release" };
    // 按 profile 选择 debug 或 release 子目录
    path.push(profile);
    // Windows 下可执行文件带 .exe 后缀
    if cfg!(windows) {
        // 拼接带扩展名的文件名
        path.push(format!("{name}.exe"));
    // 非 Windows 直接用无后缀名
    } else {
        // Unix 产物无扩展名
        path.push(name);
    // 平台分支结束
    }
    // 返回完整二进制路径
    path
// bin_path 结束
}

// 子进程节点句柄：退出时 kill，并持有地址与目录生命周期
struct NodeProc {
    // OS 子进程
    child: Child,
    // 该节点 TCP 监听地址
    addr: SocketAddr,
    // 配置/数据目录，Drop 前保持存在
    _dir: PathBuf,
// NodeProc 字段定义结束
}

// 测试结束时自动回收子进程，防止端口与句柄泄漏
impl Drop for NodeProc {
    // RAII：作用域结束即强制终止 raft-node
    fn drop(&mut self) {
        // 测试结束强制结束子进程，避免残留占用端口
        let _ = self.child.kill();
        // 等待退出，避免僵尸进程
        let _ = self.child.wait();
    // Drop::drop 结束
    }
// Drop 实现结束
}

// 写出单个 raft-node 的 yaml 配置（id、监听、data_dir、peers、options）
fn write_node_config(dir: &PathBuf, id: u8, port: u16, peers: &[(u8, u16)]) -> PathBuf {
    // 确保根目录存在
    fs::create_dir_all(dir).unwrap();
    // 每节点独立 data_dir，BitCask 落盘
    let data_dir = dir.join(format!("data{id}"));
    // 预先创建存储目录，避免节点启动时找不到路径
    fs::create_dir_all(&data_dir).unwrap();
    // 逐步拼装 yaml 文本
    let mut body = String::new();
    // node 段：身份与监听
    body.push_str(&format!("node:\n  id: {id}\n  listen: \"127.0.0.1:{port}\"\n"));
    // Windows 路径转正斜杠，便于 yaml
    let data = data_dir.display().to_string().replace('\\', "/");
    // 写入本节点持久化目录
    body.push_str(&format!("  data_dir: \"{data}\"\n\n"));
    // 单节点与多节点 peers 段格式不同
    if peers.is_empty() {
        // 单节点无 peer
        body.push_str("peers: []\n\n");
    // 多节点列出其余成员
    } else {
        // peers 数组起始
        body.push_str("peers:\n");
        // 列出其它节点 id 与端口
        for (pid, pport) in peers {
            // 每 peer 一行：id + 本机回环地址
            body.push_str(&format!(
                // 写入 peer 地址 YAML 行
                "  - {{ id: {pid}, addr: \"127.0.0.1:{pport}\" }}\n"
            // format! 参数列表结束
            ));
        // peer 循环结束
        }
        // peers 段后空行分隔 options
        body.push('\n');
    // peers 分支结束
    }
    // 缩短选举/心跳，加速多进程集成测试收敛
    body.push_str(
        // 写入加速选举的 options YAML
        "options:\n  heartbeat_interval: 2\n  election_timeout_min: 5\n  election_timeout_max: 10\n  max_append_entries: 100\n  pre_vote: true\n  check_quorum: true\n  snapshot_threshold: 0\n",
    // options 字符串实参结束
    );
    // 配置文件按节点 id 命名
    let cfg_path = dir.join(format!("node{id}.yaml"));
    // 落盘供子进程 --config 加载
    fs::write(&cfg_path, body).unwrap();
    // 返回配置路径给 spawn
    cfg_path
// write_node_config 结束
}

// 以给定配置路径 spawn raft-node 子进程
fn spawn_node(cfg: &PathBuf) -> Child {
    // 解析当前构建产物中的 raft-node 路径
    let bin = bin_path("raft-node");
    // 二进制缺失时给出明确构建提示
    assert!(
        // 路径必须真实存在，否则测例无意义
        bin.exists(),
        // 提示先 cargo build --bin raft-node
        "raft-node binary missing at {bin:?}; run cargo build --bin raft-node first"
    // assert 结束
    );
    // 以 --config 启动节点主进程
    ProcessCommand::new(bin)
        // 配置文件参数名
        .arg("--config")
        // 指向刚写出的 yaml
        .arg(cfg)
        // 静默 stdout/stderr，避免污染测试输出
        .stdout(Stdio::null())
        // 同样丢弃 stderr，失败靠客户端超时体现
        .stderr(Stdio::null())
        // 真正创建 OS 子进程
        .spawn()
        // spawn 失败直接 panic，测例无法继续
        .expect("spawn raft-node")
// spawn_node 结束
}

// 在超时内轮询各 peer Status，直到任一节点可响应（进程已 listen 且 Raft 可服务）
fn wait_ready(peers: &[SocketAddr], timeout: Duration) -> Result<Response> {
    // 绝对截止时间，避免无限阻塞
    let deadline = Instant::now() + timeout;
    // 保留最后错误便于超时返回
    let mut last = Error::Abort;
    // 在截止前反复探测
    while Instant::now() < deadline {
        // 轮询所有 peer，任一成功即视为集群可服务
        for addr in peers {
            // 短超时 Status 探测
            match run_client_request(*addr, Request::Status, Duration::from_millis(500)) {
                // 收到 Status 说明 listen 与协议栈已就绪
                Ok(r) => return Ok(r),
                // 记录错误继续试下一地址
                Err(e) => last = e,
            // match 结束
            }
        // 本轮 peer 扫完
        }
        // 短暂休眠再下一轮，避免打满 CPU
        thread::sleep(Duration::from_millis(100));
    // 超时循环结束
    }
    // 全部失败则返回最后一次错误
    Err(last)
// wait_ready 结束
}

// 带重试的客户端请求：在领导切换/短暂 IO 失败时轮询所有 peer
fn request(peers: &[SocketAddr], req: Request) -> Result<Response> {
    // 累计可重试错误，最终作为超时结果
    let mut last = Error::Abort;
    // 外层重试轮次
    for _ in 0..40 {
        // 每轮扫一遍所有节点，适配未知 leader
        for addr in peers {
            // 2s 超时发真实业务请求
            match run_client_request(*addr, req.clone(), Duration::from_secs(2)) {
                // 成功立即返回
                Ok(r) => return Ok(r),
                // Abort：无主或需重定向，继续试其它节点
                Err(Error::Abort) => last = Error::Abort,
                // 瞬时网络错误
                Err(Error::IO(e)) => last = Error::IO(e),
                // 业务/协议错误立即上抛
                Err(e) => return Err(e),
            // match 结束
            }
        // 本轮地址扫完
        }
        // 领导切换窗口内稍等再试
        thread::sleep(Duration::from_millis(50));
    // 重试轮次耗尽
    }
    // 仍失败则返回累计的最后错误
    Err(last)
// request 结束
}

// 验证真多进程单节点：子进程启动后成为领导并完成 Put/Get
#[test]
// 单进程单节点端到端写读冒烟
fn multi_process_single_node_put_get() {
    // 独立临时目录
    let dir = unique_dir();
    // 为本节点申请空闲监听端口
    let port = free_port();
    // 无 peers 的单节点配置
    let cfg = write_node_config(&dir, 1, port, &[]);
    // 拉起 raft-node 子进程
    let child = spawn_node(&cfg);
    // 用 NodeProc 托管生命周期
    let mut proc = NodeProc {
        // 持有子进程句柄以便后续 kill
        child,
        // 客户端连接目标地址
        addr: format!("127.0.0.1:{port}").parse().unwrap(),
        // 目录随 NodeProc 存活，避免过早删除配置与数据
        _dir: dir,
    // 结构体字面量结束
    };

    // 单 peer 列表，供 wait_ready/request 复用
    let peers = [proc.addr];
    // 等子进程可 Status
    let st = wait_ready(&peers, Duration::from_secs(10)).expect("status");
    // 校验启动后的集群状态视图
    match st {
        // 单节点必须自认为领导
        Response::Status(s) => assert_eq!(s.leader, 1),
        // 非 Status 响应视为协议异常
        other => panic!("{other:?}"),
    // match 结束
    }

    // 编码 Put 命令走 Write 路径
    let put = Request::Write(kv::encode(&KvCommand::Put {
        // 测试键
        key: "mp".into(),
        // 期望落盘的值
        value: "ok".into(),
    // Put 命令构造结束
    }));
    // 单节点多数即自己，应提交成功
    assert!(matches!(request(&peers, put).expect("put"), Response::Write(_)));

    // 读回验证状态机
    let get = Request::Read(kv::encode(&KvCommand::Get { key: "mp".into() }));
    // 解析读响应并断言值
    match request(&peers, get).expect("get") {
        // 成功读路径：解码 payload
        Response::Read(bytes) => {
            // 反序列化为 KV 层 Response
            let r: kv::Response = kv::decode(&bytes).unwrap();
            // 必须读到刚 Put 的值
            assert_eq!(r, kv::Response::Get(Some("ok".into())));
        // Read 分支结束
        }
        // 其它响应类型直接失败
        other => panic!("{other:?}"),
    // match 结束
    }

    // 显式 kill，双保险
    let _ = proc.child.kill();
// 单节点测例结束
}

// 验证三进程经 TCP 互连后能选举并跨节点提交/读取
#[test]
// 三节点真多进程选举与多数写读
fn multi_process_three_nodes_elect_and_write() {
    // 三节点共享同一临时根，子目录按 id 区分
    let dir = unique_dir();
    // 三节点各占独立端口
    let p1 = free_port();
    // 节点 2 监听端口
    let p2 = free_port();
    // 节点 3 监听端口
    let p3 = free_port();

    // 互相声明 peers，形成全连接配置
    let c1 = write_node_config(&dir, 1, p1, &[(2, p2), (3, p3)]);
    // 节点 2 配置：peers 为 1 与 3
    let c2 = write_node_config(&dir, 2, p2, &[(1, p1), (3, p3)]);
    // 节点 3 配置：peers 为 1 与 2
    let c3 = write_node_config(&dir, 3, p3, &[(1, p1), (2, p2)]);

    // 同时拉起三个子进程
    let mut nodes = vec![
        // 节点 1 进程句柄
        NodeProc {
            // 以 c1 配置启动
            child: spawn_node(&c1),
            // 节点 1 客户端地址
            addr: format!("127.0.0.1:{p1}").parse().unwrap(),
            // 共享根目录（clone 延长生命周期）
            _dir: dir.clone(),
        // 节点 1 结束
        },
        // 节点 2 进程句柄
        NodeProc {
            // 以 c2 配置启动
            child: spawn_node(&c2),
            // 节点 2 客户端地址
            addr: format!("127.0.0.1:{p2}").parse().unwrap(),
            // 共享根目录
            _dir: dir.clone(),
        // 节点 2 结束
        },
        // 节点 3 进程句柄
        NodeProc {
            // 以 c3 配置启动
            child: spawn_node(&c3),
            // 节点 3 客户端地址
            addr: format!("127.0.0.1:{p3}").parse().unwrap(),
            // 最后一处接管 dir 所有权
            _dir: dir,
        // 节点 3 结束
        },
    // vec 结束
    ];

    // 收集三节点地址供客户端轮询
    let peers: Vec<SocketAddr> = nodes.iter().map(|n| n.addr).collect();
    // 等集群可服务（选举完成）
    let st = wait_ready(&peers, Duration::from_secs(15)).expect("cluster ready");
    // 校验选举结果与投票者集合
    match st {
        // Status 成功路径
        Response::Status(s) => {
            // 领导者 id 合法
            assert!((1..=3).contains(&s.leader));
            // 配置为三投票者
            assert_eq!(s.voters.len(), 3);
        // Status 分支结束
        }
        // 非 Status 视为异常
        other => panic!("{other:?}"),
    // match 结束
    }

    // 经 TCP 多数提交写
    let put = Request::Write(kv::encode(&KvCommand::Put {
        // 集群级测试键
        key: "cluster".into(),
        // 标识三节点场景的值
        value: "3nodes".into(),
    // Put 结束
    }));
    // 写必须在多数提交后返回
    request(&peers, put).expect("put");

    // 任意可达节点读应一致
    let get = Request::Read(kv::encode(&KvCommand::Get { key: "cluster".into() }));
    // 校验读路径与值一致性
    match request(&peers, get).expect("get") {
        // 解码 Read 载荷
        Response::Read(bytes) => {
            // KV 响应反序列化
            let r: kv::Response = kv::decode(&bytes).unwrap();
            // 跨节点读到相同提交值
            assert_eq!(r, kv::Response::Get(Some("3nodes".into())));
        // Read 分支结束
        }
        // 其它类型失败
        other => panic!("{other:?}"),
    // match 结束
    }

    // 清理全部子进程
    for n in &mut nodes {
        // 逐个强制终止，避免 Drop 顺序依赖
        let _ = n.child.kill();
    // for 结束
    }
// 三节点测例结束
}
