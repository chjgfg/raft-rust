//! 确定性、无线程的节点驱动。

// 节点集合、出站邮箱队列
use std::collections::{HashMap, HashSet, VecDeque};

// 节点出站通道（同步，便于 try_recv 排空）
use crossbeam::channel;
// 状态机
use raft_rust::raft::kv::Kv;
// 核心 Node 与消息信封
use raft_rust::raft::{Envelope, Log, Node, NodeID, Options};
// 持久化引擎
use raft_rust::storage::BitCask;

// 确定性 harness：无真实网络/时钟，靠 tick + 手动 deliver 推进
struct Harness {
    // 各节点当前角色状态机
    nodes: HashMap<NodeID, Node>,
    // 各节点出站通道接收端
    rxs: HashMap<NodeID, channel::Receiver<Envelope>>,
    // 待投递入站邮箱（模拟网络缓冲）
    mailboxes: HashMap<NodeID, VecDeque<Envelope>>,
// Harness 字段定义结束
}

// 实现确定性集群驱动方法
impl Harness {
    // 为给定 id 列表构造全互连集群，共享 Options
    fn new(ids: &[NodeID], opts: Options) -> Self {
        // 节点 id → 状态机实例
        let mut nodes = HashMap::new();
        // 节点 id → 出站接收端
        let mut rxs = HashMap::new();
        // 节点 id → 待投递入站队列
        let mut mailboxes = HashMap::new();
        // 路径序号，避免并行测试日志文件冲突
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        // 逐个 id 初始化独立 Node 与邮箱
        for &id in ids {
            // 除自己外的全部 peer 作为初始投票者配置
            let peers: HashSet<NodeID> = ids.iter().copied().filter(|&p| p != id).collect();
            // 无界通道承载 Node 发出的 Envelope
            let (tx, rx) = channel::unbounded();
            // 原子递增保证并行用例路径唯一
            let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // 每节点独立临时 BitCask 路径
            let path = std::env::temp_dir().join(format!(
                // 进程号+节点+序号+纳秒组成唯一文件名
                "raft-nu-{}-{}-{}-{}.log",
                // 当前进程 id 隔离不同测试进程
                std::process::id(),
                // 节点 id 区分同进程内节点
                id,
                // 全局序号防并行冲突
                seq,
                // 纳秒时间戳再加一层唯一性
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
            // 拼好路径后交给 BitCask
            ));
            // 日志层包一层 BitCask
            let log = Log::new(Box::new(BitCask::new(path).unwrap())).unwrap();
            // 构造 Follower（或单节点 Leader）
            let node = Node::new(id, peers, log, Kv::new(), tx, opts.clone()).unwrap();
            // 登记状态机
            nodes.insert(id, node);
            // 登记出站接收端
            rxs.insert(id, rx);
            // 空邮箱待收消息
            mailboxes.insert(id, VecDeque::new());
        // 所有节点初始化完成
        }
        // 组装 harness 三表
        Self { nodes, rxs, mailboxes }
    // new 结束
    }

    // 把各节点出站消息放入目标邮箱（丢弃自发自收环回）
    fn drain_outboxes(&mut self) {
        // 遍历每个节点的出站接收端
        for (&id, rx) in &self.rxs {
            // 非阻塞排空该节点当前积压
            while let Ok(msg) = rx.try_recv() {
                // 忽略发往自己的消息，避免自环干扰
                if msg.to == id {
                    // 跳过环回，不入邮箱
                    continue;
                // 非环回则继续投递
                }
                // 按 msg.to 入队，模拟网络投递缓冲
                self.mailboxes.get_mut(&msg.to).unwrap().push_back(msg);
            // 当前节点出站已空
            }
        // 全部节点出站已 drain
        }
    // drain_outboxes 结束
    }

    // 从任一邮箱弹出一条消息并 step，返回是否投递成功
    fn deliver_one(&mut self) -> bool {
        // 拷贝 key 列表，避免与 mut 借用冲突
        for id in self.mailboxes.keys().copied().collect::<Vec<_>>() {
            // 优先从该节点邮箱取队首消息
            if let Some(msg) = self.mailboxes.get_mut(&id).unwrap().pop_front() {
                // take 节点所有权再 step（Node 为状态机枚举）
                let node = self.nodes.remove(&id).unwrap();
                // 用 step 驱动状态机消费该消息
                let node = match node.step(msg) {
                    // 正常转移后的新角色
                    Ok(n) => n,
                    // 测试环境遇错直接 panic 暴露问题
                    Err(e) => panic!("step error on {id}: {e}"),
                // match 结束，得到更新后的 Node
                };
                // 写回更新后的状态机
                self.nodes.insert(id, node);
                // step 可能产生新出站消息
                self.drain_outboxes();
                // 已成功投递并处理一条
                return true;
            // 该邮箱为空则试下一个
            }
        // 所有邮箱皆空
        }
        // 无消息可投
        false
    // deliver_one 结束
    }

    // 排空所有邮箱直至无待投递消息（同步收敛一轮）
    fn deliver_all(&mut self) {
        // 反复 deliver_one 直到返回 false
        while self.deliver_one() {}
    // deliver_all 结束
    }

    // 对单个节点 tick 一次（推进选举/心跳计时），并投递产生的消息
    fn tick(&mut self, id: NodeID) {
        // 取出节点所有权以调用 tick
        let node = self.nodes.remove(&id).unwrap();
        // 推进一次选举/心跳计时器
        let node = node.tick().unwrap();
        // 写回 tick 后的状态
        self.nodes.insert(id, node);
        // 先把 tick 产生的出站消息入邮箱
        self.drain_outboxes();
        // 再同步投递直至收敛
        self.deliver_all();
    // tick 结束
    }

    // 所有节点各 tick 一次，再统一投递（模拟全局时钟步进）
    fn tick_all(&mut self) {
        // 先拷贝 id 列表，避免边改边借
        for id in self.nodes.keys().copied().collect::<Vec<_>>() {
            // 取出该节点
            let node = self.nodes.remove(&id).unwrap();
            // 全局时钟下各节点同步推进一拍
            let node = node.tick().unwrap();
            // 写回
            self.nodes.insert(id, node);
        // 全部节点已各自 tick 一次
        }
        // 统一收集本轮出站
        self.drain_outboxes();
        // 统一投递至收敛
        self.deliver_all();
    // tick_all 结束
    }

    // 收集当前处于 Leader 角色的节点 id
    fn leaders(&self) -> Vec<NodeID> {
        // 遍历节点表筛选 Leader 变体
        self.nodes
            // 引用遍历 (id, node)
            .iter()
            // 仅保留 Leader 的 id
            .filter_map(|(id, n)| match n {
                // 匹配 Leader 枚举臂
                Node::Leader(_) => Some(*id),
                // Follower/Candidate 丢弃
                _ => None,
            // filter_map 闭包结束
            })
            // 收集为 Vec
            .collect()
    // leaders 结束
    }

    // 调试用：返回节点角色名字符串
    fn role(&self, id: NodeID) -> &'static str {
        // 按枚举变体映射可读角色名
        match &self.nodes[&id] {
            // 跟随者
            Node::Follower(_) => "follower",
            // 候选人
            Node::Candidate(_) => "candidate",
            // 领导者
            Node::Leader(_) => "leader",
        // match 结束
        }
    // role 结束
    }
// Harness impl 结束
}

// 构造测试 Options；prevote 开关可切换预投票
fn opts(prevote: bool) -> Options {
    // 结构化字面量填充选举相关参数
    Options {
        // 心跳间隔 2 tick
        heartbeat_interval: 2,
        // 宽范围，配合错开 tick
        election_timeout_range: 5..6, // 固定 5
        // 单次 AppendEntries 批量上限
        max_append_entries: 100,
        // 由参数决定是否走预投票
        pre_vote: prevote,
        // 单元驱动测选举，关闭 check_quorum 降噪
        check_quorum: false,
        // 不触发快照
        snapshot_threshold: 0,
    // Options 字面量结束
    }
// opts 结束
}

// 关闭 pre-vote：仅让节点 1 超时竞选，验证可稳定成为唯一领导者
#[test]
// 错开超时选举（无 pre-vote）
fn staggered_election_without_prevote() {
    // 三节点集群，关闭预投票
    let mut h = Harness::new(&[1, 2, 3], opts(false));
    // 只让节点 1 超时并竞选；2/3 仍是 follower 会投票
    for _ in 0..5 {
        // 5 次 tick 达到固定 election_timeout=5
        h.tick(1);
    // 节点 1 超时循环结束
    }
    // 节点 1 应拿到多数票成为领导
    assert!(
        // 断言领导者集合含节点 1
        h.leaders().contains(&1),
        // 失败时打印三角色与任期便于排查
        "node1 should be leader, roles: 1={} 2={} 3={} terms: {} {} {}",
        // 各节点当前角色字符串
        h.role(1), h.role(2), h.role(3),
        // 各节点当前任期
        h.nodes[&1].term(), h.nodes[&2].term(), h.nodes[&3].term()
    // assert 结束
    );
// 无 prevote 选举用例结束
}

// 开启 pre-vote：错开 tick 后最终仍应选出领导者（可能多一轮消息）
#[test]
// 错开超时选举（有 pre-vote）
fn staggered_election_with_prevote() {
    // 三节点集群，开启预投票
    let mut h = Harness::new(&[1, 2, 3], opts(true));
    // 先让节点 1 连续 tick 触发 pre-vote
    for _ in 0..5 {
        // 仅推进节点 1 至 pre-vote 超时
        h.tick(1);
    // 节点 1 预投票触发循环结束
    }
    // pre-vote + real election 可能需要多一轮 deliver；再补几 tick
    for _ in 0..5 {
        // 全局 tick 推进其它节点与后续真实选举
        h.tick_all();
        // 任一节点成为领导即可
        if h.leaders().contains(&1) || !h.leaders().is_empty() {
            // 已有主则提前退出补 tick
            break;
        // 尚无主则继续下一轮全局 tick
        }
    // 补 tick 循环结束
    }
    // 集群不得长期无主
    assert!(
        // 至少应选出一名领导者
        !h.leaders().is_empty(),
        // 失败时打印角色与任期
        "should elect a leader, roles: 1={} 2={} 3={} terms: {} {} {}",
        // 角色快照
        h.role(1), h.role(2), h.role(3),
        // 任期快照
        h.nodes[&1].term(), h.nodes[&2].term(), h.nodes[&3].term()
    // assert 结束
    );
// 有 prevote 选举用例结束
}

// 单节点集群创建即为领导，tick 后仍保持领导
#[test]
// 单节点自选举与 check_quorum 兼容
fn single_node_leader() {
    // 打开 pre_vote/check_quorum 也不应阻碍单节点
    let opts = Options {
        // 即使开预投票，无 peer 也应直接当选
        pre_vote: true,
        // 开启法定人数检查，验证单节点不会误降级
        check_quorum: true,
        // 关闭快照避免干扰角色断言
        snapshot_threshold: 0,
        // 其余字段走默认
        ..Options::default()
    // Options 覆盖结束
    };
    // 仅一个节点的 harness
    let mut h = Harness::new(&[1], opts);
    // 无 peer 时 Node::new 直接成为 Leader
    assert_eq!(h.leaders(), vec![1]);
    // tick 不应因 check_quorum 误 step down（单节点法定人数为 1）
    h.tick(1);
    // tick 后仍应是唯一领导者
    assert_eq!(h.leaders(), vec![1]);
// 单节点领导用例结束
}
