# TinyKV 实验小结
## 实验概述
本实验在给定的框架代码的基础上, 实现基于 Raft 共识算法和 Percolator 算法的高可用水平扩展键值对存储系统. 共分为四个 Project:
* Project 1: 实现单机键值对存储服务
* Project 2: 实现 Raft 共识算法的基础功能, 将 Raft 算法与 Raftstore 对接, 增加 Snapshot 支持
* Project 3: 在 Raft 算法中增加 Leader Transfer, Add Node, Remove Node 的支持, 在 Raftstore 中增加 TransferLeader, ChangePeer, Region Split 支持, 实现调度器收集 Heartbeat 和Region 平衡的功能.
* Project 4: 实现 MVCC 事务接口和 KvGet, KvPrewrite, KvCommit 等接口
## 实验思路
* Project 1

  此Project较容易上手. 主要是实现 StandAloneStorage 和 RawGet, RawPut, RawScan, RawDelete. StandAloneStorage 作为 Storage 接口的实现, 需要提供 Start, Stop, Reader, Write 等接口. 由于 Reader 方法会返回一个 StorageReader, 因此需要实现一个 StandAloneStorageReader, 并提供 GetCF, IterCF, Close 等方法. 运用 engine_util 提供的接口后, 这些功能较为容易实现. 而 RawGet, RawPut, RawScan, RawDelete 等调用 Storage 和 StorageReader 提供的接口即可完成.
* Project 2
  * Project 2A

    Project 2A 主要是实现 Raft 共识算法的基础功能. 主要对照 In Search of an Understandable Consensus Algorithm 的 Figure 2 和 论文的第 5 部分的讲解即可. 实现中需要注意的点可以参考 6824 Lab 2 中给出的链接. 与 6824 的 Raft 相比, 本实验中的 Raft 中避免了互斥锁的使用, 并且将算法输入和输出封装为 Message, Message 的输入会改变 Raft 状态机, 并产生新的 Message. 而持久化, Message 收发, 外部状态机等处理工作则会在后续实验中展开.
    
    * Project 2AA
      * 本部分主要完成 Leader 选举相关工作. 首先从 tick 函数开始, 实现超时后发送心跳/转为 Candidate. 接着可以完成 becomeFollower 等节点身份转换的方法, 以及 newRaft 初始化 Raft 状态机的函数. 接着则是与 RequestVote RPC, Heartbeat RPC 相关的方法的实现. RaftLog中也有需要对应实现的部分. 一些注意事项:
        * 所有节点信息存在 Prs 中而不再另外存放.
        * handleRequestVote 中注意 Raft 论文中对 up-to-date 的定义
        * applied 信息优先从 config 中获取, 如果不存在则设为 RaftLog 的首个下标, committed 直接从 hardState 中读取
        * 在发送 RequestVote 前进行一轮计票, 在只有 1 个节点时直接成为 Leader
        * 收到 Request Vote Response 时统计得票情况, 统计投赞成和反对票的信息, 如果反对票超过半数则转为 Follower
    * Project 2AB
      * 本部分主要完成 AppendEntries 相关的实现. 对照 Figure 2 和 5.3, 5.4 中给出的相关要求实现即可. 一些注意事项:
        * 领导人完全性. Figure 8 中的案例体现了这个问题, 对应的解决措施为 Leader 只 Commit 当前 Term 的日志, 因此 Leader 在收到肯定的 AppendEntries RPC 响应后计算新 Commit 位置时应该注意这些问题.
        * Commit, Applied 的下标应该是只能单增的, 在对这些下标修改时应该注意这些问题.
        * 完善 log.go 中操作日志的相关工具函数, 会在后续实现 Snapshot 时带来极大的便利.
        * Follower 更新 commit 下标时应按照论文的要求, 即 `commitIndex = min(leaderCommit, index of last NEW entry)`
        * 可以增加快速回退功能. 我在实现时选用的是 6824 中给出的回退方案, 即通过 XLen, XIndex, XTerm 提高回退的幅度. 但在结合 Snapshot 实现时可能会出现回退到 lastIncludedIndex 前的情况, 因此后续增加了不能回退到此节点的 commitIndex 之前的设定.
        * 取 commitIndex 时应超过半数, 特别是节点个数为偶数时 
    * Project 2AC
      * 本部分主要完成 RawNode 相关接口的设计, 主要为 Ready 和 Advance 相关接口的设计. 一些注意事项:
        * 在 RawNode 中存储之前的 HardState 和 Softstate, 便于判断是否需要在 Ready 中返回 HardState 和 Softstate
        * 比较 HardState 是否相等的接口在 util.go 中有所体现.
  * Project 2B
  
    本部分开始需要和 Raftstore, TinyKV 的核心组件接触. 本组件结构较为复杂, 不过在本部分实验仅与 RaftWorker 接触. 除实验讲义外还可以阅读 PingCAP 提供的 `TiKV 源码解析` 对应部分. 虽然具体源码均为 Rust, 但架构是有相通之处的. 如果直接阅读全部 Raftstore 源码可能较为困难, 可以在阅读 RaftWorker, 完成了一部分代码工作后继续阅读其他部分. 对于 Store, Peer, Region 的定义需要熟练掌握, 之后会多次使用.

    实验中首先需要实现 PeerStorage 相关函数, 用于持久化 Ready. Append 待持久化的 Log 数据时, 直接按照 Key 的要求写入 WriteBatch, 更新 RaftState 即可, 应该不需要删除存储的日志.  SaveRaftState 函数中则调用 Append 函数, 再根据 Ready 信息修改 RaftState 即可, 后续增加 Snapshot 时再增加对 applyState, regionLocalState 的修改. 本部分的测试似乎不能通过`make project2b`调用, 但可以手工调用`peer_storage_test.go`中的测试函数.

    接着是对`peer_msg_handler.go`的修改. 修改之前需要详细阅读实验指导书中对 RaftStorage 的使用过程的解析, 并能结合代码框架加以理解. 实验中主要修改的是 proposeRaftCommand 函数和 handleRaftReady 函数. proposeRaftCommand 函数目前仅需要记录 callback, 再将消息序列化调用 RawNode.Propose 即可. handleRaftReady 函数的执行步骤在源码解析中有给出, 主要为 SaveReadyState, 发送 Message, 应用 CommittedEntries, 保存 ApplyState, Advance四个步骤. 除了应用 CommittedEntries,其他步骤都有辅助函数. 应用时主要步骤为先找 callback, 再根据 Request 对数据库进行修改.

    使用过 callback 后注意清除 callback, 否则在3B destroyPeer 时会出现 channel 阻塞的情况.
  * Project 2C
    
    本部分主要是增加 Snapshot 支持, Snapshot 支持分为两部分, 首先是 Raft 中的相关 RPC. 这一部分可以参考 Diego 的博士论文, 在发送 AppendEntries RPC 前, 我们如果判断NextIndex小于当前的日志下标, 则发送 Snapshot. handleSnapshot 和 handleAppendEntries 相似, 但为避免 commit 的回退, 只有当 snapshot 的最后下标大于 committed 时才处理 snapshot, 处理时类似新启动 Raft 时的操作, 将commit, appied, stabled 全部重设, 日志的首个 index 和 term 与 snapshot 的最后 index 和 term 相同, 对日志的处理参考论文描述. 由于 snapshot 中携带节点信息, 且这部分信息经过 checkSnapshot 核验过, 因此节点信息需要无条件重设.

    PeerStorage 中首先需要增加 snapshot 保存, 具体流程讲义有描述, 注意向 regionSched 传递 RegionTaskApply 时, StartKey 和 EndKey 设置为 snapshot 中的 Region.StartKey 和 Region.EndKey, 否则在 3B region split 中会出现删除过度的情况. 之后在 SaveReadyState 中根据 ApplySnapshot 的返回值按需写入 RegionLocalState.

    handleRaftReady 中增加对 AdminCommand 的支持. 处理 AdminCommand 应在 Request 之前. 处理 CompactLog 为避免 no need to gc 报错, 应将 CompactIndex 取为 min(CompactIndex, AppliedIndex).

* Project 3
  * Project 3A

    这部分主要在 Raft 算法上修改以实现 Transfer Leader 和 Config Change, 这部分可以参考 Diego 的博士论文. Transfer Leader 的实现较为简单, 注意处理 Transfer Leader 时在 Election Timeout 内未完成时需要取消 Transfer Leader 即可. Config Change 实现的是 One at a time 版本, 而不是 Joint Consensus 版本. 实现时, 注意 AddNode 时需要判定是否已经添加, RemoveNode 时判断是否已经删除. 同时, 由于 Majority Quorum 发生了变化, 在 RemoveNode 中 Leader 节点需要尝试更新 commitIndex

  * Project 3B

    这部分实现的难度依赖于之前的实现的准确性, 如果准确度较好则很轻松, 否则需要在 Raft, Worker, Msg Handler 等多部分打 Log 以 Debug.

    首先是 Transfer Leader, 这部分按照讲义要求即可. 

    然后是 Config Change, 首先需要仔细阅读并理解处理流程, 其次是需要在 Raft 中增加 PendingConfIndex 的判断和处理, 并且需要在 Raft 初始化和当选 Leader 时增加 PendingConfIndex 的处理和更新, 以实现在当前的 ConfChange 应用之前不 Propose 新的 ConfChange. 在选举时需要在 `len(Prs) == 0` 时禁止当选, 以避免复制的节点当选 Leader 然后 panic. 在新节点应用 snapshot 后, 我们需要在 storeMeta 中更新 regionMap 和 regionBtree, 在 store 层面体现 region 的存在, 并避免 meta corruption.

    Region Split 部分按照讲义的描述, 完成对 region 信息的修改, 持久化, 在 storeMeta 中设置, 调用 createPeer, router 中注册即可. 在实现此部分时注意各种报错信息的处理, 如在 Split 前检测 SplitKey 是否在 Region 中, 每条请求的 Region Epoch 和 Key 是否在 Region 中.

    实现过程中除了实验讲义外, 同样可以参考 `TiKV 源码解析` 系列

  * Project 3C
    
    这部分讲义的描述很详细, 按照讲义实现即可. 在处理 Region Heartbeat 前检查 ConfVer 和 Version 是否落后, 之后更新 region 和 storeStatus. 在处理 Region Balance 时注意 Target Store 中应该不含有这个 Region.

* Project 4
  
  这部分主要实现 Percolator 事务算法, 对此算法的讲解除了 Google Percolator 论文外, 还可以参考 TiKV 源码解析, 以及 [TiKV Deep Dive](https://tikv.org/deep-dive/distributed-transaction/percolator/) 中的讲解.

  Project 4 共分为 3 个部分. 第一部分实现 MVCC Txn API 能够参考测试用例实现. 第二部分和第三部分实现时参考 [TiKV Deep Dive](https://tikv.org/deep-dive/distributed-transaction/percolator/), [TiKV 源码解析系列文章（十二）分布式事务](https://pingcap.com/blog-cn/tikv-source-code-reading-12), [TiKV 源码解析系列文章（十三）MVCC 数据读取](https://pingcap.com/blog-cn/tikv-source-code-reading-13)即可完成. 
    
## 实验评估
* 由于在2B, 2C, 3B 中 make projectX 所需内存过大, 在本机上无法完成测试, 我选择了提取测试函数, 编写简易 Bash 脚本以逐个运行测试用例的方式进行测试. 同时, 对 2B, 2C, 3B 中的最后一个测试点, 我分别运行了 30 次, 以观测实现的准确性. 测试日志随 Write Up 附带.
* 而对于其他测试点, 我采用 make projectX 的方式完成测试. 

## 实验总结
* 通过本次实验, 我学到关于分布式的键值对存储系统的更多知识. 并通过实现 Raft 共识算法和 Percolator 分布式事务算法, 对这两种算法有更深的理解. 在完善 PeerStorage 和 Peer Msg Handler 的过程中, 我对 Raftstore 的基本架构, 以及 Go Channel, Interface 等语言特性的使用方式有初步的认识. 
* 在完成实验的过程中, 我提出了一个修改GOPATH获取方式的小[PR](https://github.com/tidb-incubator/tinykv/pull/262), 体会到参与开源社区的流程.