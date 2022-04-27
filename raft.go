package raft

type Raft interface {
	// Start 启动节点
	Start() error

	// Stop 暂停节点
	Stop()

	// Join 通过集群中的节点加入集群
	// 参数：
	// 	existing：集群中某个节点的地址
	Join(existing string)

	// Do 客户端需要执行的命令
	// 参数：
	// 	commandName: 命令名
	//	command: 命令数据
	Do(commandName string, command []byte) (done <-chan error)

	// TakeSnapshot 生成快照
	TakeSnapshot() error

	// LoadSnapshot 从快照中恢复状态
	LoadSnapshot() error

	// Running 节点运行状态
	Running() bool

	// IsLeader 当前节点是否是leader
	IsLeader() bool

	// LeaderId 集群中leader的id
	LeaderId() uint64

	// LeaderAddress 集群中leader的地址
	LeaderAddress() string

	// CurrentTerm 当前任期
	CurrentTerm() uint64

	// CurrentIndex 当前最新日志条目的索引
	CurrentIndex() uint64
}
