package raft

// ReplicationType 日志复制类型
type ReplicationType int

const (
	// Synchronous 同步复制。leader执行完一个写请求后，必须等待大于1/2的follower
	// 都执行完毕，并收到确认后，才回复客户端写入成功。
	Synchronous ReplicationType = iota

	// Asynchronous 异步复制。leader执行完写请求后，会立即将结果返回给客户端，
	// 无须等待其它副本是否写入完成。
	Asynchronous

	// Semisynchronous 半同步复制。介于同步复制与异步复制之间一种复制机制。leader只
	// 需要等待一个follower执行完毕并返回确认信息，不需要等待大于1/2的follower都完成。
	// 保证至少有两个节点拥有最新的数据副本。
	Semisynchronous
)
