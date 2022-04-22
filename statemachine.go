package raft

type StateMachine interface {
	// Save 读取状态机快照
	Save() ([]byte, error)

	// Recovery 从快照中恢复状态机状态
	Recovery([]byte) error

	// Apply 状态机执行命令的回调函数
	Apply(commandName string, command []byte)
}
