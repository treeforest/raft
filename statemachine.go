package raft

type StateMachine interface {
	// Save 读取状态机快照
	Save() ([]byte, error)

	// Recovery 从快照中恢复状态机状态
	Recovery([]byte) error
}