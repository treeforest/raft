package raft

type Raft interface {
	Start() error
	Stop()
	Join(existing string)
	Do(commandName string, command []byte) (uint64, error)
	Running() bool
	IsLeader() bool
	LeaderId() uint64
	LeaderAddress() string
}
