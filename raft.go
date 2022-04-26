package raft

type Raft interface {
	Start() error
	Stop()
	Join(existing string)
	Do(commandName string, command []byte) (uint64, error)
	TakeSnapshot() error
	LoadSnapshot() error
	Running() bool
	IsLeader() bool
	LeaderId() uint64
	LeaderAddress() string
	CurrentTerm() uint64
	CurrentIndex() uint64
}
