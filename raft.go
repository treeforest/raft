package raft

type Raft interface {
	Start() error
	Stop()
	Join(existing []string)
	Running() bool
	IsLeader() bool
	LeaderId() uint64
	LeaderAddress() string
}
