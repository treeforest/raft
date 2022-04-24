package raft

import "github.com/treeforest/raft/pb"

type Raft interface {
	Start() error
	Stop()
	Join(existing string)
	Do(commandName string, command []byte) (*pb.LogEntry, error)
	Running() bool
	IsLeader() bool
	LeaderId() uint64
	LeaderAddress() string
}
