package raft

import "github.com/treeforest/raft/pb"

type AppendEntriesResponse struct {
	*pb.AppendEntriesResponse
	Id uint64
}
