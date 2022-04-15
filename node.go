package raft

import (
	"context"
	"github.com/treeforest/raft/pb"
)

type raftImpl struct {
	pb.UnimplementedRaftServer
	currentTerm uint64
}

func (r *raftImpl) RequestVote(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteResp, error) {
	resp := &pb.RequestVoteResp{}
	return resp, nil
}
func (r *raftImpl) AppendEntries(ctx context.Context, req *pb.AppendEntriesReq) (*pb.AppendEntriesResp, error) {
	resp := &pb.AppendEntriesResp{Success: false}

	switch {
	case req.GetTerm() < r.currentTerm:
		break

	}

	if {

	}

	return resp, nil
}
func (r *raftImpl) Heartbeat(ctx context.Context, req *pb.HeartbeatReq) (*pb.HeartbeatResp, error) {
	resp := &pb.HeartbeatResp{}
	return resp, nil
}
