package raft

import (
	"context"
	"errors"
	log "github.com/treeforest/logger"
	"github.com/treeforest/raft/pb"
	"io/ioutil"
	"sort"
	"sync"
	"time"
)

var (
	StopError      = errors.New("stopped")
	NotLeaderError = errors.New("not leader")
)

type server struct {
	pb.UnimplementedRaftServer
	config          *Config
	stateMachine    StateMachine
	pendingSnapshot *pb.Snapshot
	snapshot        *pb.Snapshot

	// persistent state on all servers
	// currentTerm server的当前任期
	currentTerm uint64
	// votedFor 在当前任期内获得选票的候选人ID(candidateId)
	votedFor uint64
	// log 日志
	log *Log
	// logs []*pb.LogEntry

	// volatile state on all servers
	// commitIndex 最后commit的日志所对应的索引，从0递增
	commitIndex uint64
	// lastApplied 被状态机执行的最后一条日志条目，从0递增;
	// 当lastApplied小于commitIndex时，状态机执行小于commitIndex的日志条目
	lastApplied uint64
	// server id
	id uint64

	// volatile state on leader
	// nextIndex 发送给其它server的下一条日志索引, id -> index
	nextIndex sync.Map
	// matchIndex 已知在其它server上复制的最新日志条目的索引， id -> index
	matchIndex sync.Map

	// 自定义变量
	// members 集群中的成员， id -> member
	members               sync.Map
	startHeartbeat        chan struct{}
	stopHeartbeat         chan struct{}
	locker                sync.RWMutex
	state                 pb.NodeState
	c                     chan *ev
	stopped               chan struct{}
	routineGroup          sync.WaitGroup
	leader                uint64
	followerLoopSince     time.Time
	followerUpdateChan    chan bool
	appendEntriesRespChan chan *pb.AppendEntriesResponse // leader loop use
}

// ev 事件循环所处理的事件
type ev struct {
	target      interface{}
	returnValue interface{}
	c           chan error
}

func (s *server) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (
	resp *pb.RequestVoteResponse, err error) {
	currentTerm := s.CurrentTerm()
	resp = &pb.RequestVoteResponse{
		Term:        currentTerm,
		VoteGranted: false,
	}

	if req.Term < currentTerm {
		return
	}

	if req.Term > currentTerm {
		s.updateCurrentTerm(req.Term, 0)
	} else if s.votedFor != 0 && s.votedFor != req.CandidateId {
		return
	}

	lastIndex, lastTerm := s.log.lastInfo()
	if lastIndex > req.LastLogIndex || lastTerm > req.LastLogTerm {
		return
	}

	s.followerUpdateChan <- true
	s.votedFor = req.CandidateId
	resp.VoteGranted = true
	return
}

// AppendEntries 用于leader进行日志复制与心跳检测
func (s *server) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (
	resp *pb.AppendEntriesResponse, err error) {
	log.Debugf("AppendEntries, request: %v", *req)

	if !s.Running() {
		return nil, StopError
	}

	resp = &pb.AppendEntriesResponse{
		Term:        s.currentTerm,
		Index:       s.log.currentIndex(),
		CommitIndex: s.log.CommitIndex(),
		Success:     false,
	}

	// 1、reply false if term < currentTerm
	currentTerm := s.CurrentTerm()
	if req.Term < currentTerm {
		log.Warn("stale term")
		return resp, nil
	} else if req.Term == currentTerm {
		if s.state == pb.NodeState_Candidate {
			s.setState(pb.NodeState_Follower)
		}
		s.leader = req.LeaderId
	} else {
		// update term and leader
		s.updateCurrentTerm(req.Term, req.LeaderId)
	}

	s.followerUpdateChan <- true

	// 2、reply false if log doesn't contain an Entry at prevLogIndex whose
	// term matches prevLogTerm
	if err = s.log.truncate(req.PrevLogIndex, req.PrevLogTerm); err != nil {
		log.Warn(err)
		return resp, nil
	}

	// 3、if an existing Entry conflicts with a new one(same index but
	// different terms), delete the existing Entry and all the follow it.
	// 4、append any new entries not already in the log
	if err = s.log.appendEntries(req.Entries); err != nil {
		log.Warn(err)
		return resp, nil
	}

	if err = s.log.setCommitIndex(req.CommitIndex); err != nil {
		log.Warn(err)
		return resp, nil
	}

	// 若返回成功，则代表leader和follower的日志条目一定相同
	resp.Success = true
	return resp, nil
}

func (s *server) Snapshot(ctx context.Context, req *pb.SnapshotRequest) (
	resp *pb.SnapshotResponse, err error) {
	resp = &pb.SnapshotResponse{Success: false}

	entry := s.log.getEntry(req.LastIndex)
	if entry != nil && entry.Term == req.LastTerm {
		return
	}

	s.setState(pb.NodeState_Snapshotting)
	resp.Success = true
	return
}

func (s *server) SnapshotRecovery(ctx context.Context, req *pb.SnapshotRecoveryRequest) (
	resp *pb.SnapshotRecoveryResponse, err error) {

	if err = s.stateMachine.Recovery(req.State); err != nil {
		log.Fatal("cannot recover from previous state")
	}

	s.setCurrentTerm(req.LastTerm)
	s.log.updateCommitIndex(req.LastIndex)

	// add member
	for _, mb := range req.Members {
		s.AddMember(&mb)
	}

	// 创建本地快照
	s.pendingSnapshot = &pb.Snapshot{
		LastIndex: req.LastIndex,
		LastTerm:  req.LastTerm,
		State:     req.State,
	}
	data, _ := s.pendingSnapshot.Marshal()
	err = ioutil.WriteFile(s.config.SnapshotPath, data, 777)
	if err == nil {
		s.snapshot = s.pendingSnapshot
		s.pendingSnapshot = nil
	}

	// 清楚 LastIncludedIndex 之前的条目
	_ = s.log.compact(req.LastIndex, req.LastTerm)

	resp = &pb.SnapshotRecoveryResponse{
		Term:        s.CurrentTerm(),
		Success:     true,
		CommitIndex: s.log.CommitIndex(),
	}

	return
}

// Membership 成员信息
func (s *server) Membership(ctx context.Context, req *pb.MembershipRequest) (
	*pb.MembershipResponse, error) {
	if value, ok := s.members.Load(req.Member.Id); !ok {
		s.members.Store(req.Member.Id, newMember(req.Member))
	} else {
		m := value.(*member)
		if m.Address != req.Member.Address {
			m.Address = req.Member.Address
			m.cc = nil
			s.members.Store(req.Member.Id, m)
		}
	}

	resp := &pb.MembershipResponse{}
	members := make([]pb.Member, 0)

	s.members.Range(func(key, value interface{}) bool {
		m := value.(*member)
		members = append(members, m.Member)
		return true
	})

	resp.Members = members
	return resp, nil
}

func (s *server) Stop() {
	if s.state == pb.NodeState_Stopped {
		return
	}

	// 结束运行的事件循环
	close(s.stopped)

	// 等待协程运行结束
	s.routineGroup.Wait()

	s.log.close()
	s.setState(pb.NodeState_Stopped)
}

// setCurrentTerm 设置当前任期
func (s *server) setCurrentTerm(term uint64) {
	s.locker.Lock()
	defer s.locker.Unlock()
	s.currentTerm = term
}

// CurrentTerm 获取的当前任期
func (s *server) CurrentTerm() uint64 {
	s.locker.RLock()
	defer s.locker.RUnlock()
	return s.currentTerm
}

// Running 是否是运行状态
func (s *server) Running() bool {
	s.locker.RLock()
	defer s.locker.RUnlock()
	return s.state != pb.NodeState_Stopped && s.state != pb.NodeState_Initialized
}

// State 当前状态
func (s *server) State() pb.NodeState {
	s.locker.RLock()
	defer s.locker.RUnlock()
	return s.state
}

// setState 设置状态
func (s *server) setState(state pb.NodeState) {
	s.locker.Lock()
	defer s.locker.Unlock()
	s.state = state
}

// Id 当前server的id
func (s *server) Id() uint64 {
	return s.id
}

// updateCurrentTerm 更新任期
func (s *server) updateCurrentTerm(term uint64, leaderId uint64) {
	if s.state == pb.NodeState_Leader {
		// 如果是leader，则停止所有心跳
		s.members.Range(func(key, value interface{}) bool {
			m := value.(*member)
			m.stopHeartbeat(false)
			return true
		})
	}

	if s.state != pb.NodeState_Follower {
		s.setState(pb.NodeState_Follower)
	}

	// 更新状态
	s.locker.Lock()
	s.currentTerm = term
	s.leader = leaderId
	s.votedFor = 0
	s.locker.Unlock()
}

//func (s *server) send(value interface{}) (interface{}, error) {
//	if !s.Running() {
//		return nil, StopError
//	}
//
//	e := &ev{target: value, c: make(chan error, 1)}
//	select {
//	case s.c <- e:
//	case <-s.stopped:
//		return nil, StopError
//	}
//	select {
//	case <-s.stopped:
//		return nil, StopError
//	case err := <-e.c:
//		return e.returnValue, err
//	}
//}
//
//func (s *server) sendAsync(value interface{}) {
//	if !s.Running() {
//		return
//	}
//
//	e := &ev{target: value, c: make(chan error, 1)}
//	select {
//	case s.c <- e:
//		return
//	default:
//		// s.c 通道阻塞，下面采用协程的方式避免阻塞的发送
//	}
//
//	s.routineGroup.Add(1)
//	go func() {
//		defer s.routineGroup.Done()
//		select {
//		case s.c <- e:
//		case <-s.stopped:
//		}
//	}()
//}

// loop 事件循环
func (s *server) loop() {
	state := s.State()
	for state != pb.NodeState_Stopped {
		log.Debugf("loop, state=%s", state.String())
		switch state {
		case pb.NodeState_Follower:
			s.followerLoop()
		case pb.NodeState_Candidate:
			s.candidateLoop()
		case pb.NodeState_Leader:
			s.leaderLoop()
		case pb.NodeState_Snapshotting:
			s.snapshotLoop()
		}
		state = s.State()
	}
}

func (s *server) followerLoop() {
	s.followerLoopSince = time.Now()
	electionTimeout := s.config.ElectionTimeout
	timeoutChan := afterBetween(electionTimeout, electionTimeout*2)
	update := false

	for s.State() == pb.NodeState_Follower {
		update = false
		select {
		case <-s.stopped:
			s.setState(pb.NodeState_Stopped)
			return
		case update = <-s.followerUpdateChan:
		case <-timeoutChan:
			// 超时未收到leader的心跳消息
			if s.log.currentIndex() > 0 {
				// 从follower转变成candidate
				s.setState(pb.NodeState_Candidate)
			} else {
				update = true
			}
		}

		if update {
			// 在超时时间内收到了server的心跳消息，重置超时时间
			s.locker.Lock()
			s.followerLoopSince = time.Now()
			s.locker.Unlock()
			timeoutChan = afterBetween(electionTimeout, electionTimeout*2)
		}
	}
}

func (s *server) candidateLoop() {
	s.leader = 0

	lastLogIndex, lastLogTerm := s.log.lastInfo()
	doVote := true
	votesGranted := 0
	var timeoutChan <-chan time.Time
	var respChan chan *pb.RequestVoteResponse

	for s.State() == pb.NodeState_Candidate {
		if doVote {
			// 任期加一
			s.currentTerm++

			// 自己的选票投给自己
			s.votedFor = s.id

			respChan = make(chan *pb.RequestVoteResponse, s.MemberCount())
			req := &pb.RequestVoteRequest{
				Term:         s.CurrentTerm(),
				CandidateId:  s.Id(),
				LastLogTerm:  lastLogTerm,
				LastLogIndex: lastLogIndex,
			}
			// 向连接的节点发送请求选票的消息
			s.members.Range(func(key, value interface{}) bool {
				s.routineGroup.Add(1)
				go func(m *member) {
					defer s.routineGroup.Done()
					_ = m.sendVoteRequest(req, respChan)
				}(value.(*member))
				return true
			})

			votesGranted = 1
			timeoutChan = afterBetween(s.config.ElectionTimeout, s.config.ElectionTimeout*2)
			doVote = false
		}

		if votesGranted == s.QuorumSize() {
			s.setState(pb.NodeState_Leader)
			return
		}

		// 收集选票
		select {
		case <-s.stopped:
			s.setState(pb.NodeState_Stopped)
			return
		case resp := <-respChan:
			if resp.VoteGranted && resp.Term == s.CurrentTerm() {
				votesGranted++
				break
			}
			if resp.Term > s.CurrentTerm() {
				s.updateCurrentTerm(resp.Term, 0)
			} else {
				// 节点拒绝了对当前节点的投票
			}
		case <-timeoutChan:
			// 选举超时，开始新一轮选举
			doVote = true
		}
	}
}

// leaderLoop
// 1、成员保活 2、发送日志条目 3、日志提交
func (s *server) leaderLoop() {
	logIndex, _ := s.log.lastInfo()

	memberCount := s.MemberCount()
	s.appendEntriesRespChan = make(chan *pb.AppendEntriesResponse, memberCount)
	s.members.Range(func(key, value interface{}) bool {
		m := value.(*member)
		m.setPrevLogIndex(logIndex)
		m.startHeartbeat()
		return true
	})

	for s.State() == pb.NodeState_Leader {
		select {
		case <-s.stopped:
			s.members.Range(func(key, value interface{}) bool {
				m := value.(*member)
				m.stopHeartbeat(false)
				return true
			})
			s.setState(pb.NodeState_Stopped)
			return

		case resp := <-s.appendEntriesRespChan:
			if resp.Term > s.CurrentTerm() {
				s.updateCurrentTerm(resp.Term, 0)
				break
			}
			if !resp.Success {
				break
			}
			if resp.Index <= s.log.CommitIndex() {
				break
			}

			// 检查超过半数的条件
			var indices []uint64
			indices = append(indices, s.log.currentIndex())
			s.members.Range(func(key, value interface{}) bool {
				indices = append(indices, value.(*member).getPrevLogIndex())
				return true
			})
			sort.Sort(sort.Reverse(Uint64Slice(indices)))

			commitIndex := indices[s.QuorumSize()-1]
			if commitIndex > s.log.CommitIndex() {
				s.locker.Lock()
				// 更新 commitIndex
				_ = s.log.setCommitIndex(commitIndex)
				s.locker.Unlock()
			}
		}
	}
}

func (s *server) snapshotLoop() {
	for s.State() == pb.NodeState_Snapshotting {
		select {
		case <-s.stopped:
			s.setState(pb.NodeState_Stopped)
			return
		default:
		}
	}
}

func (s *server) AddMember(m *pb.Member) {
	if s.Id() == m.Id {
		return
	}

	if _, ok := s.members.Load(m.Id); ok {
		return
	}

	mem := newMember(*m)
	if s.State() == pb.NodeState_Leader {
		mem.startHeartbeat()
	}

	s.members.Store(mem.Id, mem)
}

func (s *server) RemoveMember(id uint64) {
	if s.Id() == id {
		return
	}

	if v, ok := s.members.Load(id); ok {
		if s.State() == pb.NodeState_Leader {
			m := v.(*member)
			s.routineGroup.Add(1)
			go func() {
				defer s.routineGroup.Done()
				m.stopHeartbeat(true)
			}()
		}

		s.members.Delete(id)
	}
}

func (s *server) Members() []pb.Member {
	members := make([]pb.Member, 0)
	s.members.Range(func(key, value interface{}) bool {
		members = append(members, value.(*member).Member)
		return true
	})
	return members
}

func (s *server) MemberCount() int {
	count := 0
	s.members.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// QuorumSize 日志提交需要超过半数的节点（n/2+1）
func (s *server) QuorumSize() int {
	return s.MemberCount()/2 + 1
}