package raft

import (
	"context"
	"errors"
	"fmt"
	log "github.com/treeforest/logger"
	"github.com/treeforest/raft/pb"
	"google.golang.org/grpc"
	"io/ioutil"
	"net"
	"sort"
	"sync"
	"time"
)

var (
	StopError = errors.New("stopped")
	NotLeader = errors.New("not leader")
)

type server struct {
	pb.UnimplementedRaftServer
	grpcServer            *grpc.Server                   // grpc server
	locker                sync.RWMutex                   // 读写锁
	config                *Config                        // 配置文件
	stateMachine          StateMachine                   // 状态机
	pendingSnapshot       *pb.Snapshot                   // 待存储的快照
	snapshot              *pb.Snapshot                   // 最新快照
	currentTerm           uint64                         // 当前的任期
	votedFor              uint64                         // 选票投给了哪个候选人
	log                   *Log                           // 日志对象
	members               sync.Map                       // members in cluster
	state                 pb.NodeState                   // 节点的当前状态,follower/candidate/leaderId/snapshotting/stopped/initialized
	stopped               chan struct{}                  // 停止信号
	routineGroup          sync.WaitGroup                 // 保证协程能够安全退出
	leaderId              uint64                         // 当state为follower时，设置为leader的id
	followerHeartbeatChan chan bool                      // follower用于更新与leader之间的心跳
	leaderRespChan        chan *pb.AppendEntriesResponse // leaderId loop use
}

func New(config *Config, stateMachine StateMachine) Raft {
	return &server{
		grpcServer:            nil,
		locker:                sync.RWMutex{},
		config:                config,
		stateMachine:          stateMachine,
		pendingSnapshot:       nil,
		snapshot:              nil,
		currentTerm:           0,
		votedFor:              0,
		log:                   newLog(config.LogPath, stateMachine.Apply),
		members:               sync.Map{},
		stopped:               make(chan struct{}, 1),
		routineGroup:          sync.WaitGroup{},
		leaderId:              0,
		followerHeartbeatChan: make(chan bool, 256),
		leaderRespChan:        make(chan *pb.AppendEntriesResponse, 256),
	}
}

func (s *server) Start() error {
	if s.Running() {
		return errors.New("server already running")
	}

	if err := s.Init(); err != nil {
		return err
	}

	s.setState(pb.NodeState_Follower)

	if s.log.currentIndex() > 0 {
		log.Info("start from previous saved state")
	} else {
		log.Info("start as a new raft server")
	}

	lis, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		log.Fatal(err)
	}
	s.grpcServer = grpc.NewServer(s.config.ServerOptions...)

	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		if err = s.grpcServer.Serve(lis); err != nil {
			log.Error(err)
		}
	}()
	time.Sleep(time.Millisecond * 100)

	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		s.loop()
	}()

	return nil
}

func (s *server) Init() error {
	if s.Running() {
		return errors.New("server already running")
	}

	if s.state == pb.NodeState_Initialized || s.log.initialized {
		s.state = pb.NodeState_Initialized
		return nil
	}

	if err := s.log.open(s.config.LogPath); err != nil {
		return fmt.Errorf("open log failed: %v", err)
	}

	_, s.currentTerm = s.log.lastInfo()

	s.state = pb.NodeState_Initialized
	return nil
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

	s.followerHeartbeatChan <- true
	s.votedFor = req.CandidateId
	resp.VoteGranted = true
	return
}

func (s *server) Join(existing []string) {
	if existing == nil || len(existing) == 0 {
		return
	}

	req := &pb.MembershipRequest{
		Member: pb.Member{
			Id:      s.config.MemberId,
			Address: s.config.Address,
		},
	}

	for _, addr := range existing {
		if addr == "" {
			continue
		}

		cc, err := dial(addr, s.config.DialTimeout, s.config.DialOptions)
		if err != nil {
			log.Errorf("dial %s failed: %v", addr, err)
			continue
		}

		resp, err := pb.NewRaftClient(cc).Membership(context.Background(), req)
		if err != nil {
			log.Errorf("send membership request failed: %v", err)
			continue
		}

		for _, m := range resp.Members {
			if _, ok := s.members.Load(m.Id); ok {
				continue
			}
			s.members.Store(m.Id, newMember(m, s))
		}

		log.Infof("join %s success", addr)
	}
}

func (s *server) Do(commandName string, command []byte) (uint64, error) {
	if s.state != pb.NodeState_Leader {
		return 0, NotLeader
	}

	entry := &pb.LogEntry{
		Term:        s.currentTerm,
		Index:       s.log.currentIndex() + 1,
		CommandName: commandName,
		Command:     command,
	}

	if err := s.log.writeEntry(entry); err != nil {
		return 0, err
	}

	// TODO: 对 index 的提交进行监听
	return entry.Index, nil
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
		if s.State() == pb.NodeState_Candidate {
			s.setState(pb.NodeState_Follower)
		}
		s.leaderId = req.LeaderId
	} else {
		// update term and leaderId
		s.updateCurrentTerm(req.Term, req.LeaderId)
	}

	s.followerHeartbeatChan <- true

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

	s.updateCurrentTerm(req.LastTerm, req.LeaderId)
	return
}

// Membership 成员信息
func (s *server) Membership(ctx context.Context, req *pb.MembershipRequest) (
	resp *pb.MembershipResponse, err error) {
	if value, ok := s.members.Load(req.Member.Id); !ok {
		s.members.Store(req.Member.Id, newMember(req.Member, s))
	} else {
		m := value.(*member)
		if m.Address != req.Member.Address {
			m.Address = req.Member.Address
			m.cc = nil
			s.members.Store(req.Member.Id, m)
		}
	}

	return &pb.MembershipResponse{Members: s.Members()}, nil
}

func (s *server) Stop() {
	if s.state == pb.NodeState_Stopped {
		return
	}

	// 结束运行的事件循环
	close(s.stopped)

	s.grpcServer.Stop()

	// 等待协程运行结束
	s.routineGroup.Wait()

	s.log.close()
	s.setState(pb.NodeState_Stopped)

	log.Info("stopped")
}

func (s *server) IsLeader() bool {
	s.locker.RLock()
	defer s.locker.RUnlock()
	return s.state == pb.NodeState_Leader
}

func (s *server) LeaderId() uint64 {
	s.locker.RLock()
	defer s.locker.RUnlock()
	return s.leaderId
}

func (s *server) LeaderAddress() string {
	address := ""
	s.members.Range(func(key, value interface{}) bool {
		m := value.(*member)
		if m.Id == s.leaderId {
			address = m.Address
			return false
		}
		return true
	})
	return address
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

// ID 当前server的id
func (s *server) ID() uint64 {
	return s.config.MemberId
}

func (s *server) Address() string {
	return s.config.Address
}

// updateCurrentTerm 更新任期
func (s *server) updateCurrentTerm(term uint64, leaderId uint64) {
	if s.State() == pb.NodeState_Leader {
		// 如果是leader，则停止所有心跳
		s.members.Range(func(key, value interface{}) bool {
			m := value.(*member)
			m.stopHeartbeat(false)
			return true
		})
	}

	if s.State() != pb.NodeState_Follower {
		s.setState(pb.NodeState_Follower)
	}

	// 更新状态
	s.locker.Lock()
	s.currentTerm = term
	s.leaderId = leaderId
	s.votedFor = 0
	s.locker.Unlock()
}

// loop 事件循环
func (s *server) loop() {
	state := s.State()
	for state != pb.NodeState_Stopped {
		log.Infof("state: %s", state.String())
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
	electionTimeout := s.config.ElectionTimeout
	timeoutChan := afterBetween(electionTimeout, electionTimeout*2)
	update := false

	for s.State() == pb.NodeState_Follower {
		update = false
		select {
		case <-s.stopped:
			s.setState(pb.NodeState_Stopped)
			return
		case update = <-s.followerHeartbeatChan:
			// 收到leader的心跳消息，准备更新超时时间
		case <-timeoutChan:
			// 超时未收到leader的心跳消息
			if s.log.currentIndex() > 0 || s.MemberCount() == 1 {
				// 从follower转变成candidate
				s.setState(pb.NodeState_Candidate)
			} else {
				update = true
			}
		}

		if update {
			// 在超时时间内收到了server的心跳消息，重置超时时间
			timeoutChan = afterBetween(electionTimeout, electionTimeout*2)
		}
	}
}

func (s *server) candidateLoop() {
	s.leaderId = 0

	lastLogIndex, lastLogTerm := s.log.lastInfo()
	doVote := true
	votesGranted := 0
	var timeoutChan <-chan time.Time
	var respChan chan *pb.RequestVoteResponse

	for s.State() == pb.NodeState_Candidate {
		if doVote {
			// 任期加一
			s.setCurrentTerm(s.CurrentTerm() + 1)

			// 自己的选票投给自己
			s.votedFor = s.ID()

			respChan = make(chan *pb.RequestVoteResponse, s.MemberCount())
			req := &pb.RequestVoteRequest{
				Term:         s.CurrentTerm(),
				CandidateId:  s.ID(),
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
	s.leaderRespChan = make(chan *pb.AppendEntriesResponse, memberCount)
	s.members.Range(func(key, value interface{}) bool {
		m := value.(*member)
		m.setPrevLogIndex(logIndex)
		m.startHeartbeat()
		return true
	})

	// 集群中只有单一节点时的信号
	single := make(chan struct{}, 1)
	go func() {
		t := time.NewTicker(time.Millisecond * 100)
		for s.MemberCount() == 1 && s.state == pb.NodeState_Leader {
			select {
			case <-t.C:
				single <- struct{}{}
			}
		}
	}()

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

		case <-single:
			_ = s.log.setCommitIndex(s.log.currentIndex())

		case resp := <-s.leaderRespChan:
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
	if s.ID() == m.Id {
		return
	}

	if _, ok := s.members.Load(m.Id); ok {
		return
	}

	mem := newMember(*m, s)
	if s.State() == pb.NodeState_Leader {
		mem.startHeartbeat()
	}

	s.members.Store(mem.Id, mem)
}

func (s *server) RemoveMember(id uint64) {
	if s.ID() == id {
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
	members = append(members, pb.Member{Id: s.ID(), Address: s.Address()})
	s.members.Range(func(key, value interface{}) bool {
		members = append(members, value.(*member).Member)
		return true
	})
	return members
}

func (s *server) MemberCount() int {
	count := 1 // 自身节点
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
