package raft

import (
	"context"
	"errors"
	"fmt"
	log "github.com/treeforest/logger"
	"github.com/treeforest/raft/pb"
	"google.golang.org/grpc"
	"hash/crc32"
	"io/ioutil"
	"net"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var (
	StopError        = errors.New("state")
	NotLeaderError   = errors.New("not leader")
	NotFollowerError = errors.New("not follower")
	NotSnapshotError = errors.New("not snapshot")
)

type event struct {
	target   any
	retValue any
	c        chan error
}

type server struct {
	grpcServer      *grpc.Server   // grpc server
	currentTerm     uint64         // 当前任期
	locker          sync.RWMutex   // 读写锁
	config          *Config        // 配置文件
	stateMachine    StateMachine   // 状态机
	pendingSnapshot *Snapshot      // 待存储的快照
	snapshot        *Snapshot      // 最新快照
	votedFor        uint64         // 选票投给了哪个候选人
	log             *Log           // 日志对象
	members         *memberManager // 集群中节点的管理（不包括自身节点）
	state           pb.NodeState   // 节点的当前状态,follower/candidate/leaderId/snapshotting/state/initialized
	stopped         chan struct{}  // 停止信号
	routineGroup    sync.WaitGroup // 保证协程能够安全退出
	leaderId        uint64         // 当state为follower时，设置为leader的id
	cmdChan         chan *Command
	cmdPool         *sync.Pool
	c               chan *event
}

func New(config *Config, stateMachine StateMachine) Raft {
	pool := sync.Pool{New: func() any {
		return new(Command)
	}}
	return &server{
		grpcServer:      nil,
		locker:          sync.RWMutex{},
		config:          config,
		stateMachine:    stateMachine,
		pendingSnapshot: nil,
		snapshot:        nil,
		votedFor:        0,
		log:             newLog(config.WriteType, config.LogWriteTimeInterval, stateMachine.Apply),
		members:         newMemberManager(),
		stopped:         make(chan struct{}, 1),
		routineGroup:    sync.WaitGroup{},
		leaderId:        0,
		cmdChan:         make(chan *Command, 1024),
		cmdPool:         &pool,
		c:               make(chan *event, 4096),
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

	if s.log.CurrentIndex() > 0 {
		log.Info("start from previous saved state")
	} else {
		log.Info("start as a new raft server")
	}

	lis, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		log.Fatal(err)
	}

	s.grpcServer = grpc.NewServer(s.config.ServerOptions...)
	pb.RegisterRaftServer(s.grpcServer, s)

	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		log.Infof("grpc server running at %s", s.config.Address)
		if err = s.grpcServer.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()
	time.Sleep(time.Millisecond * 50)

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

func (s *server) Join(existing string) {
	if existing == "" || len(existing) == 0 {
		return
	}

	join := func(addr string) *pb.AddMemberResponse {
		cc, err := dial(addr, s.config.DialTimeout, s.config.DialOptions)
		if err != nil {
			log.Errorf("dial %s failed: %v", addr, err)
			return nil
		}

		req := &pb.AddMemberRequest{
			Leader: false,
			Member: pb.Member{Id: s.ID(), Address: s.Address()},
		}

		resp, err := pb.NewRaftClient(cc).AddMember(context.Background(), req)
		if err != nil {
			log.Errorf("send AddMember request failed: %v", err)
			return nil
		}

		return resp
	}

	resp := join(existing)
	if resp == nil {
		return
	}

	if resp.Success == false {
		resp = join(resp.Leader.Address)
		if resp == nil {
			return
		}

		if resp.Success == false {
			log.Info("join cluster failed")
			return
		}
	}

	log.Info("join cluster success")
}

func (s *server) Do(commandName string, command []byte) <-chan error {
	done := make(chan error, 1)

	go func() {
		if !s.IsLeader() {
			done <- NotLeaderError
			return
		}

		cmd := s.cmdPool.Get().(*Command)
		cmd.name = commandName
		cmd.data = command
		cmd.errCh = make(chan error, 1)

		defer func() {
			cmd.name = ""
			cmd.data = []byte{}
			s.cmdPool.Put(cmd)
		}()

		select {
		case s.cmdChan <- cmd:
		case <-s.stopped:
			done <- StopError
			return
		default:
			// cmdChan 通道阻塞，采用异步发送的方式
			s.routineGroup.Add(1)
			go func() {
				defer s.routineGroup.Done()
				select {
				case s.cmdChan <- cmd:
				case <-s.stopped:
					done <- StopError
					return
				}
			}()
		}

		select {
		case <-s.stopped:
			done <- StopError
			return
		case err := <-cmd.errCh:
			done <- err
		}
	}()

	return done
}

// AppendEntries 用于leader进行日志复制与心跳检测
func (s *server) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	log.Debugf("AppendEntries, request: %v", *req)
	//since := time.Now()
	//defer func() {
	//	log.Infof("time used:%d ms", time.Now().Sub(since).Milliseconds())
	//}()
	resp, err := s.send(req)
	if err != nil {
		return &pb.AppendEntriesResponse{}, err
	}
	return resp.(*pb.AppendEntriesResponse), nil
}

func (s *server) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	log.Debug("RequestVote")
	resp, err := s.send(req)
	if err != nil {
		return &pb.RequestVoteResponse{}, err
	}
	return resp.(*pb.RequestVoteResponse), nil
}

func (s *server) SnapshotAsk(ctx context.Context, req *pb.SnapshotAskRequest) (*pb.SnapshotAskResponse, error) {
	log.Debug("SnapshotAsk")
	switch s.State() {
	case pb.NodeState_Follower:
		resp, err := s.send(req)
		if err != nil {
			return &pb.SnapshotAskResponse{}, err
		}
		return resp.(*pb.SnapshotAskResponse), nil
	default:
		return &pb.SnapshotAskResponse{Success: false}, NotFollowerError
	}
}

func (s *server) SnapshotRecovery(ctx context.Context, req *pb.SnapshotRecoveryRequest) (*pb.SnapshotRecoveryResponse, error) {
	log.Debug("SnapshotRecovery")
	switch s.State() {
	case pb.NodeState_Snapshotting:
		resp, err := s.send(req)
		if err != nil {
			return &pb.SnapshotRecoveryResponse{}, err
		}
		return resp.(*pb.SnapshotRecoveryResponse), nil
	default:
		return &pb.SnapshotRecoveryResponse{Success: false}, NotSnapshotError
	}
}

// Membership 成员信息,由leader->follower
func (s *server) Membership(ctx context.Context, req *pb.MembershipRequest) (*pb.MembershipResponse, error) {
	log.Debug("Membership")
	for _, m := range req.Members {
		if m.Id == s.ID() {
			continue
		}
		if s.members.Exist(m.Id) {
			continue
		}
		s.members.Store(newMember(m, s))
		log.Infof("add member %d %s", m.Id, m.Address)
	}
	return &pb.MembershipResponse{Success: true}, nil
}

// AddMember 添加成员，只能由leader完成
func (s *server) AddMember(ctx context.Context, req *pb.AddMemberRequest) (*pb.AddMemberResponse, error) {
	log.Debug("AddMember")
	resp, err := s.send(req)
	if err != nil {
		return &pb.AddMemberResponse{}, err
	}
	return resp.(*pb.AddMemberResponse), nil
}

// RemoveMember 移除节点，只能由leader完成
func (s *server) RemoveMember(ctx context.Context, req *pb.RemoveMemberRequest) (*pb.RemoveMemberResponse, error) {
	log.Debug("RemoveMember")
	resp, err := s.send(req)
	if err != nil {
		return &pb.RemoveMemberResponse{}, err
	}
	return resp.(*pb.RemoveMemberResponse), nil
}

// Ping ping message
func (s *server) Ping(ctx context.Context, req *pb.Empty) (*pb.Empty, error) {
	log.Debug("Ping")
	return &pb.Empty{}, nil
}

func (s *server) send(v any) (any, error) {
	if !s.Running() {
		return nil, StopError
	}

	e := &event{target: v, c: make(chan error, 1)}

	select {
	case <-s.stopped:
		return nil, StopError
	case s.c <- e:
	}

	select {
	case <-s.stopped:
		return nil, StopError
	case err := <-e.c:
		return e.retValue, err
	}
}

func (s *server) sendAsync(v any) {
	if !s.Running() {
		return
	}

	e := &event{target: v, c: make(chan error, 1)}

	select {
	case s.c <- e:
		return
	default:
	}

	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		select {
		case s.c <- e:
		case <-s.stopped:
		}
	}()
}

func (s *server) processAppendEntriesRequest(req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, bool) {
	// 1、reply false if term < currentTerm
	if req.Term < s.currentTerm {
		log.Debug("stale term")
		return &pb.AppendEntriesResponse{
			Term:        s.currentTerm,
			Index:       s.log.CurrentIndex(),
			CommitIndex: s.log.CommitIndex(),
			Success:     false,
		}, false

	} else if req.Term == s.currentTerm {
		if s.state == pb.NodeState_Candidate {
			s.setState(pb.NodeState_Follower)
		}
		s.leaderId = req.LeaderId

	} else {
		// update term and leaderId
		log.Debug("update currentTerm, become follower")
		s.updateCurrentTerm(req.Term, req.LeaderId)
	}

	// 2、reply false if log doesn't contain an Entry at prevLogIndex whose
	// term matches prevLogTerm
	if err := s.log.truncate(req.PrevLogIndex, req.PrevLogTerm); err != nil {
		log.Debug(err)
		return &pb.AppendEntriesResponse{
			Term:        s.currentTerm,
			Index:       s.log.CurrentIndex(),
			CommitIndex: s.log.CommitIndex(),
			Success:     false,
		}, true
	}

	// 3、if an existing Entry conflicts with a new one(same index but
	// different terms), delete the existing Entry and all the follow it.
	// 4、append any new entries not already in the log
	if err := s.log.appendEntries(req.Entries); err != nil {
		log.Warn(err)
		return &pb.AppendEntriesResponse{
			Term:        s.currentTerm,
			Index:       s.log.CurrentIndex(),
			CommitIndex: s.log.CommitIndex(),
			Success:     false,
		}, true
	}

	log.Debugf("append entries: (prevLogIndex:%d entries:%d)",
		req.PrevLogIndex, len(req.Entries))

	if err := s.log.setCommitIndex(req.CommitIndex); err != nil {
		log.Warn(err)
		return &pb.AppendEntriesResponse{
			Term:        s.currentTerm,
			Index:       s.log.CurrentIndex(),
			CommitIndex: s.log.CommitIndex(),
			Success:     false,
		}, true
	}

	// log.Debugf("AppendEntries success")
	// 若返回成功，则代表leader和follower的日志条目一定相同
	return &pb.AppendEntriesResponse{
		Term:        s.currentTerm,
		Index:       s.log.CurrentIndex(),
		CommitIndex: s.log.CommitIndex(),
		Success:     true,
	}, true
}

func (s *server) processAppendEntriesResponse(resp *AppendEntriesResponse, syncedMember map[uint64]struct{}) {
	if resp.Term > s.currentTerm {
		// 主动退位
		log.Debug("resp.CurrentTerm > s.CurrentTerm(), change from leader to follower")
		s.updateCurrentTerm(resp.Term, 0)
		return
	}

	if !resp.Success {
		return
	}

	if resp.Index <= s.log.CommitIndex() {
		return
	}

	switch s.config.ReplicationType {
	case Semisynchronous:
		_ = s.log.setCommitIndex(resp.Index)

	case Synchronous:
		syncedMember[resp.Id] = struct{}{}

		// 检查超过半数的条件
		if len(syncedMember)+1 < s.QuorumSize() {
			return
		}

		// 找到(1/2+1)个节点都确认的日志条目索引
		var indices []uint64
		indices = append(indices, s.log.CurrentIndex())
		s.members.Range(func(m *member) bool {
			indices = append(indices, m.getPrevLogIndex())
			return true
		})

		sort.Sort(sort.Reverse(Uint64Slice(indices))) // 从大到小排序
		commitIndex := indices[s.QuorumSize()-1]

		// 更新 commitIndex
		_ = s.log.setCommitIndex(commitIndex)

		// 重置
		syncedMember = make(map[uint64]struct{}, s.MemberCount())
	}
}

func (s *server) processRequestVoteRequest(req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, bool) {
	if req.Term < s.currentTerm {
		return &pb.RequestVoteResponse{Term: s.currentTerm, VoteGranted: false}, false
	}

	if req.Term > s.currentTerm {
		// 任期比对方小，成为跟随者
		log.Debugf("req.CurrentTerm > s.CurrentTerm()")
		s.updateCurrentTerm(req.Term, 0)

	} else if s.votedFor != 0 && s.votedFor != req.CandidateId {
		// 已投过票，且投票对象不是req.CandidateId
		log.Debugf("have been voted, votedFor:%d candidateId:%d", s.votedFor, req.CandidateId)
		return &pb.RequestVoteResponse{Term: s.currentTerm, VoteGranted: false}, false
	}

	lastLogIndex, lastLogTerm := s.log.lastInfo()
	if lastLogIndex > req.LastLogIndex || lastLogTerm > req.LastLogTerm {
		// 当前节点的日志更新，不做投票
		log.Debugf("local log is more newer")
		return &pb.RequestVoteResponse{Term: s.currentTerm, VoteGranted: false}, false
	}

	// 选票投给 req.CandidateId
	s.votedFor = req.CandidateId
	log.Infof("votedFor: %d", s.votedFor)

	return &pb.RequestVoteResponse{Term: s.currentTerm, VoteGranted: true}, true
}

func (s *server) processRequestVoteResponse(resp *pb.RequestVoteResponse) bool {
	if resp.VoteGranted && resp.Term == s.currentTerm {
		return true
	}

	if resp.Term > s.currentTerm {
		log.Debug("resp.CurrentTerm > s.CurrentTerm")
		s.updateCurrentTerm(resp.Term, 0)
	} else {
		// 节点拒绝了对当前节点的投票
	}

	return false
}

func (s *server) processAddMemberRequest(req *pb.AddMemberRequest) (*pb.AddMemberResponse, bool) {
	if !s.IsLeader() && !req.Leader {
		return &pb.AddMemberResponse{
			Success: false,
			Leader:  pb.Member{Id: s.LeaderId(), Address: s.LeaderAddress()},
		}, false
	}

	s.addMember(req.Member)
	return &pb.AddMemberResponse{Success: true}, true
}

func (s *server) processRemoveMemberRequest(req *pb.RemoveMemberRequest) (*pb.RemoveMemberResponse, bool) {
	if s.state != pb.NodeState_Leader && !req.Leader {
		return &pb.RemoveMemberResponse{
			Success: false,
			Leader:  pb.Member{Id: s.LeaderId(), Address: s.LeaderAddress()},
		}, false
	}

	s.removeMember(req.Member.Id)
	return &pb.RemoveMemberResponse{Success: true}, true
}

func (s *server) processSnapshotAskRequest(req *pb.SnapshotAskRequest) *pb.SnapshotAskResponse {
	entry := s.log.getEntry(req.LastIndex)
	if entry != nil && entry.Term == req.LastTerm {
		return &pb.SnapshotAskResponse{Success: false}
	}

	s.setState(pb.NodeState_Snapshotting)
	return &pb.SnapshotAskResponse{Success: true}
}

func (s *server) processSnapshotRecoveryRequest(req *pb.SnapshotRecoveryRequest) *pb.SnapshotRecoveryResponse {
	if err := s.stateMachine.Recovery(req.State); err != nil {
		log.Fatal("cannot recover from previous state")
	}

	// add member
	for _, mb := range req.Members {
		s.addMember(mb)
	}

	s.currentTerm = req.LastTerm
	s.log.updateCommitIndex(req.LastIndex)

	// 创建本地快照
	s.pendingSnapshot = newSnapshot(s.snapshotPath(req.LastIndex, req.LastTerm), &pb.Snapshot{
		LastIndex: req.LastIndex,
		LastTerm:  req.LastTerm,
		State:     req.State,
		Members:   req.Members,
	})
	_ = s.saveSnapshot()

	// 清除 LastIndex 之前的条目
	_ = s.log.compact(req.LastIndex, req.LastTerm)

	log.Debugf("update currentTerm, become follower")
	s.updateCurrentTerm(req.LastTerm, req.LeaderId)

	return &pb.SnapshotRecoveryResponse{Term: req.LastTerm, Success: true, CommitIndex: req.LastIndex}
}

func (s *server) Stop() {
	if s.state == pb.NodeState_Stopped {
		return
	}

	s.leaveCluster()

	// 结束运行的事件循环
	close(s.stopped)

	s.grpcServer.Stop()

	// 等待协程运行结束
	s.routineGroup.Wait()

	s.log.close()
	s.setState(pb.NodeState_Stopped)

	close(s.cmdChan)
	close(s.c)

	log.Info("stopped")
}

// leaveCluster 离开集群
func (s *server) leaveCluster() {
	if s.IsLeader() {
		s.members.Range(func(m *member) bool {
			_ = m.sendRemoveMemberRequest(&pb.RemoveMemberRequest{Leader: true, Member: s.Self()})
			return true
		})
		return
	}

	if s.State() == pb.NodeState_Candidate {
		return
	}

	leader, ok := s.members.Load(s.leaderId)
	if !ok {
		return
	}

	_ = leader.sendRemoveMemberRequest(&pb.RemoveMemberRequest{Leader: false, Member: s.Self()})
}

func (s *server) IsLeader() bool {
	s.locker.RLock()
	defer s.locker.RUnlock()
	return s.state == pb.NodeState_Leader
}

func (s *server) LeaderId() uint64 {
	switch s.State() {
	case pb.NodeState_Leader:
		return s.ID()
	case pb.NodeState_Follower:
		return s.leaderId
	default:
		return 0
	}
}

func (s *server) LeaderAddress() string {
	switch s.State() {
	case pb.NodeState_Leader:
		return s.Address()
	case pb.NodeState_Follower:
		m, _ := s.members.Load(s.leaderId)
		return m.Address
	default:
		return ""
	}
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
	log.Infof("update term:%d leaderId:%d", term, leaderId)

	if s.State() == pb.NodeState_Leader {
		// 如果是leader，则停止所有心跳
		s.members.Range(func(m *member) bool {
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
	s.leaderId = leaderId
	s.votedFor = 0
	s.locker.Unlock()
}

// loop 事件循环
func (s *server) loop() {
	state := s.State()
	for state != pb.NodeState_Stopped {
		log.Infof("state:%s term:%d index:%d", state.String(), s.CurrentTerm(), s.log.CurrentIndex())
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
	heartbeatTimeout := s.config.HeartbeatTimeout
	timeoutChan := afterBetween(heartbeatTimeout, heartbeatTimeout*2)
	update := false

	for s.State() == pb.NodeState_Follower {
		update = false
		select {
		case <-s.stopped:
			s.setState(pb.NodeState_Stopped)
			return

		case e := <-s.c:
			var err error
			switch msg := e.target.(type) {
			case *pb.AppendEntriesRequest:
				e.retValue, update = s.processAppendEntriesRequest(msg)
			case *pb.RequestVoteRequest:
				e.retValue, update = s.processRequestVoteRequest(msg)
			case *pb.AddMemberRequest:
				e.retValue, update = s.processAddMemberRequest(msg)
			case *pb.RemoveMemberRequest:
				e.retValue, update = s.processRemoveMemberRequest(msg)
			case *pb.SnapshotAskRequest:
				e.retValue = s.processSnapshotAskRequest(msg)
			}
			e.c <- err

		case <-timeoutChan:
			// 超时未收到leader的心跳消息
			if s.log.CurrentIndex() > 0 || s.MemberCount() == 1 {
				// 从follower转变成candidate
				log.Debugf("heartbeat timeout, currentIndex:%d change state from Follower to Candidate", s.log.CurrentIndex())
				s.setState(pb.NodeState_Candidate)
			} else {
				update = true
			}
		}

		if update {
			// 在超时时间内收到了server的心跳消息，重置超时时间
			timeoutChan = afterBetween(heartbeatTimeout, heartbeatTimeout*2)
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
			s.currentTerm++

			// 自己的选票投给自己
			s.votedFor = s.ID()

			respChan = make(chan *pb.RequestVoteResponse, s.MemberCount())
			req := &pb.RequestVoteRequest{
				Term:         s.currentTerm,
				CandidateId:  s.ID(),
				LastLogTerm:  lastLogTerm,
				LastLogIndex: lastLogIndex,
			}

			// 向连接的节点发送请求选票的消息
			s.members.Range(func(m *member) bool {
				s.routineGroup.Add(1)
				go func(m *member) {
					defer s.routineGroup.Done()
					if err := m.sendVoteRequest(req, respChan); err != nil {
						// 投票时以能连接的节点为目标，采用强一致性方式，不能通信的就移除
						s.removeMember(m.Id)
					}
				}(m)
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
			if succ := s.processRequestVoteResponse(resp); succ {
				votesGranted++
			}

		case e := <-s.c:
			var err error
			switch msg := e.target.(type) {
			case *pb.AppendEntriesRequest:
				e.retValue, _ = s.processAppendEntriesRequest(msg)
			case *pb.RequestVoteRequest:
				e.retValue, _ = s.processRequestVoteRequest(msg)
			case *pb.AddMemberRequest:
				e.retValue, _ = s.processAddMemberRequest(msg)
			case *pb.RemoveMemberRequest:
				e.retValue, _ = s.processRemoveMemberRequest(msg)
			}
			e.c <- err

		case <-timeoutChan:
			// 选举超时，开始新一轮选举
			log.Infof("candidate vote timeout")
			doVote = true
		}
	}
}

// leaderLoop
// 1、成员保活 2、发送日志条目 3、日志提交
func (s *server) leaderLoop() {
	s.members.Range(func(m *member) bool {
		m.setPrevLogIndex(s.log.CurrentIndex())
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
				select {
				case single <- struct{}{}:
				default:
				}
			}
		}
	}()

	// 已同步了commit后的日志条目的节点数（）
	syncedMember := make(map[uint64]struct{})

	for s.State() == pb.NodeState_Leader {
		select {
		case <-s.stopped:
			s.members.Range(func(m *member) bool {
				m.stopHeartbeat(false)
				return true
			})
			s.setState(pb.NodeState_Stopped)
			return

		case <-single:
			_ = s.log.setCommitIndex(s.log.CurrentIndex())

		case cmd := <-s.cmdChan:
			entry := &pb.LogEntry{
				Term:        s.currentTerm,
				Index:       s.log.nextIndex(),
				CommandName: cmd.name,
				Command:     cmd.data,
			}

			if err := s.log.appendEntries([]pb.LogEntry{*entry}); err != nil {
				cmd.errCh <- errors.New("write entry error")
				log.Error("write entry error: ", err)
				break
			}

			sub := s.log.Subscribe(entry.Index, s.config.SubscribeTTL)
			go func(cmd *Command) {
				_, err := sub.Listen()
				cmd.errCh <- err
			}(cmd)

			if s.config.ReplicationType == Asynchronous || s.MemberCount() == 1 {
				// 异步复制或集群中只有一个节点，leader直接提交日志，并返回结果
				_ = s.log.setCommitIndex(entry.Index)
			}

		case e := <-s.c:
			var err error
			switch msg := e.target.(type) {
			case *pb.AppendEntriesRequest:
				e.retValue, _ = s.processAppendEntriesRequest(msg)
			case *AppendEntriesResponse:
				s.processAppendEntriesResponse(msg, syncedMember)
			case *pb.RequestVoteRequest:
				e.retValue, _ = s.processRequestVoteRequest(msg)
			case *pb.AddMemberRequest:
				e.retValue, _ = s.processAddMemberRequest(msg)
			case *pb.RemoveMemberRequest:
				e.retValue, _ = s.processRemoveMemberRequest(msg)
			}
			e.c <- err
		}
	}
}

func (s *server) snapshotLoop() {
	for s.State() == pb.NodeState_Snapshotting {
		select {
		case <-s.stopped:
			s.setState(pb.NodeState_Stopped)
			return

		case e := <-s.c:
			var err error
			switch msg := e.target.(type) {
			case *pb.AppendEntriesRequest:
				e.retValue, _ = s.processAppendEntriesRequest(msg)
			case *pb.RequestVoteRequest:
				e.retValue, _ = s.processRequestVoteRequest(msg)
			case *pb.AddMemberRequest:
				e.retValue, _ = s.processAddMemberRequest(msg)
			case *pb.RemoveMemberRequest:
				e.retValue, _ = s.processRemoveMemberRequest(msg)
			case *pb.SnapshotRecoveryRequest:
				e.retValue = s.processSnapshotRecoveryRequest(msg)
			}
			e.c <- err
		}
	}
}

func (s *server) addMember(mem pb.Member) {
	// 不允许节点加入两次
	if s.members.Exist(mem.Id) {
		return
	}

	if mem.Id == s.ID() {
		return
	}

	m := newMember(mem, s)

	// 再次检查是否添加成功
	if ok := s.members.Store(m); !ok {
		return
	}
	log.Infof("add member %d %s", m.Id, m.Address)

	if s.State() == pb.NodeState_Leader {
		m.setPrevLogIndex(s.log.CurrentIndex())
		m.startHeartbeat()
	}

	// 将节点加入集群的消息采用 gossip 的方式广播到整个网络
	if s.IsLeader() {
		// 发送当前成员信息
		s.routineGroup.Add(1)
		go func() {
			s.routineGroup.Done()
			_, _ = m.sendMembershipRequest(&pb.MembershipRequest{Members: s.Members()})
		}()

		// 将新节点加入的信息通知给其它节点
		req := &pb.AddMemberRequest{
			Leader: true,
			Member: mem,
		}
		s.members.Range(func(m *member) bool {
			go func(m *member) {
				_ = m.sendAddMemberRequest(req)
			}(m)
			return true
		})
	}
}

func (s *server) removeMember(id uint64) {
	if s.ID() == id {
		return
	}

	m, loaded := s.members.Delete(id)
	if !loaded {
		return
	}

	log.Infof("remove member %d", id)

	if s.state == pb.NodeState_Leader {
		s.routineGroup.Add(1)
		go func() {
			defer s.routineGroup.Done()
			m.stopHeartbeat(true)
		}()

		req := &pb.RemoveMemberRequest{Leader: true, Member: m.Member}
		s.members.Range(func(m *member) bool {
			go func(m *member) {
				_ = m.sendRemoveMemberRequest(req)
			}(m)
			return true
		})
	}
}

func (s *server) Members() []pb.Member {
	members := []pb.Member{s.Self()}
	s.members.Range(func(m *member) bool {
		members = append(members, m.Member)
		return true
	})
	return members
}

func (s *server) MemberCount() int {
	return s.members.Count() + 1
}

// QuorumSize 日志提交需要超过半数的节点（n/2+1）
func (s *server) QuorumSize() int {
	return s.MemberCount()/2 + 1
}

func (s *server) Self() pb.Member {
	return pb.Member{Id: s.ID(), Address: s.Address()}
}

func (s *server) CurrentTerm() uint64 {
	s.locker.RLock()
	defer s.locker.RUnlock()
	return s.currentTerm
}

// snapshotPath 快照路径
func (s *server) snapshotPath(lastIndex, lastTerm uint64) string {
	return filepath.Join(s.config.SnapshotPath, "snapshot", fmt.Sprintf("%d_%d.ss", lastTerm, lastIndex))
}

// TakeSnapshot 创建快照
func (s *server) TakeSnapshot() error {
	if s.stateMachine == nil {
		return errors.New("missing state machine")
	}

	if s.pendingSnapshot != nil {
		// 当前节点正在创建快照
		return errors.New("last snapshot is not finished")
	}

	lastIndex, lastTerm := s.log.commitInfo()

	if lastIndex == s.log.startIndex {
		return nil
	}

	state, err := s.stateMachine.Save()
	if err != nil {
		return err
	}

	s.pendingSnapshot = newSnapshot(s.snapshotPath(lastIndex, lastTerm), &pb.Snapshot{
		LastIndex: lastIndex,
		LastTerm:  lastTerm,
		State:     state,
		Members:   s.Members(),
	})

	_ = s.saveSnapshot()

	if lastIndex-s.log.startIndex > s.config.NumberOfLogEntriesAfterSnapshot {
		// 压缩：保留 NumberOfLogEntriesAfterSnapshot 条日志条目，删除多余的日志条目
		compactIndex := lastIndex - s.config.NumberOfLogEntriesAfterSnapshot
		compactTerm := s.log.getEntry(compactIndex).Term
		_ = s.log.compact(compactIndex, compactTerm)
	}

	return nil
}

// saveSnapshot 保存快照
func (s *server) saveSnapshot() error {
	if s.pendingSnapshot == nil {
		return errors.New("pendingSnapshot is nil")
	}

	if err := s.pendingSnapshot.Save(); err != nil {
		log.Errorf("pending snapshot save failed: %v", err)
		return err
	}

	tmp := s.snapshot
	s.snapshot = s.pendingSnapshot

	// 当快照发生变化时，删除前一个快照
	if tmp != nil && (tmp.LastIndex != s.snapshot.LastIndex || tmp.LastTerm != s.snapshot.LastTerm) {
		_ = tmp.Remove()
	}
	s.pendingSnapshot = nil

	return nil
}

// LoadSnapshot 重启时加载快照恢复状态
func (s *server) LoadSnapshot() error {
	dir, err := os.OpenFile(path.Join(s.config.SnapshotPath, "snapshot"), os.O_RDONLY, 0)
	if err != nil {
		log.Debug("cannot open snapshot")
		return err
	}

	filenames, err := dir.Readdirnames(-1)
	if err != nil {
		_ = dir.Close()
		panic(any(err))
	}
	_ = dir.Close()

	if len(filenames) == 0 {
		log.Debug("no snapshot to load")
		return nil
	}

	sort.Strings(filenames)

	// 最新快照路径
	snapshotPath := path.Join(s.config.SnapshotPath, "snapshot", filenames[len(filenames)-1])

	// 读取状态数据
	file, err := os.OpenFile(snapshotPath, os.O_RDONLY, 0)
	if err != nil {
		log.Debug("cannot open snapshot file ", snapshotPath)
		return err
	}
	defer file.Close()

	var checksum uint32
	n, err := fmt.Fscanf(file, "%08x\n", &checksum)
	if err != nil {
		return err
	} else if n != 1 {
		return errors.New("checksum error, bad snapshot file")
	}

	b, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	if checksum != crc32.ChecksumIEEE(b) {
		return errors.New("bad snapshot file")
	}

	if s.snapshot == nil {
		s.snapshot = newSnapshot(snapshotPath, &pb.Snapshot{})
	}
	if err = s.snapshot.Unmarshal(b); err != nil {
		log.Debug("unmarshal snapshot error: ", err)
		return err
	}

	// 恢复状态
	if err = s.stateMachine.Recovery(s.snapshot.State); err != nil {
		log.Debug("recovery snapshot error: ", err)
		return err
	}

	// 恢复集群中的成员
	for _, m := range s.snapshot.Members {
		s.addMember(m)
	}

	// 更新日志状态
	s.log.startTerm = s.snapshot.LastTerm
	s.log.startIndex = s.snapshot.LastIndex
	s.log.updateCommitIndex(s.snapshot.LastIndex)

	return nil
}
