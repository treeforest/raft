package raft

import (
	"context"
	"fmt"
	log "github.com/treeforest/logger"
	"github.com/treeforest/raft/pb"
	"google.golang.org/grpc"
	"sync"
	"sync/atomic"
	"time"
)

// member 集群中的成员
type member struct {
	pb.Member
	sync.RWMutex
	server       *server
	dialTimeout  time.Duration
	dialOptions  []grpc.DialOption
	cc           *grpc.ClientConn
	lastActivity time.Time
	prevLogIndex uint64
	stop         chan bool
	state        int32 // 0:running, 1:stopping, 2:state
}

func newMember(m pb.Member, server *server) *member {
	return &member{
		server:       server,
		dialTimeout:  server.config.DialTimeout,
		dialOptions:  server.config.DialOptions,
		Member:       m,
		cc:           nil,
		lastActivity: time.Time{},
		prevLogIndex: 0,
		state:        0,
	}
}

func (m *member) setPrevLogIndex(index uint64) {
	m.Lock()
	defer m.Unlock()
	m.prevLogIndex = index
}

func (m *member) getPrevLogIndex() uint64 {
	m.RLock()
	defer m.RUnlock()
	return m.prevLogIndex
}

func (m *member) setLastActivity(now time.Time) {
	m.Lock()
	defer m.Unlock()
	m.lastActivity = now
}

func (m *member) LastActivity() time.Time {
	m.RLock()
	defer m.RUnlock()
	return m.lastActivity
}

func (m *member) sendAppendEntriesRequest(req *pb.AppendEntriesRequest) error {
	client, err := m.client()
	if err != nil {
		return err
	}

	resp, err := client.AppendEntries(context.Background(), req)
	if err != nil {
		return fmt.Errorf("send AppendEntries error: %v", err)
	}

	m.setLastActivity(time.Now())

	m.Lock()
	if resp.Success {
		if len(req.Entries) > 0 {
			m.prevLogIndex = req.Entries[len(req.Entries)-1].Index
		} else {
			// heartbeat
			m.Unlock()
			return nil
		}
	} else {
		if resp.Term > m.server.CurrentTerm() {
			// 出现了任期比自己新的leader
		} else if resp.Term == req.Term && resp.CommitIndex >= m.prevLogIndex {
			// 可能由于节点的丢包，导致leader没有即使更新prevLogIndex值，
			// 直接采用更新prevLogIndex的操作
			m.prevLogIndex = resp.CommitIndex
		} else if m.prevLogIndex > 0 {
			// 没有找到匹配的日志索引，对prevLogIndex采用自减操作，直到匹配
			m.prevLogIndex--
			if m.prevLogIndex > resp.Index {
				// 采用直接设置为resp.Index
				m.prevLogIndex = resp.Index
			}
		}
	}
	m.Unlock()

	// 通知server处理AppendEntriesResponse
	m.server.leaderRespChan <- resp

	return nil
}

// sendVoteRequest 发送投票请求
func (m *member) sendVoteRequest(req *pb.RequestVoteRequest, c chan *pb.RequestVoteResponse) error {
	client, err := m.client()
	if err != nil {
		return err
	}

	resp, err := client.RequestVote(context.Background(), req)
	if err != nil {
		return fmt.Errorf("requestVote grpc error: %v", err)
	}

	m.setLastActivity(time.Now())
	c <- resp
	return nil
}

func (m *member) sendSnapshotAskRequest() error {
	client, err := m.client()
	if err != nil {
		return err
	}

	snapshot := m.server.snapshot

	resp, err := client.SnapshotAsk(context.Background(), &pb.SnapshotAskRequest{
		LeaderId:  m.server.ID(),
		LastIndex: snapshot.LastIndex,
		LastTerm:  snapshot.LastTerm,
	})
	if err != nil {
		log.Warnf("send SnapshotRequest failed: %v", err)
		return err
	}

	m.setLastActivity(time.Now())

	if resp.Success {
		_ = m.sendSnapshotRecoveryRequest(&snapshot.Snapshot)
	}

	return nil
}

func (m *member) sendSnapshotRecoveryRequest(snapshot *pb.Snapshot) error {
	client, err := m.client()
	if err != nil {
		return err
	}

	req := &pb.SnapshotRecoveryRequest{
		LeaderId:  m.server.CurrentTerm(),
		LastIndex: snapshot.LastIndex,
		LastTerm:  snapshot.LastTerm,
		Members:   m.server.Members(),
		State:     snapshot.State,
	}
	resp, err := client.SnapshotRecovery(context.Background(), req)
	if err != nil {
		log.Warnf("send SnapshotRecovery failed: %v", err)
		return err
	}

	m.setLastActivity(time.Now())
	if resp.Success {
		m.prevLogIndex = req.LastIndex
	} else {
		log.Debug("send snapshot recovery failed: ", m.Id)
	}

	return nil
}

func (m *member) sendAddMemberRequest(req *pb.MemberRequest) error {
	client, err := m.client()
	if err != nil {
		return err
	}

	err = timeoutFunc(m.dialTimeout, func() error {
		_, err = client.AddMember(context.Background(), req)
		return err
	})

	return err
}

func (m *member) sendRemoveMemberRequest(req *pb.MemberRequest) error {
	client, err := m.client()
	if err != nil {
		return err
	}

	err = timeoutFunc(m.dialTimeout, func() error {
		_, err = client.RemoveMember(context.Background(), req)
		return err
	})

	return err
}

func (m *member) sendMembershipRequest(req *pb.MembershipRequest) (*pb.MembershipResponse, error) {
	client, err := m.client()
	if err != nil {
		return nil, err
	}

	var resp *pb.MembershipResponse
	err = timeoutFunc(m.dialTimeout, func() error {
		resp, err = client.Membership(context.Background(), req)
		return err
	})

	return resp, err
}

func (m *member) client() (pb.RaftClient, error) {
	if m.getCC() == nil {
		if err := m.dial(); err != nil {
			return nil, fmt.Errorf("dial failed: %v", err)
		}
	}
	return pb.NewRaftClient(m.getCC()), nil
}

func (m *member) startHeartbeat() {
	m.stop = make(chan bool, 1)
	go m.heartbeat()
}

func (m *member) stopHeartbeat(flush bool) {
	m.stop <- flush
}

func (m *member) heartbeat() {
	stop := m.stop

	ticker := time.NewTicker(m.server.config.HeartbeatInterval)
	log.Debugf("peer.heartbeat: ", m.Id, m.server.config.HeartbeatInterval)

	for {
		select {
		case flush := <-stop:
			if flush {
				log.Debug("stop heartbeat with flush")
				_ = m.flush()
			} else {
				log.Debug("stop heartbeat")
			}
			log.Infof("stop heartbeat with %d", m.Id)
			atomic.SwapInt32(&m.state, 2)
			return
		case <-ticker.C:
			if atomic.LoadInt32(&m.state) != 0 {
				break
			}
			if err := m.flush(); err != nil {
				log.Warnf("flush failed: %d", m.Id)

				// 暂停心跳
				ticker.Stop()

				// 心跳失败, 尝试重连3次
				var i = 0
				for i < 3 {
					if err = m.dial(); err == nil {
						break
					}
					i++
				}
				if i == 3 {
					// 移除节点,不需要return，在RemoveMember方法里会向stop通道传值
					m.setCC(nil)
					m.server.removeMember(m.Id)
					atomic.SwapInt32(&m.state, 1)
					break
				}

				// 恢复心跳
				ticker.Reset(m.server.config.HeartbeatInterval)
			}
		}
	}
}

func (m *member) flush() (err error) {
	log.Debug("flush: ", m.Id)
	prevLogIndex := m.getPrevLogIndex()
	term := m.server.CurrentTerm()
	entries, prevLogTerm := m.server.log.getEntriesAfter(prevLogIndex, m.server.config.MaxLogEntriesPerRequest)
	if entries != nil {
		err = m.sendAppendEntriesRequest(&pb.AppendEntriesRequest{
			Term:         term,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			CommitIndex:  m.server.log.CommitIndex(),
			LeaderId:     m.server.ID(),
			Entries:      entries,
		})
	} else {
		err = m.sendSnapshotAskRequest()
	}
	return
}

func (m *member) getCC() *grpc.ClientConn {
	m.RLock()
	defer m.RUnlock()
	return m.cc
}

func (m *member) setCC(cc *grpc.ClientConn) {
	m.Lock()
	defer m.Unlock()
	m.cc = cc
}

func (m *member) dial() error {
	cc := m.getCC()
	if cc != nil {
		if ping(cc, m.dialTimeout) == true {
			return nil
		}
		// ping failed, reconnect
	}

	cc, err := dial(m.Address, m.dialTimeout, m.dialOptions)
	if err != nil {
		return err
	}

	// log.Infof("dial %s success", m.Address)

	m.setCC(cc)
	return nil
}
