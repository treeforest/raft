package raft

import (
	"context"
	"fmt"
	log "github.com/treeforest/logger"
	"github.com/treeforest/raft/pb"
	"google.golang.org/grpc"
	"sync"
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
	// respCh <- resp
	// m.server.sendAsync(resp)
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

func (m *member) sendSnapshotRequest() error {
	client, err := m.client()
	if err != nil {
		return err
	}

	snapshot := m.server.snapshot

	resp, err := client.Snapshot(context.Background(), &pb.SnapshotRequest{
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
		_ = m.sendSnapshotRecoveryRequest(snapshot)
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

func (m *member) client() (pb.RaftClient, error) {
	if m.cc == nil {
		if err := m.dial(); err != nil {
			return nil, fmt.Errorf("dial failed: %v", err)
		}
	}
	return pb.NewRaftClient(m.cc), nil
}

func (m *member) startHeartbeat() {
	m.stop = make(chan bool, 1)
	c := make(chan bool)
	m.setLastActivity(time.Now())

	go func() {
		m.heartbeat(c)
	}()
	<-c
}

func (m *member) stopHeartbeat(flush bool) {
	m.setLastActivity(time.Time{})
	m.stop <- flush
}

func (m *member) heartbeat(c chan bool) {
	stop := m.stop
	c <- true

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
			return
		case <-ticker.C:
			//start := time.Now()
			if err := m.flush(); err != nil {
				// 心跳失败
				log.Warnf("flush failed: %d", m.Id)
			}
			//duration := time.Now().Sub(start)
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
		err = m.sendSnapshotRequest()
	}
	return
}

func (m *member) dial() error {
	if m.cc != nil {
		if ping(m.cc, m.dialTimeout) {
			return nil
		}
		// ping failed, reconnect
	}

	cc, err := dial(m.Address, m.dialTimeout, m.dialOptions)
	if err != nil {
		// TODO: 移除节点
		return err
	}

	m.cc = cc
	return nil
}
