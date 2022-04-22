package raft

import (
	"github.com/treeforest/raft/pb"
	"sync"
	"time"
)

type Peer struct {
	pb.Peer
	sync.RWMutex
	server            *server
	prevLogIndex      uint64
	stopChan          chan bool
	heartbeatInterval time.Duration
	lastActivity      time.Time
}

func newPeer(server *server, id uint64, connectionString string,
	heartbeatInterval time.Duration) *Peer {
	return &Peer{
		Peer: pb.Peer{
			Id:               id,
			ConnectionString: connectionString,
		},
		server:            server,
		heartbeatInterval: heartbeatInterval,
	}
}

func (p *Peer) setHeartbeatInterval(duration time.Duration) {
	p.heartbeatInterval = duration
}

func (p *Peer) getPrevLogIndex() uint64 {
	p.RLock()
	defer p.RUnlock()
	return p.prevLogIndex
}

func (p *Peer) setPrevLogIndex(index uint64) {
	p.Lock()
	defer p.Unlock()
	p.prevLogIndex = index
}

func (p *Peer) setLastActivity(now time.Time) {
	p.Lock()
	defer p.Unlock()
	p.lastActivity = now
}

func (p *Peer) LastActivity() time.Time {
	p.RLock()
	defer p.RUnlock()
	return p.lastActivity
}

func (p *Peer) startHeartbeat() {
	p.stopChan = make(chan bool)
	c := make(chan bool)
	p.setLastActivity(time.Now())

	p.server.routineGroup.Add(1)
	go func() {
		defer p.server.routineGroup.Done()

	}()
	<-c
}

func (p *Peer) stopHeartbeat(flush bool) {
	p.setLastActivity(time.Now())
	p.stopChan <- flush
}

func (p *Peer) clone() *Peer {
	p.RLock()
	defer p.RUnlock()
	return &Peer{
		Peer:         p.Peer,
		prevLogIndex: p.prevLogIndex,
		lastActivity: p.lastActivity,
	}
}

func (p *Peer) heartbeat(c chan bool) {
	stopChan := p.stopChan
	c <- true

	ticker := time.Tick(p.heartbeatInterval)
	for {
		select {
		case flush := <-stopChan:
			if flush {
				// 必须先将remove命令写到节点中
				return
			} else {

			}
			return
		case <-ticker:
			start := time.Now()

		}
	}
}

func (p *Peer) flush() {
	prevLogIndex := p.getPrevLogIndex()
	term := p.server.currentTerm

}
