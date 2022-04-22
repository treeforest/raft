package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gin-gonic/gin"
	log "github.com/treeforest/logger"
	"github.com/treeforest/raft"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
)

type Command struct {
	Key   string
	Value string
}

type Server struct {
	raft.StateMachine
	peer   raft.Raft
	r      *gin.Engine
	state  map[string]string
	locker sync.RWMutex
	pool   *sync.Pool
}

func New() *Server {
	return &Server{
		state:  map[string]string{},
		locker: sync.RWMutex{},
		pool: &sync.Pool{
			New: func() interface{} {
				return new(Command)
			},
		},
	}
}

// Save 读取状态机快照
func (s *Server) Save() ([]byte, error) {
	s.locker.RLock()
	defer s.locker.RUnlock()
	return json.Marshal(s.state)
}

// Recovery 从快照中恢复状态机状态
func (s *Server) Recovery(state []byte) error {
	s.locker.Lock()
	defer s.locker.Unlock()
	return json.Unmarshal(state, &s.state)
}

// Apply 状态机执行命令的回调函数
func (s *Server) Apply(commandName string, command []byte) {
	c := s.pool.Get().(*Command)
	_ = json.Unmarshal(command, c)
	switch commandName {
	case "set":
		s.state[c.Key] = c.Value
	case "del":
		delete(s.state, c.Key)
	}
	s.pool.Put(c)
}

func (s *Server) Serve(addr string) {
	r := gin.Default()
	r.POST("/set", func(c *gin.Context) {
		if !s.peer.IsLeader() {
			c.JSON(200, gin.H{"code": -1, "leader": s.peer.LeaderAddress()})
		}
		key := c.Query("key")
		val := c.Query("val")

	})

	s.r = r
	if err := s.r.Run(addr); err != nil {
		log.Error(err)
	}
}

func main() {
	port := flag.Int("port", 0, "raft server port")
	addr := flag.String("addr", "localhost:0", "web address")
	existing := flag.String("existing", "", "existing raft member")
	flag.Parse()

	s := New()

	config := raft.DefaultConfig()
	config.Address = fmt.Sprintf("localhost:%d", *port)
	config.LogPath = fmt.Sprintf("%d", *port)

	peer := raft.New(raft.DefaultConfig(), s)
	if err := peer.Start(); err != nil {
		log.Fatal(err)
	}

	peer.Join(strings.Split(*existing, ","))

	go func() {
		s.peer = peer
		s.Serve(*addr)
	}()

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, os.Kill)
	<-done

	srv.Stop()
	time.Sleep(time.Millisecond * 500)
}
