package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gin-gonic/gin"
	log "github.com/treeforest/logger"
	"github.com/treeforest/raft"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
)

const (
	SET string = "set"
	DEL string = "del"
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
	cmd := s.pool.Get().(*Command)
	_ = json.Unmarshal(command, cmd)
	log.Debugf("commandName:%s key:%s value:%s", commandName, cmd.Key, cmd.Value)
	switch commandName {
	case SET:
		s.state[cmd.Key] = cmd.Value
	case DEL:
		delete(s.state, cmd.Key)
	}
	s.pool.Put(cmd)
}

func (s *Server) Serve(addr string) {
	r := gin.Default()
	r.POST("/test", func(c *gin.Context) {
		cmd := s.pool.Get().(*Command)
		cmd.Key = "hello"
		cmd.Value = "world"
		data, _ := json.Marshal(cmd)

		n := 1000
		succ := int32(0)
		wg := sync.WaitGroup{}
		wg.Add(n)
		since := time.Now()
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				_, err := s.peer.Do(SET, data)
				if err == nil {
					atomic.AddInt32(&succ, 1)
				}
			}()
		}
		wg.Wait()
		used := time.Now().Sub(since).Milliseconds()

		c.JSON(http.StatusOK, gin.H{
			"code": 0,
			"detail": fmt.Sprintf("count:%d success:%d used:%dms average:%dms",
				n, succ, used, used/int64(n)),
		})
	})
	r.POST("/set", func(c *gin.Context) {
		if !s.peer.IsLeader() {
			c.JSON(http.StatusOK, gin.H{"code": -1, "leader": s.peer.LeaderAddress()})
			return
		}

		cmd := s.pool.Get().(*Command)
		cmd.Key = c.Query("key")
		cmd.Value = c.Query("value")
		data, _ := json.Marshal(cmd)
		index, err := s.peer.Do(SET, data)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": -1, "error": err.Error()})
			return
		}

		log.Infof("index=%d currentIndex=%d", index, s.peer.CurrentIndex())
		c.JSON(http.StatusOK, gin.H{"code": 0})
	})
	r.POST("/del", func(c *gin.Context) {
		if !s.peer.IsLeader() {
			c.JSON(http.StatusOK, gin.H{"code": -1, "leader": s.peer.LeaderAddress()})
			return
		}

		cmd := s.pool.Get().(*Command)
		cmd.Key = c.Query("key")
		data, _ := json.Marshal(cmd)

		index, err := s.peer.Do(SET, data)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": -1, "error": err.Error()})
			return
		}

		log.Info("index=", index)
		c.JSON(http.StatusOK, gin.H{"code": 0})
	})
	r.GET("/get", func(c *gin.Context) {
		key := c.Query("key")
		if val, ok := s.state[key]; ok {
			c.JSON(http.StatusOK, gin.H{"code": 0, "value": val})
			return
		}
		c.JSON(http.StatusOK, gin.H{"code": -1, "error": "not found"})
	})

	s.r = r
	if err := s.r.Run(addr); err != nil {
		log.Error(err)
	}
}

func main() {
	port := flag.Int("port", 0, "raft server port")
	addr := flag.String("addr", "", "web address")
	existing := flag.String("existing", "", "existing raft member")
	flag.Parse()

	s := New()
	log.SetLevel(log.INFO)

	config := raft.DefaultConfig()
	config.MemberId = uint64(*port)
	config.Address = fmt.Sprintf("0.0.0.0:%d", *port)
	config.LogPath = fmt.Sprintf("%d", *port)
	config.URL = `http://` + *addr

	peer := raft.New(config, s)

	go func() {
		s.peer = peer
		if *addr == "" {
			return
		}
		s.Serve(*addr)
	}()
	time.Sleep(time.Millisecond * 50)

	if err := peer.Start(); err != nil {
		log.Fatal(err)
	}

	if *existing != "" {
		peer.Join(*existing)
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, os.Kill)
	<-done
	peer.Stop()
	time.Sleep(time.Millisecond * 500)
}
