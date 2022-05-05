package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gin-gonic/gin"
	log "github.com/treeforest/logger"
	"github.com/treeforest/raft"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
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
	peer    raft.Raft
	r       *gin.Engine
	state   map[string]string
	locker  sync.RWMutex
	cmdPool sync.Pool
}

func New() *Server {
	s := &Server{
		state:  map[string]string{},
		locker: sync.RWMutex{},
		cmdPool: sync.Pool{
			New: func() any {
				return new(Command)
			},
		},
	}
	return s
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
	return
	//cmd := s.cmdPool.Get().(*Command)
	//_ = json.Unmarshal(command, &cmd)
	//
	//switch commandName {
	//case SET:
	//	s.state[cmd.Key] = cmd.Value
	//case DEL:
	//	delete(s.state, cmd.Key)
	//}
	//
	//*cmd = Command{}
	//s.cmdPool.Put(cmd)
}

func (s *Server) Serve(addr string) {
	r := gin.Default()
	r.POST("/test", func(c *gin.Context) {
		cmd := &Command{}
		cmd.Key = "hello"
		cmd.Value = "world"
		data, _ := json.Marshal(cmd)

		n := 10000
		succ := int32(0)
		errCh := make(chan error, 100)
		done := make(chan struct{}, 1)
		go func() {
			for {
				select {
				case err := <-errCh:
					log.Debug(err)
				case <-done:
					return
				}
			}
		}()

		wg := sync.WaitGroup{}
		wg.Add(n)
		since := time.Now()
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				err := <-s.peer.Do(SET, data)
				if err == nil {
					atomic.AddInt32(&succ, 1)
				} else {
					errCh <- err
				}
			}()
		}
		wg.Wait()
		used := time.Now().Sub(since).Milliseconds()
		done <- struct{}{}

		s.peer.TakeSnapshot()

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

		cmd := &Command{}
		cmd.Key = c.Query("key")
		cmd.Value = c.Query("value")
		data, _ := json.Marshal(cmd)

		err := <-s.peer.Do(SET, data)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": -1, "error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"code": 0})
	})
	r.POST("/del", func(c *gin.Context) {
		if !s.peer.IsLeader() {
			c.JSON(http.StatusOK, gin.H{"code": -1, "leader": s.peer.LeaderAddress()})
			return
		}

		cmd := &Command{}
		cmd.Key = c.Query("key")
		data, _ := json.Marshal(cmd)

		err := <-s.peer.Do(SET, data)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": -1, "error": err.Error()})
			return
		}

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
	openPprof := flag.Bool("p", false, "open pprof")
	flag.Parse()

	if *openPprof {
		go func() {
			if err := http.ListenAndServe(":8080", nil); err != nil {
				panic(any(err))
			}
		}()
	}

	s := New()
	log.SetLevel(log.INFO)

	config := raft.DefaultConfig()
	config.MemberId = uint64(*port)
	config.Address = fmt.Sprintf("localhost:%d", *port)
	config.LogPath = filepath.Join(".", "log", fmt.Sprintf("%d.rdb", *port))
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
	go func() {
		peer.Stop()
	}()
	time.Sleep(time.Millisecond * 1000)
}
