package raft

import (
	"context"
	"errors"
	"github.com/treeforest/raft/pb"
	"google.golang.org/grpc"
	"math/rand"
	"strconv"
	"time"
)

type Uint64Slice []uint64

func (x Uint64Slice) Len() int           { return len(x) }
func (x Uint64Slice) Less(i, j int) bool { return x[i] < x[j] }
func (x Uint64Slice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// afterBetween 随机等待
func afterBetween(min time.Duration, max time.Duration) <-chan time.Time {
	rd := rand.New(rand.NewSource(time.Now().UnixNano()))
	d, delta := min, max-min
	if delta > 0 {
		d += time.Duration(rd.Int63n(int64(delta)))
	}
	return time.After(d)
}

func uint64ToBytes(v uint64) []byte {
	return []byte(strconv.FormatUint(v, 10))
}

func bytesToUint64(b []byte) (uint64, error) {
	return strconv.ParseUint(string(b), 10, 64)
}

func ping(cc *grpc.ClientConn, dialTimeout time.Duration) bool {
	var err error
	_ = timeoutFunc(dialTimeout, func() error {
		_, err = pb.NewRaftClient(cc).Ping(context.Background(), &pb.Empty{})
		return err
	})
	if err != nil {
		return false
	}
	return true
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func timeoutFunc(timeout time.Duration, fn func() error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- fn()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return errors.New("timeout")
	}
}
