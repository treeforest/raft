package raft

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"time"
)

type Config struct {
	// MaxLogEntriesPerRequest 每次最多请求的日志条目
	MaxLogEntriesPerRequest uint64

	// HeartbeatInterval 探测节点状态的间隔
	HeartbeatInterval time.Duration

	// ElectionTimeout 选举超时
	ElectionTimeout time.Duration

	// ElectionTimeoutThresholdPercent 选举超时的阈值
	ElectionTimeoutThresholdPercent float64

	// DialTimeout 拨号超时时间
	DialTimeout time.Duration

	// DialOptions 拨号参数设置
	DialOptions []grpc.DialOption

	// ServerOptions 服务端参数设置
	ServerOptions []grpc.ServerOption

	SnapshotPath string
}

func DefaultConfig() *Config {
	return &Config{
		HeartbeatInterval: time.Millisecond * 100,
		ElectionTimeout:   time.Millisecond * 150,
		DialTimeout:       time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		ServerOptions: []grpc.ServerOption{
			grpc.Creds(insecure.NewCredentials()),
		},
		SnapshotPath: "./snapshot",
	}
}
