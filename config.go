package raft

import (
	"github.com/treeforest/snowflake"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"time"
)

type Config struct {
	// MemberId 节点的id
	MemberId uint64

	// Address 节点的监听地址
	Address string

	// URL 节点相关的url地址
	URL string

	// MaxLogEntriesPerRequest 每次最多请求的日志条目
	MaxLogEntriesPerRequest uint64

	// HeartbeatInterval 探测节点状态的间隔
	HeartbeatInterval time.Duration

	// HeartbeatTimeout 心跳超时时间
	HeartbeatTimeout time.Duration

	// ElectionTimeout 选举超时(T~2T)
	ElectionTimeout time.Duration

	// DialTimeout 拨号超时时间
	DialTimeout time.Duration

	// DialOptions 拨号参数设置
	DialOptions []grpc.DialOption

	// ServerOptions 服务端参数设置
	ServerOptions []grpc.ServerOption

	// SnapshotPath 快照存储路径
	SnapshotPath string

	// LogPath 日志文件路径
	LogPath string

	// PullMembershipInterval 同步成员信息的间隔时间
	PullMembershipInterval time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		MemberId:                uint64(snowflake.Generate()),
		Address:                 "localhost:4399",
		MaxLogEntriesPerRequest: 40,
		HeartbeatInterval:       time.Millisecond * 100,
		HeartbeatTimeout:        time.Millisecond * 250,
		ElectionTimeout:         time.Millisecond * 300,
		DialTimeout:             time.Millisecond * 300,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		ServerOptions: []grpc.ServerOption{
			grpc.Creds(insecure.NewCredentials()),
		},
		SnapshotPath:           "./snapshot",
		LogPath:                "./log",
		PullMembershipInterval: time.Second * 5,
	}
}
