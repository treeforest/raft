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

	// MaxLogEntriesPerRequest 每次最多请求的日志条目，该参数与 HeartbeatTimeout 相关。
	// 由于心跳操作采用的是同步的机制，当发送 MaxLogEntriesPerRequest 条日志条目的时间
	// 大于 HeartbeatTimeout 时，将会导致集群再次进入投票环节，影响整体性能。大约200条日志
	// 条目在局域网内的RPC响应在400ms左右。
	MaxLogEntriesPerRequest uint64

	// NumberOfLogEntriesAfterSnapshot 在保存快照后仍然保留的日志条目数量
	NumberOfLogEntriesAfterSnapshot uint64

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

	// SubscribeTTL 订阅事件的超时时间
	SubscribeTTL time.Duration

	// ReplicationType 日志复制类型
	ReplicationType ReplicationType

	// LogWriteType 日志写磁盘的方式。包括同步写写磁盘；异步写磁盘。
	// 如果是异步写磁盘，则需要填写
	WriteType LogWriteType

	// LogWriteTimeInterval 异步写磁盘的时间间隔
	LogWriteTimeInterval time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		MemberId:                        uint64(snowflake.Generate()),
		Address:                         "localhost:4399",
		MaxLogEntriesPerRequest:         5000,
		NumberOfLogEntriesAfterSnapshot: 2000,
		HeartbeatInterval:               time.Millisecond * 100,
		HeartbeatTimeout:                time.Millisecond * 1000,
		ElectionTimeout:                 time.Millisecond * 1000,
		DialTimeout:                     time.Millisecond * 1000,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		ServerOptions: []grpc.ServerOption{
			grpc.Creds(insecure.NewCredentials()),
		},
		SnapshotPath:         ".",
		LogPath:              "./log",
		SubscribeTTL:         time.Second * 3,
		ReplicationType:      Synchronous,
		WriteType:            Async,
		LogWriteTimeInterval: time.Second,
	}
}
