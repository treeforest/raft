package raft

type Command struct {
	name string
	data []byte
	c    chan uint64
}
