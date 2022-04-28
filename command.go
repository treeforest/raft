package raft

type Command struct {
	name  string
	data  []byte
	errCh chan error
}
