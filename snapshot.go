package raft

import (
	"bufio"
	"fmt"
	"github.com/treeforest/raft/pb"
	"hash/crc32"
	"os"
)

type Snapshot struct {
	pb.Snapshot
	Path string
}

func newSnapshot(path string, ss *pb.Snapshot) *Snapshot {
	return &Snapshot{Snapshot: *ss, Path: path}
}

func (ss *Snapshot) Save() error {
	file, err := os.OpenFile(ss.Path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	b, err := ss.Snapshot.Marshal()
	if err != nil {
		return err
	}

	w := bufio.NewWriter(file)

	if _, err = w.Write([]byte(fmt.Sprintf("%08x\n", crc32.ChecksumIEEE(b)))); err != nil {
		return err
	}

	if _, err = w.Write(b); err != nil {
		return err
	}

	return w.Flush()
}

func (ss *Snapshot) Remove() error {
	return os.Remove(ss.Path)
}

func (ss *Snapshot) Marshal() ([]byte, error) {
	return ss.Snapshot.Marshal()
}

func (ss *Snapshot) Unmarshal(b []byte) error {
	return ss.Snapshot.Unmarshal(b)
}
