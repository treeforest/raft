package raft

import (
	"bufio"
	"errors"
	"fmt"
	log "github.com/treeforest/logger"
	"github.com/treeforest/raft/pb"
	"io"
	"os"
	"sync"
	"time"
)

/*
日志文件结构：
commitIndex(16字节) + /n: commitIndex
日志条目：
	length(8字节) + /n
	target(长度为length)
*/

type LogWriteType int

const (
	// Sync 同步写
	Sync LogWriteType = iota
	// Async 异步写
	Async
	// Auto 由操作系统自动将缓存写到磁盘。当缓存写满了之后，操作系统自动写到磁盘。
	Auto
)

// Log a log is a collection of log entries that are persisted to durable storage.
type Log struct {
	ApplyFunc         func(commandName string, command []byte)
	flushTimeInterval time.Duration // 将文件缓冲区的日志刷新到磁盘的时间间隔
	writeType         LogWriteType  // 0：同步写；1：异步写；
	mutex             sync.RWMutex
	file              *os.File
	w                 *bufio.Writer
	path              string
	entries           []pb.LogEntry
	positions         []int64 // 日志条目的位置
	commitIndex       uint64
	startIndex        uint64 // the index before the first Entry in the Log entries
	startTerm         uint64
	initialized       bool
	ps                *PubSub
	stop              chan struct{}
}

// newLog creates a new log.
func newLog(wt LogWriteType, fti time.Duration, applyFunc func(string, []byte)) *Log {
	l := &Log{
		ApplyFunc:         applyFunc,
		flushTimeInterval: fti,
		writeType:         wt,
		entries:           make([]pb.LogEntry, 0, 1024*1024),
		positions:         make([]int64, 0, 1024*1024),
		startIndex:        0,
		startTerm:         0,
		initialized:       false,
		ps:                NewPubSub(),
	}
	return l
}

func (l *Log) asyncFlush() {
	t := time.NewTicker(l.flushTimeInterval)
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			select {
			case <-l.stop:
				return
			default:
				if err := l.file.Sync(); err != nil {
					panic(any(err))
				}
			}
		}
	}
}

func (l *Log) Subscribe(index uint64, ttl time.Duration) Subscription {
	return l.ps.Subscribe(index, ttl)
}

func (l *Log) publish(index uint64) {
	_ = l.ps.Publish(index, struct{}{})
}

// CommitIndex the last committed index in the log.
func (l *Log) CommitIndex() uint64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.commitIndex
}

// CurrentIndex the current index in the log.
func (l *Log) CurrentIndex() uint64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.internalCurrentIndex()
}

// internalCurrentIndex the current index in the log without locking
func (l *Log) internalCurrentIndex() uint64 {
	if len(l.entries) == 0 {
		return l.startIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// nextIndex the next index in the log.
func (l *Log) nextIndex() uint64 {
	return l.CurrentIndex() + 1
}

// isEmpty determines if the log contains zero entries.
func (l *Log) isEmpty() bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return (len(l.entries) == 0) && (l.startIndex == 0)
}

func (l *Log) currentTerm() uint64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if len(l.entries) == 0 {
		return l.startTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// open it opens the log levelDB and reads existing entries. The log can remain open and
// continue to append entries to the end of the log.
func (l *Log) open(path string) error {
	var err error
	// open log levelDB
	l.file, err = os.OpenFile(path, os.O_RDWR, 0600)
	l.path = path

	if err != nil {
		if os.IsNotExist(err) {
			l.file, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0600)
			if err == nil {
				// step over commit index
				_, _ = l.file.Seek(16+1, os.SEEK_SET)
				l.w = bufio.NewWriter(l.file)
				l.initialized = true

				if l.writeType == Async {
					go l.asyncFlush()
				}
			}
			return err
		}
		return err
	}

	l.w = bufio.NewWriter(l.file)
	if l.writeType == Async {
		go l.asyncFlush()
	}

	r := bufio.NewReader(l.file)

	// load commit index
	_, err = fmt.Fscanf(r, "%016x\n", &l.commitIndex)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		log.Errorf("load commit index failed: %v", err)
		return err
	}
	log.Info("load commitIndex: ", l.commitIndex)

	var readBytes int64 = 16 + 1
	var n int

	for {
		position, _ := l.file.Seek(0, os.SEEK_CUR)

		entry := pb.LogEntry{}

		n, err = l.decodeLogEntry(r, &entry)
		if err != nil {
			if err != io.EOF {
				log.Error("decode log entry failed: ", err)
				l.commitIndex = 0
				if err = os.Truncate(path, readBytes); err != nil {
					return fmt.Errorf("unable recover: %v", err)
				}
			}
			break
		}

		if entry.Index > l.startIndex {
			// append entry
			l.entries = append(l.entries, entry)
			l.positions = append(l.positions, position)
			if entry.Index <= l.commitIndex {
				l.ApplyFunc(entry.CommandName, entry.Command)
			}
		}

		readBytes += int64(n)
	}

	log.Debug("recovery number of log ", len(l.entries))
	l.initialized = true
	return nil
}

// Closes the log file.
func (l *Log) close() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.stop <- struct{}{}

	_ = l.file.Sync()

	if l.file != nil {
		_ = l.file.Close()
		l.file = nil
	}
	l.entries = []pb.LogEntry{}
}

// getEntry retrieves an Entry from the log. If the Entry has been eliminated
// because of a snapshot then nil is returned.
func (l *Log) getEntry(index uint64) *pb.LogEntry {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if index <= l.startIndex || index > (l.startIndex+uint64(len(l.entries))) {
		return nil
	}
	return &l.entries[index-l.startIndex-1]
}

// containsEntry checks if the log contains a given index/term combination.
func (l *Log) containsEntry(index uint64, term uint64) bool {
	entry := l.getEntry(index)
	return entry != nil && entry.Term == term
}

// getEntriesAfter retrieves a list of entries after a given index as well as the term of the
// index provided. A nil list of entries is returned if the index no longer
// exists because a snapshot was made.
func (l *Log) getEntriesAfter(index uint64, maxLogEntriesPerRequest uint64) ([]pb.LogEntry, uint64) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	// Return nil if index is before the start of the log.
	if index < l.startIndex {
		log.Debug("log.entriesAfter.before: ", index, " ", l.startIndex)
		return nil, 0
	}

	// Return an error if the index doesn't exist.
	if index > (uint64(len(l.entries)) + l.startIndex) {
		panic(any(fmt.Sprintf("index is beyond end of log: %v %v", len(l.entries), index)))
	}

	// If we're going from the beginning of the log then return the whole log.
	if index == l.startIndex {
		// log.Debug("log.entriesAfter.beginning: ", index, " ", l.startIndex)
		if uint64(len(l.entries)) < maxLogEntriesPerRequest {
			return l.entries, l.startTerm
		} else {
			return l.entries[:maxLogEntriesPerRequest], l.startTerm
		}
	}

	entries := l.entries[index-l.startIndex:]
	length := len(entries)

	if uint64(length) < maxLogEntriesPerRequest {
		// Determine the term at the given Entry and return a subslice.
		return entries, l.entries[index-1-l.startIndex].Term
	} else {
		return entries[:maxLogEntriesPerRequest], l.entries[index-1-l.startIndex].Term
	}
}

// commitInfo retrieves the last index and term that has been committed to the log.
func (l *Log) commitInfo() (index uint64, term uint64) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	// If we don't have any committed entries then just return zeros.
	if l.commitIndex == 0 {
		return 0, 0
	}

	// No new commit log after snapshot
	if l.commitIndex == l.startIndex {
		return l.startIndex, l.startTerm
	}

	// Return the last index & term from the last committed Entry.
	log.Debug("commitInfo.get.[", l.commitIndex, "/", l.startIndex, "]")
	entry := l.entries[l.commitIndex-1-l.startIndex]
	return entry.Index, entry.Term
}

// Retrieves the last index and term that has been appended to the log.
func (l *Log) lastInfo() (index uint64, term uint64) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	// If we don't have any entries then just return zeros.
	if len(l.entries) == 0 {
		return l.startIndex, l.startTerm
	}

	// Return the last index & term
	entry := l.entries[len(l.entries)-1]
	return entry.Index, entry.Term
}

// Updates the commit index
func (l *Log) updateCommitIndex(index uint64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if index < l.commitIndex {
		return
	}

	l.commitIndex = index
	l.flushCommitIndex()
	log.Debug("update.commit.index ", index)
}

// Updates the commit index and writes entries after that index to the stable storage.
func (l *Log) setCommitIndex(index uint64) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if index > l.startIndex+uint64(len(l.entries)) {
		log.Debugf("commit index %d is more, set back to %d", index, l.startIndex+uint64(len(l.entries)))
		index = l.startIndex + uint64(len(l.entries))
	}

	if index < l.commitIndex {
		return nil
	}

	// Find all entries whose index is between the previous index and the current index.
	for i := l.commitIndex + 1; i <= index; i++ {
		entryIndex := i - 1 - l.startIndex
		entry := l.entries[entryIndex]

		// Update commit index.
		l.commitIndex = entry.Index
		l.flushCommitIndex()
		// log.Debugf("update commitIndex: %d", l.commitIndex)

		l.publish(entry.Index)

		// Apply the changes to the state machine and store the error code.
		l.ApplyFunc(entry.CommandName, entry.Command)

		// log.Debugf("index: %v, entries index: %v", i, entryIndex)
	}
	return nil
}

// Set the commitIndex at the head of the log levelDB to the current
// commit Index. This should be called after obtained a log lock
func (l *Log) flushCommitIndex() {
	_, _ = l.file.Seek(0, os.SEEK_SET)
	_, _ = fmt.Fprintf(l.file, "%016x\n", l.commitIndex)
	_, _ = l.file.Seek(0, os.SEEK_END)
}

// truncate 截断index之后的未提交的所有日志条目，如果已提交或超出日志范围，则返回错误
func (l *Log) truncate(index uint64, term uint64) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	// log.Debug("log.truncate: ", index)

	// 不允许截断已经提交的日志，即要求 index >= commitIndex
	if index < l.commitIndex {
		log.Debug("log.truncate.before")
		return fmt.Errorf("index is already committed (%v): (IDX=%v, TERM=%v)", l.commitIndex, index, term)
	}

	// 要截断的日志超出当前日志范围，返回错误
	if index > l.startIndex+uint64(len(l.entries)) {
		log.Debug("log.truncate.after")
		return fmt.Errorf("entry index does not exist : (startIndex:%d, entries=%d, index=%v, term=%v)", l.startIndex, len(l.entries), index, term)
	}

	// 开始截断操作
	if index == l.startIndex {
		// log.Debug("log.truncate.clear")
		_ = l.file.Truncate(0)
		_, _ = l.file.Seek(16+1, os.SEEK_SET)
		l.entries = []pb.LogEntry{}
	} else {
		// 相同索引，却是不同任期，则不进行截断，并返回错误
		entry := l.entries[index-l.startIndex-1]
		if len(l.entries) > 0 && entry.Term != term {
			log.Debug("log.truncate.termMismatch")
			return fmt.Errorf("entry at index does not have matching term (%v): (IDX=%v, TERM=%v)", entry.Term, index, term)
		}

		// 截断所要求的条目
		if index < l.startIndex+uint64(len(l.entries)) {
			log.Debug("log.truncate.finish")

			position := l.positions[index-l.startIndex]
			_ = l.file.Truncate(position)
			_, _ = l.file.Seek(position, os.SEEK_SET)

			l.entries = l.entries[0 : index-l.startIndex]
			l.positions = l.positions[0 : index-l.startIndex]
		}
	}

	return nil
}

// Appends a series of entries to the log.
func (l *Log) appendEntries(entries []pb.LogEntry) error {
	if l.file == nil {
		return errors.New("log is not open")
	}

	if len(entries) == 0 {
		return nil
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	startPosition, _ := l.file.Seek(0, os.SEEK_CUR)

	var size int64
	var err error

	for _, entry := range entries {
		l.positions = append(l.positions, startPosition)
		if size, err = l.writeEntry(&entry, l.w); err != nil {
			return err
		}
		startPosition += size
	}

	if err = l.w.Flush(); err != nil {
		panic(any(err))
	}

	if l.writeType == Sync {
		if err = l.file.Sync(); err != nil {
			panic(any(err))
		}
	}

	return nil
}

func (l *Log) writeEntry(entry *pb.LogEntry, w *bufio.Writer) (int64, error) {
	if l.file == nil {
		return -1, errors.New("log is not open")
	}

	// Make sure the term and index are greater than the previous.
	if len(l.entries) > 0 {
		lastEntry := l.entries[len(l.entries)-1]
		if entry.Term < lastEntry.Term {
			return -1, fmt.Errorf("cannot append Entry with earlier term (%x:%x <= %x:%x)",
				entry.Term, entry.Index, lastEntry.Term, lastEntry.Index)
		} else if entry.Term == lastEntry.Term && entry.Index <= lastEntry.Index {
			return -1, fmt.Errorf("cannot append Entry with earlier index in the same term (%x:%x <= %x:%x)",
				entry.Term, entry.Index, lastEntry.Term, lastEntry.Index)
		}
	}

	size, err := l.encodeLogEntry(w, entry)
	if err != nil {
		return -1, err
	}

	// Append to entries list if stored on disk.
	l.entries = append(l.entries, *entry)

	return int64(size), nil
}

// compact 清除index之前的日志条目，起到压缩的目的
func (l *Log) compact(index, term uint64) error {
	var entries []pb.LogEntry

	l.mutex.Lock()
	defer l.mutex.Unlock()

	if index == 0 {
		return nil
	}

	if index >= l.internalCurrentIndex() {
		entries = make([]pb.LogEntry, 0)
	} else {
		entries = l.entries[index-l.startIndex:]
	}

	newFilePath := l.path + ".new"
	file, err := os.OpenFile(newFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(file)
	positions := make([]int64, len(entries))
	for i, entry := range entries {
		positions[i], _ = l.file.Seek(0, os.SEEK_CUR)

		if _, err = l.encodeLogEntry(w, &entry); err != nil {
			_ = file.Close()
			_ = os.Remove(newFilePath)
			return err
		}
	}
	if err = w.Flush(); err != nil {
		_ = file.Close()
		_ = os.Remove(newFilePath)
		return err
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(newFilePath)
		return err
	}

	oldFile := l.file

	err = os.Rename(newFilePath, l.path)
	if err != nil {
		_ = file.Close()
		_ = os.Remove(newFilePath)
		return err
	}
	l.file = file
	l.w = w

	_ = oldFile.Close()

	l.entries = entries
	l.positions = positions
	l.startIndex = index
	l.startTerm = term
	return nil
}

func (l *Log) encodeLogEntry(w io.Writer, e *pb.LogEntry) (int, error) {
	b, err := e.Marshal()
	if err != nil {
		return -1, err
	}

	if _, err = fmt.Fprintf(w, "%08x", len(b)); err != nil {
		return -1, err
	}

	return w.Write(b)
}

func (l *Log) decodeLogEntry(r io.Reader, e *pb.LogEntry) (int, error) {
	var length int = 0

	_, err := fmt.Fscanf(r, "%08x", &length)
	if err != nil {
		return -1, err
	}

	data := make([]byte, length)

	_, err = io.ReadFull(r, data)
	if err != nil {
		return -1, err
	}

	if err = e.Unmarshal(data); err != nil {
		return -1, err
	}

	return length + 8 + 1, nil
}
