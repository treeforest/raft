package raft

import (
	"errors"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	log "github.com/treeforest/logger"
	"github.com/treeforest/raft/pb"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	commitIndexKey = "__commit_index_key__"
	startIndexKey  = "__start_index_key__"
	startTermKey   = "__start_term_key__"
	lastEntryIndex = "__last_entry_key__"
	currentTermKey = "__latest_term_key__"
)

type notify []<-chan struct{}

// Log a log is a collection of log entries that are persisted to durable storage.
type Log struct {
	ApplyFunc   func(commandName string, command []byte)
	levelDB     *leveldb.DB
	path        string
	entries     []pb.LogEntry
	commitIndex uint64
	mutex       sync.RWMutex
	startIndex  uint64 // the index before the first Entry in the Log entries
	startTerm   uint64
	currentTerm uint64
	initialized bool
	pubsub      *PubSub
}

// newLog creates a new log.
func newLog(path string, applyFunc func(string, []byte)) *Log {
	l := &Log{
		ApplyFunc: applyFunc,
		entries:   make([]pb.LogEntry, 0),
		pubsub:    NewPubSub(),
	}
	return l
}

func (l *Log) Subscribe(index uint64, ttl time.Duration) Subscription {
	return l.pubsub.Subscribe(strconv.FormatUint(index, 10), ttl)
}

func (l *Log) publish(index uint64) {
	_ = l.pubsub.Publish(strconv.FormatUint(index, 10), struct{}{})
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

func (l *Log) nextTerm() uint64 {
	return l.CurrentTerm() + 1
}

func (l *Log) CurrentTerm() uint64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.currentTerm
}

func (l *Log) SetCurrentTerm(term uint64) {
	err := l.levelDB.Put([]byte(currentTermKey), uint64ToBytes(term), nil)
	if err != nil {
		log.Fatal(err)
	}

	l.mutex.Lock()
	l.currentTerm = term
	l.mutex.Unlock()
}

func (l *Log) SetNextTerm() uint64 {
	term := l.nextTerm()
	l.SetCurrentTerm(term)
	return term
}

// open it opens the log levelDB and reads existing entries. The log can remain open and
// continue to append entries to the end of the log.
func (l *Log) open(path string) error {
	var err error
	// open log levelDB
	l.levelDB, err = leveldb.OpenFile(path, &opt.Options{})
	if err != nil {
		log.Fatalf("open levelDB failed: %v", err)
	}
	l.path = path

	// Read the levelDB and decode entries.
	l.startIndex, err = l.getUint64Value(startIndexKey)
	if err != nil {
		log.Fatal("get startIndex failed: ", err)
	}
	l.startTerm, err = l.getUint64Value(startTermKey)
	if err != nil {
		log.Fatal("get startTerm failed: ", err)
	}
	l.commitIndex, err = l.getUint64Value(commitIndexKey)
	if err != nil {
		log.Fatal("get commitIndex failed: ", err)
	}
	l.currentTerm, err = l.getUint64Value(currentTermKey)
	if err != nil {
		log.Fatal("get currentTerm failed: ", err)
	}

	index, end := l.startIndex+1, l.commitIndex
	var value []byte
	for index <= end {
		value, err = l.levelDB.Get(uint64ToBytes(index), nil)
		if err != nil {
			log.Fatalf("recover failed, get key error: %v ", err)
		}

		entry := pb.LogEntry{}
		if err = entry.Unmarshal(value); err != nil {
			log.Fatalf("recover failed, unmarshal entry error: %v", err)
		}

		if entry.Index != index {
			log.Fatalf("recover failed, entryIndex[%d] != index[%d]", entry.Index, index)
		}

		// Append Entry.
		l.entries = append(l.entries, entry)
		if entry.Index <= l.commitIndex {
			l.ApplyFunc(entry.CommandName, entry.Command)
		}
		log.Debug("append log index ", entry.Index)
		index++
	}

	log.Debug("recovery number of log ", len(l.entries))
	l.initialized = true
	return nil
}

func (l *Log) getUint64Value(key string) (uint64, error) {
	value, err := l.levelDB.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	return bytesToUint64(value)
}

// Closes the log levelDB.
func (l *Log) close() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.levelDB != nil {
		_ = l.levelDB.Close()
		l.levelDB = nil
	}
	l.entries = make([]pb.LogEntry, 0)
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
		panic(fmt.Sprintf("index is beyond end of log: %v %v", len(l.entries), index))
	}

	// If we're going from the beginning of the log then return the whole log.
	if index == l.startIndex {
		log.Debug("log.entriesAfter.beginning: ", index, " ", l.startIndex)
		return l.entries, l.startTerm
	}

	log.Debug("log.entriesAfter.partial: ", index, " ", l.entries[len(l.entries)-1].Index)

	entries := l.entries[index-l.startIndex:]
	length := len(entries)

	log.Debug("log.entriesAfter: startIndex:", l.startIndex, " length", len(l.entries))

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
	if index > l.commitIndex {
		l.commitIndex = index
	}
	log.Debug("update.commit.index ", index)
}

// Updates the commit index and writes entries after that index to the stable storage.
func (l *Log) setCommitIndex(index uint64) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if index > l.startIndex+uint64(len(l.entries)) {
		log.Debug("Commit index", index, "set back to ", len(l.entries))
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
		log.Infof("update commitIndex: %d", l.commitIndex)

		l.publish(entry.Index)

		// Apply the changes to the state machine and store the error code.
		l.ApplyFunc(entry.CommandName, entry.Command)

		log.Debugf("index: %v, entries index: %v", i, entryIndex)
	}
	return nil
}

// Set the commitIndex at the head of the log levelDB to the current
// commit Index. This should be called after obtained a log lock
func (l *Log) flushCommitIndex() {
	_ = l.levelDB.Put([]byte(commitIndexKey), uint64ToBytes(l.commitIndex), nil)
}

// truncate 截断index之后的未提交的所有日志条目，如果已提交或超出日志范围，则返回错误
func (l *Log) truncate(index uint64, term uint64) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	log.Debug("log.truncate: ", index)

	// 不允许截断已经提交的日志，即要求 index >= commitIndex
	if index < l.commitIndex {
		log.Debug("log.truncate.before")
		return fmt.Errorf("index is already committed (%v): (IDX=%v, TERM=%v)", l.commitIndex, index, term)
	}

	// 要截断的日志超出当前日志范围，返回错误
	if index > l.startIndex+uint64(len(l.entries)) {
		log.Debug("log.truncate.after")
		return fmt.Errorf("entry index does not exist (MAX=%v): (IDX=%v, TERM=%v)", len(l.entries), index, term)
	}

	// 开始截断操作
	if index == l.startIndex {
		log.Debug("log.truncate.clear")
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
			for i := index - l.startIndex; i < uint64(len(l.entries)); i++ {
				_ = l.levelDB.Delete(uint64ToBytes(l.entries[i].Index), nil)
			}
			l.entries = l.entries[0 : index-l.startIndex]
		}
	}

	return nil
}

// Appends a series of entries to the log.
func (l *Log) appendEntries(entries []pb.LogEntry) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	var err error
	// Append each Entry but exit if we hit an error.
	for _, entry := range entries {
		if err = l.writeEntry(&entry); err != nil {
			return err
		}
		log.Infof("append entry, term:%d index:%d", entry.Term, entry.Index)
	}

	if err != nil {
		panic(err)
	}

	return nil
}

func (l *Log) writeEntry(entry *pb.LogEntry) error {
	if l.levelDB == nil {
		return errors.New("log is not open")
	}

	// Make sure the term and index are greater than the previous.
	if len(l.entries) > 0 {
		lastEntry := l.entries[len(l.entries)-1]
		if entry.Term < lastEntry.Term {
			return fmt.Errorf("cannot append Entry with earlier term (%x:%x <= %x:%x)",
				entry.Term, entry.Index, lastEntry.Term, lastEntry.Index)
		} else if entry.Term == lastEntry.Term && entry.Index <= lastEntry.Index {
			return fmt.Errorf("cannot append Entry with earlier index in the same term (%x:%x <= %x:%x)",
				entry.Term, entry.Index, lastEntry.Term, lastEntry.Index)
		}
	}

	indexBytes := uint64ToBytes(entry.Index)
	data, _ := entry.Marshal()

	// 开启事务
	tx, err := l.levelDB.OpenTransaction()
	if err != nil {
		return fmt.Errorf("open transaction error: %v", err)
	}
	defer tx.Discard()

	_ = tx.Put([]byte(lastEntryIndex), indexBytes, nil)
	_ = tx.Put(indexBytes, data, nil)

	// 执行事务
	if err = tx.Commit(); err != nil {
		log.Warnf("commit transaction error: %v", err)
		return err
	}

	// Append to entries list if stored on disk.
	l.entries = append(l.entries, *entry)

	return nil
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
	levelDB, err := leveldb.OpenFile(newFilePath, nil)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		data, _ := entry.Marshal()
		err = levelDB.Put(uint64ToBytes(entry.Index), data, nil)
		if err != nil {
			_ = levelDB.Close()
			_ = os.Remove(newFilePath)
			return err
		}
	}

	if err = levelDB.Close(); err != nil {
		_ = os.Remove(newFilePath)
		return err
	}
	if err = os.Rename(newFilePath, l.path); err != nil {
		_ = os.Remove(newFilePath)
		return err
	}
	if err = l.levelDB.Close(); err != nil {
		_ = os.Remove(newFilePath)
		return err
	}

	l.levelDB, _ = leveldb.OpenFile(l.path, nil)
	l.entries = entries
	l.startIndex = index
	l.startTerm = term
	return nil
}
