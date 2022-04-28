package raft

import "sync"

type memberManager struct {
	sync.RWMutex
	memberMap map[uint64]*member
}

func newMemberManager() *memberManager {
	return &memberManager{memberMap: make(map[uint64]*member)}
}

func (mgr *memberManager) Load(id uint64) (*member, bool) {
	mgr.RLock()
	defer mgr.RUnlock()
	m, ok := mgr.memberMap[id]
	return m, ok
}

func (mgr *memberManager) Store(m *member) bool {
	mgr.Lock()
	defer mgr.Unlock()
	if _, ok := mgr.memberMap[m.Id]; ok {
		return false
	}
	mgr.memberMap[m.Id] = m
	return true
}

func (mgr *memberManager) Delete(id uint64) (*member, bool) {
	mgr.Lock()
	defer mgr.Unlock()
	m, ok := mgr.memberMap[id]
	if !ok {
		return nil, false
	}
	delete(mgr.memberMap, id)
	return m, true
}

func (mgr *memberManager) Exist(id uint64) bool {
	mgr.RLock()
	defer mgr.RUnlock()
	_, ok := mgr.memberMap[id]
	return ok
}

func (mgr *memberManager) Count() int {
	mgr.RLock()
	defer mgr.RUnlock()
	return len(mgr.memberMap)
}

func (mgr *memberManager) Range(fn func(*member) bool) {
	mgr.RLock()
	defer mgr.RUnlock()
	for _, m := range mgr.memberMap {
		if !fn(m) {
			return
		}
	}
}
