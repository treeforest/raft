package raft

import (
	"errors"
	"sync"
	"time"
)

type PubSub struct {
	sync.RWMutex
	subscriptions map[string]*Set // topic -> subscriptions
}

type subscription struct {
	top string
	ttl time.Duration
	c   chan interface{}
}

type Subscription interface {
	Listen() (interface{}, error)
}

// Listen 监听订阅的结果，超时返回timed out
func (s *subscription) Listen() (interface{}, error) {
	select {
	case <-time.After(s.ttl):
		return nil, errors.New("timed out")
	case item := <-s.c:
		return item, nil
	}
}

func NewPubSub() *PubSub {
	return &PubSub{
		subscriptions: make(map[string]*Set),
	}
}

const (
	subscriptionBuffSize = 50
)

// Publish 发布topic对应的item，监听topic的所有订阅者都将收到item
func (ps *PubSub) Publish(topic string, item interface{}) error {
	ps.RLock()
	defer ps.RUnlock()
	s, subscribed := ps.subscriptions[topic]
	if !subscribed {
		return errors.New("no subscribers")
	}
	for _, sub := range s.ToArray() {
		c := sub.(*subscription).c
		// Not enough room in buffer, continue in order to not block publisher
		if len(c) == subscriptionBuffSize {
			continue
		}
		c <- item
	}
	return nil
}

// Subscribe 订阅事件，返回一个Listen接口
func (ps *PubSub) Subscribe(topic string, ttl time.Duration) Subscription {
	sub := &subscription{
		top: topic,
		ttl: ttl,
		c:   make(chan interface{}, subscriptionBuffSize),
	}

	ps.Lock()
	// Store subscription to subscriptions map
	s, exists := ps.subscriptions[topic]
	// If no subscription set for the topic exists, create one
	if !exists {
		s = NewSet()
		ps.subscriptions[topic] = s
	}
	ps.Unlock()

	// Store the subscription
	s.Add(sub)

	// When the timeout expires, remove the subscription
	time.AfterFunc(ttl, func() {
		ps.unSubscribe(sub)
	})
	return sub
}

func (ps *PubSub) unSubscribe(sub *subscription) {
	ps.Lock()
	defer ps.Unlock()
	ps.subscriptions[sub.top].Remove(sub)
	if ps.subscriptions[sub.top].Size() != 0 {
		return
	}
	// Else, this is the last subscription for the topic.
	// Remove the set from the subscriptions map
	delete(ps.subscriptions, sub.top)
}

type Set struct {
	items map[interface{}]struct{}
	lock  *sync.RWMutex
}

func NewSet() *Set {
	return &Set{lock: &sync.RWMutex{}, items: make(map[interface{}]struct{})}
}

func (s *Set) Add(item interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.items[item] = struct{}{}
}

func (s *Set) Exists(item interface{}) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, exists := s.items[item]
	return exists
}

func (s *Set) Size() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.items)
}

func (s *Set) ToArray() []interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()
	a := make([]interface{}, len(s.items))
	i := 0
	for item := range s.items {
		a[i] = item
		i++
	}
	return a
}

func (s *Set) Clear() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.items = make(map[interface{}]struct{})
}

func (s *Set) Remove(item interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.items, item)
}
