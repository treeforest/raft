package raft

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDispatchEvent(t *testing.T) {
	var count int
	dispatcher := newEventDispatcher(nil)
	dispatcher.AddEventListener("foo", func(e Event) {
		count += 1
	})
	dispatcher.AddEventListener("foo", func(e Event) {
		count += 10
	})
	dispatcher.AddEventListener("bar", func(e Event) {
		count += 100
	})
	dispatcher.DispatchEvent(&event{typ: "foo", value: nil, prevValue: nil})
	require.Equal(t, 11, count)
}
