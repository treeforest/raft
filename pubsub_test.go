package raft

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNewPubSub(t *testing.T) {
	ps := NewPubSub()
	sub := ps.Subscribe("hi", time.Second*2)

	go func() {
		err := ps.Publish("hi", "tony")
		require.NoError(t, err)
	}()

	val, err := sub.Listen()
	require.NoError(t, err)

	require.Equal(t, "tony", val)
}
