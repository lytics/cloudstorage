package csbufio

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReaderContextDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	m := memRWC([]byte("some-data"))
	rc := NewReader(ctx, &m)

	var p []byte
	n, err := rc.Read(p)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, n)
	require.Len(t, p, 0)

	err = rc.Close()
	require.ErrorIs(t, err, context.Canceled)
}

type memRWC []byte

func (m memRWC) Read(p []byte) (int, error) {
	n := len(p)
	if n > len(m) {
		n = len(m)
	}
	copy(p, m)
	return n, nil
}

func (m *memRWC) Write(p []byte) (int, error) {
	*m = append(*m, p...)
	return len(p), nil
}

func (m memRWC) Close() error {
	return nil
}
