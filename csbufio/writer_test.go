package csbufio

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriterContextDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var m memRWC
	wc := NewWriter(ctx, &m)

	n, err := wc.Write([]byte("some-data"))
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, n)
	require.Len(t, m, 0)

	err = wc.Close()
	require.ErrorIs(t, err, context.Canceled)
}
