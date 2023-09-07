package csbufio

import (
	"context"
	"testing"

	"github.com/acomagu/bufpipe"
	"github.com/stretchr/testify/require"
)

func TestReaderContextDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pr, pw := bufpipe.New([]byte("some-data"))
	pw.Close()
	rc, err := NewReader(ctx, pr)
	require.NoError(t, err)

	var p []byte
	n, err := rc.Read(p)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, n)
	require.Len(t, p, 0)

	err = rc.Close()
	require.ErrorIs(t, err, context.Canceled)
}
