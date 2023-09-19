package csbufio

import (
	"context"
	"io"
	"testing"

	"github.com/acomagu/bufpipe"
	"github.com/stretchr/testify/require"
)

func TestWriterContextDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pr, pw := bufpipe.New(nil)
	wc := NewWriter(ctx, pw)

	n, err := wc.Write([]byte("some-data"))
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, n)
	err = pw.Close()
	require.NoError(t, err)

	b, err := io.ReadAll(pr)
	require.NoError(t, err, "error reading")
	require.Equal(t, 0, len(b), "")

	err = wc.Close()
	require.ErrorIs(t, err, context.Canceled)
}
