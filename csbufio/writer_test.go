package csbufio

import (
	"bytes"
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
	wc := NewWriter(ctx, pw, false)

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

func TestRoundtrip(t *testing.T) {
	t.Parallel()

	numBytes := 1000
	testVal := bytes.Repeat([]byte("x"), numBytes)
	pr, pw := bufpipe.New(nil)
	wc := NewWriter(context.Background(), pw, false)

	n, err := wc.Write(testVal)
	require.NoError(t, err, "failed to write")
	err = wc.Close()
	require.NoError(t, err, "failed to close writer")
	require.Equal(t, numBytes, n, "wrong number of uncompressed bytes written")
	//require.Less(t, m.Length(), numBytes, "low entropy bytes did not compress")
	err = pw.Close()
	require.NoError(t, err, "failed to close writer, but the other one")

	rc := NewReader(context.Background(), pr)
	x, err := io.ReadAll(rc)
	require.NoError(t, err, "failed to read")
	//require.Equal(t, m.Length(), len(x), "wrong number of compressed bytes read")
	err = rc.Close()
	require.NoError(t, err, "failed to close reader")
	require.Equal(t, testVal, x)
}

func TestRoundtripWithCompression(t *testing.T) {
	t.Parallel()

	numBytes := 1000
	testVal := bytes.Repeat([]byte("x"), numBytes)
	pr, pw := bufpipe.New(nil)
	wc := NewWriter(context.Background(), pw, true)

	n, err := wc.Write(testVal)
	require.NoError(t, err, "failed to write")
	err = wc.Close()
	require.NoError(t, err, "failed to close writer")
	require.Equal(t, numBytes, n, "wrong number of uncompressed bytes written")
	//require.Less(t, m.Length(), numBytes, "low entropy bytes did not compress")
	err = pw.Close()
	require.NoError(t, err, "failed to close writer, but the other one")

	rc := NewReader(context.Background(), pr)
	x, err := io.ReadAll(rc)
	require.NoError(t, err, "failed to read")
	//require.Equal(t, m.Length(), len(x), "wrong number of compressed bytes read")
	err = rc.Close()
	require.NoError(t, err, "failed to close reader")
	require.Equal(t, testVal, x)
}
