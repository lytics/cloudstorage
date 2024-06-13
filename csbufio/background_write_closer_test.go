package csbufio_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lytics/cloudstorage/csbufio"
)

func TestBackgroundWriteCloser(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		label         string
		mustCancelCtx bool
		closeErrMsg   string
	}{
		{
			label:         "test success on write and close",
			mustCancelCtx: false,
		},
		{
			label:         "test success on write but close return error",
			mustCancelCtx: true,
			closeErrMsg:   "error from background job: context canceled",
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			if tc.mustCancelCtx {
				cancel()
			}

			var dataSentToCloud []byte
			wc := csbufio.NewBackgroundWriteCloser(ctx, func(ctx context.Context, rc io.ReadCloser) error {
				defer rc.Close()

				data, err := io.ReadAll(rc)
				if err != nil {
					return err
				}

				dataSentToCloud = data

				return ctx.Err()
			})

			_, err := wc.Write([]byte("foo"))
			require.NoError(t, err)

			err = wc.Close()
			if tc.closeErrMsg != "" {
				require.EqualError(t, err, tc.closeErrMsg)

				return
			}

			require.NoError(t, err)

			assert.Equal(t, []byte("foo"), dataSentToCloud)
		})
	}
}

func TestBackgroundWriteCloserCheckWriteAfterClose(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	wc := csbufio.NewBackgroundWriteCloser(ctx, func(ctx context.Context, rc io.ReadCloser) error {
		defer rc.Close()

		return nil
	})

	require.NoError(t, wc.Close())

	_, err := wc.Write([]byte("foo"))
	require.EqualError(t, err, "writer already closed")
}

func TestBackgroundWriteCloserCheckMultipleCloses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	wc := csbufio.NewBackgroundWriteCloser(ctx, func(ctx context.Context, rc io.ReadCloser) error {
		defer rc.Close()

		return nil
	})

	require.NoError(t, wc.Close())

	require.NoError(t, wc.Close(), "close an already closed wc should not trigger an error")
}
