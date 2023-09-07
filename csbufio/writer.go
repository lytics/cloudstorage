package csbufio

import (
	"bufio"
	"context"
	"io"
	"os"
)

type bufWriteCloser struct {
	ctx context.Context
	w   *bufio.Writer
	c   io.Closer
}

func OpenWriter(ctx context.Context, name string, enableCompression bool) (io.WriteCloser, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0665)
	if err != nil {
		return nil, err
	}
	return NewWriter(ctx, f), nil
}

// NewWriter is a io.WriteCloser.
func NewWriter(ctx context.Context, rc io.WriteCloser) io.WriteCloser {
	return &bufWriteCloser{ctx, bufio.NewWriter(rc), rc}
}

func (b *bufWriteCloser) Write(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	return b.w.Write(p)
}

func (b *bufWriteCloser) Close() error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if err := b.w.Flush(); err != nil {
		return err
	}
	return b.c.Close()
}
