package csbufio

import (
	"bufio"
	"context"
	"io"
	"os"
)

func OpenReader(ctx context.Context, name string) (io.ReadCloser, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return NewReader(ctx, f), nil
}

func NewReader(ctx context.Context, rc io.ReadCloser) io.ReadCloser {
	return &bufReadCloser{ctx, bufio.NewReader(rc), rc}
}

type bufReadCloser struct {
	ctx context.Context
	r   io.Reader
	c   io.Closer
}

func (b *bufReadCloser) Read(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	return b.r.Read(p)
}

func (b *bufReadCloser) Close() error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	return b.c.Close()
}
