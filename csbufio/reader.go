package csbufio

import (
	"bufio"
	"context"
	"io"
	"os"

	"github.com/golang/snappy"
)

func OpenReader(ctx context.Context, name string, enableCompression bool) (io.ReadCloser, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return NewReader(ctx, f, enableCompression), nil
}

func NewReader(ctx context.Context, rc io.ReadCloser, enableCompression bool) io.ReadCloser {
	if enableCompression {
		return &bufReadCloser{ctx, snappy.NewReader(rc), rc}
	}
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
