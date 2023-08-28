package csbufio

import (
	"bufio"
	"context"
	"io"
	"os"

	"github.com/golang/snappy"
)

type flusher interface {
	Flush() error
}

type bufWriteFlusherCloser struct {
	ctx context.Context
	w   io.Writer
	f   flusher
	c   io.Closer
}

func OpenWriter(ctx context.Context, name string, enableCompression bool) (io.WriteCloser, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0665)
	if err != nil {
		return nil, err
	}
	return NewWriter(ctx, f, enableCompression), nil
}

// NewWriter is a io.WriteCloser.
func NewWriter(ctx context.Context, rc io.WriteCloser, enableCompression bool) io.WriteCloser {
	if enableCompression {
		sw := snappy.NewBufferedWriter(rc)
		return &bufWriteFlusherCloser{ctx, sw, sw, sw}
	}
	bw := bufio.NewWriter(rc)
	return &bufWriteFlusherCloser{ctx, bw, bw, rc}
}

func (b *bufWriteFlusherCloser) Write(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	return b.w.Write(p)
}

func (b *bufWriteFlusherCloser) Close() error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if err := b.f.Flush(); err != nil {
		return err
	}
	return b.c.Close()
}
