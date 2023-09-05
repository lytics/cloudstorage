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
	return NewReader(ctx, f), nil
}

var snappyHeader = []byte{0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59}

func NewReader(ctx context.Context, rc io.ReadCloser) io.ReadCloser {
	br := bufio.NewReader(rc)
	header, _ := br.Peek(10) // errors are handled by treating it not as snappy
	if len(header) == 10 {
		for i := range header {
			if header[i] != snappyHeader[i] {
				break
			}
			if i == 9 {
				return &bufReadCloser{ctx, bufio.NewReader(snappy.NewReader(br)), rc}
			}
		}
	}
	return &bufReadCloser{ctx, br, rc}
}

type bufReadCloser struct {
	ctx context.Context
	r   *bufio.Reader
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
