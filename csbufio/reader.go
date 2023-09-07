package csbufio

import (
	"bufio"
	"compress/gzip"
	"context"
	"io"
	"os"
)

func OpenReader(ctx context.Context, name string, enableCompression bool) (io.ReadCloser, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return NewReader(ctx, f), nil
}

var compressionHeader = []byte{0x1f, 0x8b, 0x08}

func NewReader(ctx context.Context, rc io.ReadCloser) io.ReadCloser {
	br := bufio.NewReader(rc)
	header, _ := br.Peek(len(compressionHeader)) // errors are handled by treating it as uncompressed data
	if len(header) == len(compressionHeader) {
		for i := range header {
			if header[i] != compressionHeader[i] {
				break
			}
			if i == len(compressionHeader)-1 {
				cr, _ := gzip.NewReader(br) // TODO: handle error? Also this may be double-wrapping bufio.Readers but I'm not sure
				return &bufReadCloser{ctx, bufio.NewReader(cr), rc}
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
