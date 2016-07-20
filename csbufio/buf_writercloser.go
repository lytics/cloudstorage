package csbufio

import (
	"bufio"
	"io"
	"os"
)

func OpenWriter(name string) (io.WriteCloser, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0665)
	if err != nil {
		return nil, err
	}
	return NewWriter(f), nil
}

func NewWriter(rc io.WriteCloser) io.WriteCloser {
	return bufWriteCloser{bufio.NewWriter(rc), rc}
}

type bufWriteCloser struct {
	*bufio.Writer
	c io.Closer
}

func (bc bufWriteCloser) Close() error {
	if err := bc.Flush(); err != nil {
		return err
	}
	return bc.c.Close()
}
