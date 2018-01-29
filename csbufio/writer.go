package csbufio

import (
	"bufio"
	"io"
	"os"
)

var (
// Ensure we implement io.ReadWriteCloser
//_ io.ReadWriteCloser = (*pipeWriter)(nil)
)

type (
	bufWriteCloser struct {
		*bufio.Writer
		c io.Closer
	}
	pipeWriter struct {
		pw     *io.PipeWriter
		pr     *io.PipeReader
		opened bool
		donec  chan struct{} // closed after err and obj are set.
		err    error
	}
)

func OpenWriter(name string) (io.WriteCloser, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0665)
	if err != nil {
		return nil, err
	}
	return NewWriter(f), nil
}

// NewWriter is a io.WriteCloser.
func NewWriter(rc io.WriteCloser) io.WriteCloser {
	return bufWriteCloser{bufio.NewWriter(rc), rc}
}

func (bc bufWriteCloser) Close() error {
	if err := bc.Flush(); err != nil {
		return err
	}
	return bc.c.Close()
}

// NewReadWriter creates a writeable pipe io.ReadWriteCloser suitable for
func NewReadWriter() io.ReadWriteCloser {
	return &pipeWriter{}
}

func (w *pipeWriter) open() {

	w.pr, w.pw = io.Pipe()
	w.opened = true
}

// Read readers from read pipe.
func (w *pipeWriter) Read(b []byte) (n int, err error) {
	if w.err != nil {
		return 0, w.err
	}
	if !w.opened {
		w.open()
	}
	return w.pr.Read(b)
}

// Write appends to w. It implements the io.Writer interface.
func (w *pipeWriter) Write(p []byte) (n int, err error) {
	if w.err != nil {
		return 0, w.err
	}
	if !w.opened {
		w.open()
	}
	return w.pw.Write(p)
}

// Close completes the write operation and flushes any buffered data.
// If Close doesn't return an error, metadata about the written object
// can be retrieved by calling Object.
func (w *pipeWriter) Close() error {
	if !w.opened {
		w.open()
	}
	if err := w.pw.Close(); err != nil {
		return err
	}
	<-w.donec
	return w.err
}

// CloseWithError aborts the write operation with the provided error.
// CloseWithError always returns nil.
func (w *pipeWriter) CloseWithError(err error) error {
	if !w.opened {
		return nil
	}
	return w.pw.CloseWithError(err)
}
