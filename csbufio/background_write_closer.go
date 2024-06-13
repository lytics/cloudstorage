package csbufio

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/araddon/gou"
	"golang.org/x/sync/errgroup"
)

var _ io.WriteCloser = (*backgroundWriteCloser)(nil)

// backgroundWriteCloser - manages data and go routines used to pipe data to the cloud, calling Close
// will flush data to the cloud and block until all inflight data has been written or
// we get an error.
type backgroundWriteCloser struct {
	pipeWriter    io.Closer
	buffioWriter  *bufio.Writer
	backgroundJob *errgroup.Group
	done          chan struct{}
}

// NewBackgroundWriteCloser - returns an io.WriteCloser that manages the cloud connection pipe and when Close is called
// it blocks until all data is flushed to the cloud via a background go routine call to uploadMultiPart.
func NewBackgroundWriteCloser(ctx context.Context, job func(context.Context, io.ReadCloser) error) io.WriteCloser {
	pipeReader, pipeWriter := io.Pipe()
	buffioWriter := bufio.NewWriter(pipeWriter)

	var backgroundJob errgroup.Group

	backgroundJob.Go(func() error {
		err := job(ctx, pipeReader)
		if err != nil {
			gou.Warnf("could not upload %v", err)

			return err
		}

		return nil
	})

	done := make(chan struct{})

	return &backgroundWriteCloser{
		pipeWriter, buffioWriter, &backgroundJob, done,
	}
}

var errAlreadyClosed = errors.New("writer already closed")

// Write writes data to our write buffer, which writes to the backing io pipe.
// If an error is encountered while writting we may not see it here, my guess is
// we wouldn't see it until someone calls close and the error is returned from the
// error group.
func (bc *backgroundWriteCloser) Write(p []byte) (nn int, err error) {
	select {
	case <-bc.done:
		return 0, errAlreadyClosed
	default:
	}

	return bc.buffioWriter.Write(p)
}

// Close and block until we flush inflight data to the cloud
func (bc *backgroundWriteCloser) Close() error {
	select {
	case <-bc.done:
		return nil
	default:
		close(bc.done)
	}

	var errs []error

	//Flush buffered data to the backing pipe writer.
	if err := bc.buffioWriter.Flush(); err != nil {
		errs = append(errs, fmt.Errorf("unable to flush buffer: %w", err))
	}

	//Close the pipe writer so that the pipe reader will return EOF,
	// doing so will cause uploadMultiPart to complete and return.
	if err := bc.pipeWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("unable to close pipe: %w", err))
	}
	//Use the error group's Wait method to block until upload has completed
	if err := bc.backgroundJob.Wait(); err != nil {
		errs = append(errs, fmt.Errorf("error from background job: %w", err))
	}

	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		return errors.Join(errs...)
	}
}
