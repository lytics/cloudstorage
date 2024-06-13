package storeutils

import (
	"bytes"
	"io"

	"cloud.google.com/go/storage"
	"context"

	"github.com/lytics/cloudstorage"
)

// GetObject Gets a single object's bytes based on bucket and name parameters
func GetObject(gc *storage.Client, bucket, name string) (*bytes.Buffer, error) {
	return GetObjectWithContext(context.Background(), gc, bucket, name)
}

// GetObject Gets a single object's bytes based on bucket and name parameters
func GetObjectWithContext(ctx context.Context, gc *storage.Client, bucket, name string) (*bytes.Buffer, error) {
	rc, err := gc.Bucket(bucket).Object(name).NewReader(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, cloudstorage.ErrObjectNotFound
		}
		return nil, err
	}
	by, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return bytes.NewBuffer(by), nil
}
