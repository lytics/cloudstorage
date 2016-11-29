package storeutils

import (
	"bytes"
	"io/ioutil"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"

	"github.com/lytics/cloudstorage"
)

// Gets a single object's bytes based on bucket and name parameters
func GetObject(gc *storage.Client, bucket, name string) (*bytes.Buffer, error) {

	rc, err := gc.Bucket(bucket).Object(name).NewReader(context.Background())
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, cloudstorage.ObjectNotFound
		}
		return nil, err
	}
	by, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return bytes.NewBuffer(by), nil
}
