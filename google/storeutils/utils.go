package storeutils

import (
	"context"
	"github.com/lytics/cloudstorage"
)

// GetAndOpen is a convenience method that combines Store.Get() and Object.Open() into
// a single call.
func GetAndOpen(s cloudstorage.Store, o string, level cloudstorage.AccessLevel) (cloudstorage.Object, error) {
	obj, err := s.Get(context.Background(), o)
	if err != nil {
		return nil, err
	}

	_, err = obj.Open(level)
	if err != nil {
		return nil, err
	}
	return obj, nil
}
