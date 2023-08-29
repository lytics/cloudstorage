package cloudstorage_test

import (
	"fmt"
	"testing"

	"github.com/lytics/cloudstorage"
	"github.com/stretchr/testify/require"
)

func TestRegistry(t *testing.T) {
	cloudstorage.Register("teststore", fakeProvider)
	paniced := didPanic(func() {
		cloudstorage.Register("teststore", fakeProvider)
	})
	require.True(t, paniced)
}
func didPanic(f func()) (dp bool) {
	defer func() {
		if r := recover(); r != nil {
			dp = true
		}
	}()
	f()
	return dp
}

func fakeProvider(conf *cloudstorage.Config) (cloudstorage.Store, error) {
	return nil, fmt.Errorf("Not Implemented")
}
