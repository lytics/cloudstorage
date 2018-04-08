package cloudstorage_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/localfs"
	"github.com/lytics/cloudstorage/testutils"
)

func TestAll(t *testing.T) {
	localFsConf := &cloudstorage.Config{
		Type:       localfs.StoreType,
		AuthMethod: localfs.AuthFileSystem,
		LocalFS:    "/tmp/mockcloud",
		TmpDir:     "/tmp/localcache",
	}

	store, err := cloudstorage.NewStore(localFsConf)
	if err != nil {
		t.Fatalf("Could not create store: config=%+v  err=%v", localFsConf, err)
		return
	}
	testutils.RunTests(t, store)
	// verify cleanup
	cloudstorage.CleanupCacheFiles(time.Minute*1, localFsConf.TmpDir)
}

func TestStore(t *testing.T) {
	invalidConf := &cloudstorage.Config{}

	store, err := cloudstorage.NewStore(invalidConf)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, nil, store)

	missingStoreConf := &cloudstorage.Config{
		Type: "non-existent-store",
	}

	store, err = cloudstorage.NewStore(missingStoreConf)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, nil, store)

	// test missing temp dir, assign local temp
	localFsConf := &cloudstorage.Config{
		Type:       localfs.StoreType,
		AuthMethod: localfs.AuthFileSystem,
		LocalFS:    "/tmp/mockcloud",
	}

	store, err = cloudstorage.NewStore(localFsConf)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, store)
}
