package localfs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dvriesman/cloudstorage"
	"github.com/dvriesman/cloudstorage/localfs"
	"github.com/dvriesman/cloudstorage/testutils"
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

	// invalid config:  empty/missing LocalFS
	localFsConf = &cloudstorage.Config{
		Type:       localfs.StoreType,
		AuthMethod: localfs.AuthFileSystem,
		LocalFS:    "",
	}
	store, err = cloudstorage.NewStore(localFsConf)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, nil, store)

	// invalid config:  LocalFS = TempDir
	localFsConf = &cloudstorage.Config{
		Type:       localfs.StoreType,
		AuthMethod: localfs.AuthFileSystem,
		LocalFS:    "/tmp/invalid",
		TmpDir:     "/tmp/invalid",
	}
	store, err = cloudstorage.NewStore(localFsConf)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, nil, store)
}
