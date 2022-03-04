package localfs_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/localfs"
	"github.com/lytics/cloudstorage/testutils"
)

func TestAll(t *testing.T) {

	os.RemoveAll("/tmp/mockcloud")
	os.RemoveAll("/tmp/localcache")

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
	testutils.RunTests(t, store, localFsConf)

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

func TestNewReaderDir(t *testing.T) {
	// When a dir is requested, serve the index.html file instead
	localFsConf := &cloudstorage.Config{
		Type:       localfs.StoreType,
		AuthMethod: localfs.AuthFileSystem,
		LocalFS:    "/tmp/mockcloud",
		TmpDir:     "/tmp/localcache",
	}
	store, err := cloudstorage.NewStore(localFsConf)
	testutils.MockFile(store, "test/index.html", "test")
	assert.Equal(t, nil, err)
	assert.Equal(t, nil, err)
	_, err = store.NewReader("test")
	assert.Equal(t, err, cloudstorage.ErrObjectNotFound)
	err = store.Delete(context.Background(), "test/index.html")
	assert.Equal(t, nil, err)
}

func TestGetDir(t *testing.T) {
	// When a dir is requested, serve the index.html file instead
	localFsConf := &cloudstorage.Config{
		Type:       localfs.StoreType,
		AuthMethod: localfs.AuthFileSystem,
		LocalFS:    "/tmp/mockcloud",
		TmpDir:     "/tmp/localcache",
	}
	store, err := cloudstorage.NewStore(localFsConf)
	testutils.MockFile(store, "test/index.html", "test")
	assert.Equal(t, nil, err)
	assert.Equal(t, nil, err)
	_, err = store.Get(context.Background(), "test")
	assert.Equal(t, err, cloudstorage.ErrObjectNotFound)
	err = store.Delete(context.Background(), "test/index.html")
	assert.Equal(t, nil, err)
}
