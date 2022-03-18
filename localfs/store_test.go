package localfs_test

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/localfs"
	"github.com/lytics/cloudstorage/testutils"
)

func TestAll(t *testing.T) {
	t.Parallel()

	tmpDir, err := ioutil.TempDir("/tmp", "all")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	localFsConf := &cloudstorage.Config{
		Type:       localfs.StoreType,
		AuthMethod: localfs.AuthFileSystem,
		LocalFS:    filepath.Join(tmpDir, "mockcloud"),
		TmpDir:     filepath.Join(tmpDir, "localcache"),
		Bucket:     "all",
	}

	store, err := cloudstorage.NewStore(localFsConf)
	if err != nil {
		t.Fatalf("Could not create store: config=%+v  err=%v", localFsConf, err)
		return
	}
	testutils.RunTests(t, store, localFsConf)
}

func TestBrusted(t *testing.T) {
	t.Parallel()

	// invalid config:  empty/missing LocalFS
	localFsConf := &cloudstorage.Config{
		Type:       localfs.StoreType,
		AuthMethod: localfs.AuthFileSystem,
		LocalFS:    "",
	}
	store, err := cloudstorage.NewStore(localFsConf)
	require.Error(t, err)
	require.Equal(t, nil, store)

	// invalid config:  LocalFS = TempDir
	localFsConf = &cloudstorage.Config{
		Type:       localfs.StoreType,
		AuthMethod: localfs.AuthFileSystem,
		LocalFS:    "/tmp/invalid",
		TmpDir:     "/tmp/invalid",
	}
	store, err = cloudstorage.NewStore(localFsConf)
	require.Error(t, err)
	require.Equal(t, nil, store)
}

func TestNewReaderDir(t *testing.T) {
	t.Parallel()

	tmpDir, err := ioutil.TempDir("/tmp", "newreaderdir")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// When a dir is requested, serve the index.html file instead
	localFsConf := &cloudstorage.Config{
		Type:       localfs.StoreType,
		AuthMethod: localfs.AuthFileSystem,
		LocalFS:    filepath.Join(tmpDir, "mockcloud"),
		TmpDir:     filepath.Join(tmpDir, "localcache"),
		Bucket:     "newreaderdir",
	}
	store, err := cloudstorage.NewStore(localFsConf)
	testutils.MockFile(store, "test/index.html", "test")
	require.NoError(t, err)
	require.Equal(t, nil, err)
	_, err = store.NewReader("test")
	require.Equal(t, err, cloudstorage.ErrObjectNotFound)
	err = store.Delete(context.Background(), "test/index.html")
	require.NoError(t, err)
}

func TestGetDir(t *testing.T) {
	t.Parallel()

	tmpDir, err := ioutil.TempDir("/tmp", "getdir")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// When a dir is requested, serve the index.html file instead
	localFsConf := &cloudstorage.Config{
		Type:       localfs.StoreType,
		AuthMethod: localfs.AuthFileSystem,
		LocalFS:    filepath.Join(tmpDir, "mockcloud"),
		TmpDir:     filepath.Join(tmpDir, "localcache"),
		Bucket:     "getdir",
	}
	store, err := cloudstorage.NewStore(localFsConf)
	require.NoError(t, err)
	err = testutils.MockFile(store, "test/index.html", "test")
	require.NoError(t, err)
	_, err = store.Get(context.Background(), "test")
	require.Equal(t, err, cloudstorage.ErrObjectNotFound)
	err = store.Delete(context.Background(), "test/index.html")
	require.NoError(t, err)
}
