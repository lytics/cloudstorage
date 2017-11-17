package testutils

import (
	"os"
	"time"

	"github.com/lytics/cloudstorage"
)

var localconfig = &cloudstorage.Config{
	TokenSource: cloudstorage.LocalFileSource,
	LocalFS:     "/tmp/mockcloud",
	TmpDir:      "/tmp/localcache",
}

var gcsIntconfig = &cloudstorage.Config{
	TokenSource: cloudstorage.GCEDefaultOAuthToken,
	Project:     "lyticsstaging",
	Bucket:      "cloudstore-tests",
	TmpDir:      "/tmp/localcache",
}

type TestingTB interface {
	Logf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

func CreateStore(t TestingTB) cloudstorage.Store {

	var config *cloudstorage.Config
	if os.Getenv("TESTINT") == "" {
		config = localconfig
	} else {
		config = gcsIntconfig
	}
	store, err := cloudstorage.NewStore(config)
	if err != nil {
		t.Fatalf("Could not create store: config=%+v  err=%v", config, err)
	}
	return store
}

func Clearstore(t TestingTB, store cloudstorage.Store) {
	t.Logf("----------------Clearstore-----------------\n")
	q := cloudstorage.Query{"", "", nil}
	q.Sorted()
	objs, err := store.List(q)
	if err != nil {
		t.Fatalf("Could not list store %v", err)
	}
	for _, o := range objs {
		t.Logf("clearstore(): deleting %v", o.Name())
		store.Delete(o.Name())
	}

	if os.Getenv("TESTINT") != "" {
		//GCS is lazy about deletes...
		time.Sleep(15 * time.Second)
	}
}
