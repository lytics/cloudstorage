package testutils

import (
	"os"
	"time"

	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/logging"
)

const (
	NOLOGGING = -1
	FATAL     = 0
	ERROR     = 1
	WARN      = 2
	INFO      = 3
	DEBUG     = 4
)

var localconfig = &cloudstorage.CloudStoreContext{
	LogggingContext: "unittest",
	TokenSource:     cloudstorage.LocalFileSource,
	LocalFS:         "/tmp/mockcloud",
	TmpDir:          "/tmp/localcache",
}

var gcsIntconfig = &cloudstorage.CloudStoreContext{
	LogggingContext: "integration-test",
	TokenSource:     cloudstorage.GCEDefaultOAuthToken,
	Project:         "lyticsstaging",
	Bucket:          "cloudstore-tests",
	TmpDir:          "/tmp/localcache",
}

type TestingTB interface {
	Logf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

func CreateStore(t TestingTB) cloudstorage.Store {

	cloudstorage.LogConstructor = func(prefix string) logging.Logger {
		return logging.NewStdLogger(true, logging.DEBUG, prefix)
		//return testutils.NewStdLogger(t, prefix)
	}

	var config *cloudstorage.CloudStoreContext
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
