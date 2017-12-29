package google_test

import (
	"os"
	"testing"

	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/google"
	"github.com/lytics/cloudstorage/testutils"
)

/*

# to use Google Cloud Storage ensure your current env
# has access to a google cloud storage account and then
# export TESTGOOGLE

export TESTGOOGLE=1

*/

var config = &cloudstorage.Config{
	Type:       google.StoreType,
	AuthMethod: google.AuthGCEDefaultOAuthToken,
	Project:    "lyticsstaging",
	Bucket:     "cloudstore-tests",
	TmpDir:     "/tmp/localcache",
}

func TestAll(t *testing.T) {
	if os.Getenv("TESTGOOGLE") == "" {
		t.Skip("Not testing")
		return
	}
	store, err := cloudstorage.NewStore(config)
	if err != nil {
		t.Fatalf("Could not create store: config=%+v  err=%v", config, err)
	}
	testutils.RunTests(t, store)
}
