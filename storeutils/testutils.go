package storeutils

import (
	"os"
	"testing"

	"github.com/lytics/cloudstorage"

	"golang.org/x/net/context"
	"google.golang.org/cloud"
	"google.golang.org/cloud/storage"
)

var testBucket = os.Getenv("TESTBUCKET")
var testProject = os.Getenv("TESTPROJECT")
var testGetFile = os.Getenv("TESTFILE")

func Setup(t *testing.T) *storage.Client {
	if testProject == "" || testBucket == "" {
		t.Skip("TESTPROJECT, and TESTBUCKET EnvVars must be set to perform integration test")
	}

	gcsctx := &cloudstorage.CloudStoreContext{
		LogggingContext: "testing-config",
		TokenSource:     cloudstorage.GCEDefaultOAuthToken,
		Project:         testProject,
		Bucket:          testBucket,
	}

	// Create http client with Google context auth
	googleClient, err := cloudstorage.NewGoogleClient(gcsctx)
	if err != nil {
		t.Errorf("Failed to create Google Client: %v\n", err)
	}

	gsc, err := storage.NewClient(context.Background(), cloud.WithBaseHTTP(googleClient.Client()))
	if err != nil {
		t.Errorf("Error creating Google cloud storage client. project:%s gs://%s/ err:%v\n",
			gcsctx.Project, gcsctx.Bucket, err)

	}
	if gsc == nil {
		t.Errorf("storage Client returned is nil!")
	}
	return gsc
}
