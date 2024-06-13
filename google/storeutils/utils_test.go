package storeutils

import (
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"context"
	"google.golang.org/api/option"

	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/google"
)

var testBucket = os.Getenv("TESTBUCKET")
var testProject = os.Getenv("TESTPROJECT")
var testGetFile = os.Getenv("TESTFILE")

func Setup(t *testing.T) *storage.Client {
	if testProject == "" || testBucket == "" {
		t.Skip("TESTPROJECT, and TESTBUCKET EnvVars must be set to perform integration test")
	}

	conf := &cloudstorage.Config{
		Type:       google.StoreType,
		AuthMethod: google.AuthGCEDefaultOAuthToken,
		Project:    testProject,
		Bucket:     testBucket,
	}

	// Create http client with Google context auth
	googleClient, err := google.NewGoogleClient(conf)
	if err != nil {
		t.Errorf("Failed to create Google Client: %v\n", err)
	}

	gsc, err := storage.NewClient(context.Background(), option.WithHTTPClient(googleClient.Client()))
	if err != nil {
		t.Errorf("Error creating Google cloud storage client. project:%s gs://%s/ err:%v\n",
			conf.Project, conf.Bucket, err)

	}
	if gsc == nil {
		t.Errorf("storage Client returned is nil!")
	}
	return gsc
}
