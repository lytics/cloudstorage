package cloudstorage

import (
	"os"
	"testing"

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

	gcsctx := &CloudStoreContext{
		LogggingContext: "testing-config",
		TokenSource:     GCEDefaultOAuthToken,
		Project:         testProject,
		Bucket:          testBucket,
	}

	// Create http client with Google context auth
	googleClient, err := NewGoogleClient(gcsctx)
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

func TestListing(t *testing.T) {
	gsc := Setup(t)

	bh := gsc.Bucket(testBucket)

	// Create Query
	q := storage.Query{MaxResults: 100}

	// Get list of objects
	ol, err := List(bh, q)
	if err != nil {
		t.Fatalf("Error getting list of objects: %v", err)
	}

	if len(ol.Results) == 0 {
		t.Logf("No results returned")
		return
	}
	for i, o := range ol.Results {
		t.Logf("%d %s", i, o.Name)
	}
}

func TestOpenObject(t *testing.T) {
	gsc := Setup(t)
	bh := gsc.Bucket(testBucket)
	if testGetFile == "" {
		t.Skip("TESTFILE EnvVar must be set to run test")
	}

	// Create Query
	q := storage.Query{Prefix: testGetFile, MaxResults: 100}

	//list objects
	ol, err := List(bh, q)
	if err != nil {
		t.Errorf("Error getting ObjectList: %v", err)
	}

	buff, errs := OpenObject(ol, bh)
	if len(errs) > 0 {
		t.Errorf("OpenObject Errors %#v", errs)
	}
	t.Logf("%s", buff.String())
}

func TestGetObject(t *testing.T) {
	if testGetFile == "" {
		t.Skip("TESTFILE EnvVar must be set to run test")
	}
	gsc := Setup(t)

	buff, err := GetObject(gsc, testBucket, testGetFile)
	if err != nil {
		t.Errorf("Error reading file %s: %v", testGetFile, err)
	}
	str := buff.String()
	if len(str) == 0 {
		t.Errorf("No bytes read from GCS")
	}
	t.Logf("%s", str)
}
