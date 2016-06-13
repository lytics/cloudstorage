package storeutils

import (
	"testing"

	"google.golang.org/cloud/storage"
)

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
