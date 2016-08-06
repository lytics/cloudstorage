package storeutils

import (
	"testing"

	"cloud.google.com/go/storage"
)

// This is a difficult test for specifics since contents of test buckets will differ
// Simply writing out the contents of a bucket and failing if an error occurs
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
