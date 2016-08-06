package storeutils

import (
	"fmt"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
)

// Iterates through pages of results from a Google Storage Bucket and
// lists objects which match the specified storage.Query
func List(gcsb *storage.BucketHandle, query storage.Query) (*storage.ObjectList, error) {

	gobjects, err := ListBucketObjectsReq(gcsb, &query, GCSRetries)
	if err != nil {
		fmt.Errorf("couldn't list objects. prefix=%s err=%v", query.Prefix, err)
		return nil, err
	}

	if gobjects == nil {
		return nil, nil
	}

	if gobjects.Next != nil {
		q := gobjects.Next
		for q != nil {
			gobjectsB, err := ListBucketObjectsReq(gcsb, q, GCSRetries)
			if err != nil {
				fmt.Errorf("couldn't list the remaining pages of objects. prefix=%s err=%v", q.Prefix, err)
				return nil, err
			}

			concatGCSObjects(gobjects, gobjectsB)

			if gobjectsB != nil {
				q = gobjectsB.Next
			} else {
				q = nil
			}
		}
	}

	return gobjects, nil
}

//ListObjects is a wrapper around storeage.ListObjects, that retries on a GCS error.  GCS isn't a prefect system :p, and returns an error
//  about once every 2 weeks.
func ListBucketObjectsReq(gcsb *storage.BucketHandle, q *storage.Query, retries int) (*storage.ObjectList, error) {
	var lasterr error = nil
	//GCS sometimes returns a 500 error, so we'll just retry...
	for i := 0; i < retries; i++ {
		objects, err := gcsb.List(context.Background(), q)
		if err != nil {
			fmt.Errorf("error listing objects for the bucket. try:%d q.prefix:%v err:%v", i, q.Prefix, err)
			lasterr = err
			backoff(i)
			continue
		}
		return objects, nil
	}
	return nil, lasterr
}
