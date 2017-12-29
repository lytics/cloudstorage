// Package cloudstorage is an interface to make Local, Google, s3 file storage
// share a common interface to aid testing local as well as
// running in the cloud.
package cloudstorage

// Creating and iterating files from a cloudstorage.Store provider
//
// 		// This is an example of a local-storage (local filesystem) provider:
//		config := &cloudstorage.Config{
//			Type: localfs.StoreType,
//			TokenSource:     localfs.AuthFileSystem,
//			LocalFS:         "/tmp/mockcloud",
//			TmpDir:          "/tmp/localcache",
//		}
//		store, _ := cloudstorage.NewStore(config)
//
//		// Create a query to define the search path
//		q := cloudstorage.NewQuery("list-test/")
//
//		// Create an Iterator to list files
//		iter := store.Objects(context.Background(), q)
//		for {
//			o, err := iter.Next()
//			if err == iterator.Done {
//				break
//			}
//			log.Println("found object %v", o.Name())
//		}
