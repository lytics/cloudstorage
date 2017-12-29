# Introduction
Cloudstorage is an abstraction layer for Google's Cloud Storage and Local Files.
It provides a unified api for local files and Google Cloud files that aids testing.

[![Code Coverage](https://codecov.io/gh/lytics/cloudstorage/branch/master/graph/badge.svg)](https://codecov.io/gh/lytics/cloudstorage)
[![GoDoc](https://godoc.org/github.com/lytics/cloudstorage?status.svg)](http://godoc.org/github.com/lytics/cloudstorage)
[![Build Status](https://travis-ci.org/lytics/cloudstorage.svg?branch=master)](https://travis-ci.org/lytics/cloudstorage)
[![Go ReportCard](https://goreportcard.com/badge/lytics/cloudstorage)](https://goreportcard.com/report/lytics/cloudstorage)


### Similar/Related works
* https://github.com/graymeta/stow
* sync tool https://github.com/ncw/rclone


# Example usage:
Note: For these examples Im ignoring all errors and using the `_` for them.

##### Creating a Store object:
```go
// This is an example of a local storage object:  
// See(https://github.com/lytics/cloudstorage/blob/master/google/google_test.go) for a GCS example:
config := &cloudstorage.Config{
	Type: localfs.StoreType,
	TokenSource:     localfs.AuthFileSystem,
	LocalFS:         "/tmp/mockcloud",
	TmpDir:          "/tmp/localcache",
}
store, _ := cloudstorage.NewStore(config)
```

##### Listing Objects:

See go Iterator pattern doc for api-design:
https://github.com/GoogleCloudPlatform/google-cloud-go/wiki/Iterator-Guidelines
```go
// From a store that has been created

// Create a query
q := cloudstorage.NewQuery("list-test/")
// Create an Iterator
iter := store.Objects(context.Background(), q)

for {
	o, err := iter.Next()
	if err == iterator.Done {
		break
	}
	log.Println("found object %v", o.Name())
}
```

##### Writing an object :
```go
obj, _ := store.NewObject("prefix/test.csv")
// open for read and writing.  f is a filehandle to the local filesystem.
f, _ := obj.Open(cloudstorage.ReadWrite) 
w := bufio.NewWriter(f)
_, _ := w.WriteString("Year,Make,Model\n")
_, _ := w.WriteString("1997,Ford,E350\n")
w.Flush()

// Close sync's the local file to the remote store and removes the local tmp file.
obj.Close()
```


##### Reading an existing object:
```go
// Calling Get on an existing object will return a cloudstorage object or the cloudstorage.ErrObjectNotFound error.
obj2, _ := store.Get("prefix/test.csv")
f2, _ := obj2.Open(cloudstorage.ReadOnly)
bytes, _ := ioutil.ReadAll(f2)
fmt.Println(string(bytes)) // should print the CSV file from the block above...
```

##### Transferring an existing object:
```go
var config = &storeutils.TransferConfig{
	Type:                  google.StoreType,
	AuthMethod:            google.AuthGCEDefaultOAuthToken
	ProjectID:             "my-project",
	DestBucket:            "my-destination-bucket",
	Src:                   storeutils.NewGcsSource("my-source-bucket"),
	IncludePrefxies:       []string{"these", "prefixes"},
}

transferer, _ := storeutils.NewTransferer(client)
resp, _ := transferer.NewTransfer(config)

```

See [testsuite.go](https://github.com/lytics/cloudstorage/blob/master/testutils/testutils.go) for more examples

## Testing

Due to the way integration tests act against a GCS bucket and objects; run tests without parallelization. 

```
cd $GOPATH/src/github.com/lytics/cloudstorage
go test -p 1 ./...
```

