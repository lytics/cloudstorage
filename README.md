# Cloudstorage Introduction:
Is an abstraction layer for distributed filesystems like Google's Cloud Storage or Amazon's S3.  In addition it also supports mocking remote storage with local files.  Lytics is currently using this framework in production for abstracting access to Google Cloud Storage. 

Note: S3 isn't implemented yet, but is on it's way.  

#Example usage:
Note: For these examples Im ignoring all errors and using the `_` for them.

##### Creating a Store object:
```go
//This is an example of a local storage object:  See(https://github.com/lytics/cloudstorage/blob/master/testutils/testutils.go#L30) for a GCS example:
var config = &cloudstorage.CloudStoreContext{
	LogggingContext: "unittest",
	TokenSource:     cloudstorage.LocalFileSource,
	LocalFS:         "/tmp/mockcloud",
	TmpDir:          "/tmp/localcache",
}
store, _ := cloudstorage.NewStore(config)
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
// Calling Get on an existing object will return a cloudstorage object or the cloudstorage.ObjectNotFound error.
obj2, _ := store.Get("prefix/test.csv")
f2, _ := obj2.Open(cloudstorage.ReadOnly)
bytes, _ := ioutil.ReadAll(f2)
fmt.Println(string(bytes)) // should print the CSV file from the block above...
```

##### Transferring an existing object:
```go
var config = &storeutils.TransferConfig{
	ProjectID:             "my-project",
	DestBucket:            "my-destination-bucket",
	Src:                   storeutils.NewGcsSource("my-source-bucket"),
	IncludePrefxies:       []string{"these", "prefixes"},
}
 
transferer, _ := storeutils.NewTransferer(client)
resp, _ := transferer.NewTransfer(config)

// use S3 source instead
config.Src = storeutils.NewAwsSource("bucket", "key", "secret") 
resp, _ = transferer.NewTransfer(config)
```

See [object_test.go](https://github.com/lytics/cloudstorage/blob/master/object_test.go) for more examples

## Testing

Due to the way integration tests act against a GS bucket and objects; run tests without parallelization. 

```
cd $GOPATH/src/github.com/lytics/cloudstorage
go test -p 1 ./...
```

