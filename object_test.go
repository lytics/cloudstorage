package cloudstorage

import (
	"bufio"
	"io/ioutil"
	"os"
	"testing"

	"github.com/lytics/cloudstorage/logging"
	"github.com/lytics/cloudstorage/testutils"
)

//
// use os.Getenv("TESTINT") == "1" to run integration tests with Google CloudStore
//

var localconfig = &CloudStoreContext{
	LogggingContext: "unittest",
	TokenSource:     LocalFileSource,
	LocalFS:         "/tmp/mockcloud",
	TmpDir:          "/tmp/localcache",
}

var gcsIntconfig = &CloudStoreContext{
	LogggingContext: "integration-test",
	TokenSource:     GCEDefaultOAuthToken,
	Project:         "lyticsstaging",
	Bucket:          "cloudstore-tests",
	TmpDir:          "/tmp/localcache",
}

func createStore(t *testing.T) Store {

	LogConstructor = func(prefix string) logging.Logger {
		return logging.NewStdLogger(true, logging.DEBUG, prefix)
		//return testutils.NewStdLogger(t, prefix)
	}

	var config *CloudStoreContext
	if os.Getenv("TESTINT") == "" {
		//os.RemoveAll("/tmp/mockcloud")
		//os.RemoveAll("/tmp/localcache")
		config = localconfig
	} else {
		config = gcsIntconfig
	}
	store, err := NewStore(config)
	testutils.AssertEq(t, nil, err, "error.")

	return store
}

func clearstore(t *testing.T, store Store) {
	objs, err := store.List(Query{"", nil})
	testutils.AssertEq(t, nil, err, "error.")
	for _, o := range objs {
		t.Logf("clearstore(): deleting %v", o.Name())
		store.Delete(o.Name())
	}
}

func TestBasicRW(t *testing.T) {
	store := createStore(t)
	clearstore(t, store)
	//
	//Create a new object and write to it.
	//
	obj, err := store.NewObject("prefix/test.csv")
	testutils.AssertEq(t, nil, err, "error.")

	f, err := obj.Open(ReadWrite)
	testutils.AssertEq(t, nil, err, "error.")
	testutils.AssertT(t, f != nil, "the file was nil")

	testcsv := "Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n"

	w := bufio.NewWriter(f)
	n, err := w.WriteString(testcsv)
	testutils.AssertEq(t, nil, err, "error. %d", n)
	w.Flush()

	err = obj.Close()
	testutils.AssertEq(t, nil, err, "error.")

	//
	//Read the object back out of the cloud storage.
	//
	obj2, err := store.Get("prefix/test.csv")
	testutils.AssertEq(t, nil, err, "error.")

	f2, err := obj2.Open(ReadOnly)
	testutils.AssertEq(t, nil, err, "error.")

	bytes, err := ioutil.ReadAll(f2)
	testutils.AssertEq(t, nil, err, "error.")

	testutils.AssertEq(t, testcsv, string(bytes))
}

func TestAppend(t *testing.T) {
	store := createStore(t)
	clearstore(t, store)
	//
	//Create a new object and write to it.
	//
	obj, err := store.NewObject("test.csv")
	testutils.AssertEq(t, nil, err, "error.")

	f1, err := obj.Open(ReadWrite)
	testutils.AssertEq(t, nil, err, "error.")
	testutils.AssertT(t, f1 != nil, "the file was nil")

	testcsv := "Year,Make,Model\n2003,VW,EuroVan\n2001,Ford,Ranger\n"

	w1 := bufio.NewWriter(f1)
	n1, err := w1.WriteString(testcsv)
	testutils.AssertEq(t, nil, err, "error. %d", n1)
	w1.Flush()

	err = obj.Close()
	testutils.AssertEq(t, nil, err, "error.")

	//
	//get the object and append to it...
	//
	morerows := "2013,VW,Jetta\n2011,Dodge,Caravan\n"
	obj2, err := store.Get("test.csv")
	testutils.AssertEq(t, nil, err, "error.")

	f2, err := obj2.Open(ReadWrite)
	testutils.AssertEq(t, nil, err, "error.")
	testutils.AssertT(t, f2 != nil, "the file was nil")

	w2 := bufio.NewWriter(f2)
	n2, err := w2.WriteString(morerows)
	testutils.AssertEq(t, nil, err, "error. %d", n2)
	w2.Flush()

	err = obj2.Close()
	testutils.AssertEq(t, nil, err, "error.")

	//
	//Read the object back out of the cloud storage.
	//
	obj3, err := store.Get("test.csv")
	testutils.AssertEq(t, nil, err, "error.")

	f3, err := obj3.Open(ReadOnly)
	testutils.AssertEq(t, nil, err, "error.")

	bytes, err := ioutil.ReadAll(f3)
	testutils.AssertEq(t, nil, err, "error.")

	testutils.AssertEq(t, testcsv+morerows, string(bytes), "not the rows we expected.")
}

func TestTruncate(t *testing.T) {
	store := createStore(t)
	clearstore(t, store)
	//
	//Create a new object and write to it.
	//
	obj, err := store.NewObject("test.csv")
	testutils.AssertEq(t, nil, err, "error.")

	f1, err := obj.Open(ReadWrite)
	testutils.AssertEq(t, nil, err, "error.")
	testutils.AssertT(t, f1 != nil, "the file was nil")

	testcsv := "Year,Make,Model\n2003,VW,EuroVan\n2001,Ford,Ranger\n"

	w1 := bufio.NewWriter(f1)
	n1, err := w1.WriteString(testcsv)
	testutils.AssertEq(t, nil, err, "error. %d", n1)
	w1.Flush()

	err = obj.Close()
	testutils.AssertEq(t, nil, err, "error.")

	//
	//get the object and replace it...
	//
	newtestcsv := "Year,Make,Model\n2013,VW,Jetta\n"
	obj2, err := store.Get("test.csv")
	testutils.AssertEq(t, nil, err, "error.")

	f2, err := obj2.Open(ReadWrite)
	testutils.AssertEq(t, nil, err, "error.")
	testutils.AssertT(t, f2 != nil, "the file was nil")

	// Truncating the file will zero out the file
	f2.Truncate(0)
	// We also want to start writing from the beginning of the file
	f2.Seek(0, 0)

	w2 := bufio.NewWriter(f2)
	n2, err := w2.WriteString(newtestcsv)
	testutils.AssertEq(t, nil, err, "error. %d", n2)
	w2.Flush()

	err = obj2.Close()
	testutils.AssertEq(t, nil, err, "error.")

	//
	//Read the object back out of the cloud storage.
	//
	obj3, err := store.Get("test.csv")
	testutils.AssertEq(t, nil, err, "error.")

	f3, err := obj3.Open(ReadOnly)
	testutils.AssertEq(t, nil, err, "error.")

	bytes, err := ioutil.ReadAll(f3)
	testutils.AssertEq(t, nil, err, "error.")

	testutils.AssertEq(t, newtestcsv, string(bytes), "not the rows we expected.")
}

func TestNewObjectWithExisting(t *testing.T) {
	store := createStore(t)
	clearstore(t, store)
	//
	//Create a new object and write to it.
	//
	obj, err := store.NewObject("test.csv")
	testutils.AssertEq(t, nil, err, "error.")

	f, err := obj.Open(ReadWrite)
	testutils.AssertEq(t, nil, err, "error.")
	testutils.AssertT(t, f != nil, "the file was nil")

	testcsv := "Year,Make,Model\n2003,VW,EuroVan\n2001,Ford,Ranger\n"

	w := bufio.NewWriter(f)
	n, err := w.WriteString(testcsv)
	testutils.AssertEq(t, nil, err, "error. %d", n)
	w.Flush()

	err = obj.Close()
	testutils.AssertEq(t, nil, err, "error.")

	//
	// Ensure calling NewObject on an existing object returns an error,
	// because the object exits.
	//
	obj2, err := store.NewObject("test.csv")
	testutils.AssertEq(t, ObjectExists, err, "error.")
	testutils.AssertEq(t, nil, obj2, "object shoudl be nil.")

	//
	//Read the object back out of the cloud storage.
	//
	obj3, err := store.Get("test.csv")
	testutils.AssertEq(t, nil, err, "error.")

	f3, err := obj3.Open(ReadOnly)
	testutils.AssertEq(t, nil, err, "error.")

	bytes, err := ioutil.ReadAll(f3)
	testutils.AssertEq(t, nil, err, "error.")

	testutils.AssertEq(t, testcsv, string(bytes))
}
