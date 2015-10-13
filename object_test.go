package cloudstorage

import (
	"bufio"
	"io/ioutil"
	"os"
	"testing"

	"github.com/lytics/cloudstorage/testutils"
)

var localconfig = &CloudStoreContext{
	LogggingContext: "unittest:1",
	TokenSource:     LocalFileSource,
	LocalFS:         "/tmp/mockcloud",
	TmpDir:          "/tmp/localcache",
}

func createStore(t *testing.T) Store {
	os.RemoveAll("/tmp/mockcloud")
	os.RemoveAll("/tmp/localcache")

	store, err := NewStore(localconfig)
	testutils.AssertEq(t, nil, err, "error.")

	return store
}

//TODO add GCS testscases if os.Getenv("TESTINT") == "true"
func TestBasicRW(t *testing.T) {
	store := createStore(t)
	//
	//Create a new object and write to it.
	//
	obj, err := store.NewObject("test.csv")
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
	obj2, err := store.Get("test.csv")
	testutils.AssertEq(t, nil, err, "error.")

	f2, err := obj2.Open(ReadOnly)
	testutils.AssertEq(t, nil, err, "error.")

	bytes, err := ioutil.ReadAll(f2)
	testutils.AssertEq(t, nil, err, "error.")

	testutils.AssertEq(t, testcsv, string(bytes))
}

func TestAppend(t *testing.T) {
	store := createStore(t)
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
