package testutils

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"

	"github.com/lytics/cloudstorage"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"
)

type TestingT interface {
	Logf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

func Clearstore(t TestingT, store cloudstorage.Store) {
	t.Logf("----------------Clearstore-----------------\n")
	q := cloudstorage.Query{"", "", nil}
	q.Sorted()
	objs, err := store.List(q)
	if err != nil {
		t.Fatalf("Could not list store %v", err)
	}
	for _, o := range objs {
		t.Logf("clearstore(): deleting %v", o.Name())
		store.Delete(o.Name())
	}

	if os.Getenv("TESTGOOGLE") != "" {
		// GCS is lazy about deletes...
		time.Sleep(15 * time.Second)
	}
}

func RunTests(t TestingT, s cloudstorage.Store) {
	t.Logf("running basic rw")
	BasicRW(t, s)
	t.Logf("running Append")
	Append(t, s)
	t.Logf("running ListObjsAndFolders")
	ListObjsAndFolders(t, s)
	t.Logf("running Truncate")
	Truncate(t, s)
	t.Logf("running NewObjectWithExisting")
	NewObjectWithExisting(t, s)
	t.Logf("running TestReadWriteCloser")
	TestReadWriteCloser(t, s)
}

func BasicRW(t TestingT, store cloudstorage.Store) {

	Clearstore(t, store)

	// Create a new object and write to it.
	obj, err := store.NewObject("prefix/test.csv")
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, obj)

	f, err := obj.Open(cloudstorage.ReadWrite)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, f)

	testcsv := "Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n"

	w := bufio.NewWriter(f)
	_, err = w.WriteString(testcsv)
	assert.Equal(t, nil, err)
	w.Flush()

	err = obj.Close()
	assert.Equal(t, nil, err)

	// Read the object back out of the cloud storage.
	obj2, err := store.Get("prefix/test.csv")
	assert.Equal(t, nil, err)

	f2, err := obj2.Open(cloudstorage.ReadOnly)
	assert.Equal(t, nil, err)

	bytes, err := ioutil.ReadAll(f2)
	assert.Equal(t, nil, err)

	assert.Equal(t, testcsv, string(bytes))
}

func Append(t TestingT, store cloudstorage.Store) {
	Clearstore(t, store)
	now := time.Now()
	time.Sleep(10 * time.Millisecond)

	// Create a new object and write to it.
	obj, err := store.NewObject("test.csv")
	assert.Equal(t, nil, err)

	f1, err := obj.Open(cloudstorage.ReadWrite)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, f1)

	testcsv := "Year,Make,Model\n2003,VW,EuroVan\n2001,Ford,Ranger\n"

	w1 := bufio.NewWriter(f1)
	_, err = w1.WriteString(testcsv)
	assert.Equal(t, nil, err)
	w1.Flush()

	err = obj.Close()
	assert.Equal(t, nil, err)

	// get the object and append to it...
	morerows := "2013,VW,Jetta\n2011,Dodge,Caravan\n"
	obj2, err := store.Get("test.csv")
	assert.Equal(t, nil, err)

	// get updated time
	updated := obj2.Updated()
	assert.True(t, updated.After(now), "updated time was not set")
	time.Sleep(10 * time.Millisecond)

	f2, err := obj2.Open(cloudstorage.ReadWrite)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, f2)

	w2 := bufio.NewWriter(f2)
	_, err = w2.WriteString(morerows)
	assert.Equal(t, nil, err)
	w2.Flush()

	err = obj2.Close()
	assert.Equal(t, nil, err)

	// Read the object back out of the cloud storage.
	obj3, err := store.Get("test.csv")
	assert.Equal(t, nil, err)
	updated3 := obj3.Updated()
	assert.True(t, updated3.After(updated), "updated time not updated")
	f3, err := obj3.Open(cloudstorage.ReadOnly)
	assert.Equal(t, nil, err)

	bytes, err := ioutil.ReadAll(f3)
	assert.Equal(t, nil, err)

	assert.Equal(t, testcsv+morerows, string(bytes), "not the rows we expected.")
}

func ListObjsAndFolders(t TestingT, store cloudstorage.Store) {
	Clearstore(t, store)

	createObjects := func(names []string) {
		for _, n := range names {
			obj, err := store.NewObject(n)
			assert.Equal(t, nil, err)

			f1, err := obj.Open(cloudstorage.ReadWrite)
			assert.Equal(t, nil, err)
			assert.NotEqual(t, nil, f1)

			testcsv := "12345\n"

			w1 := bufio.NewWriter(f1)
			_, err = w1.WriteString(testcsv)
			assert.Equal(t, nil, err)
			w1.Flush()

			err = obj.Close()
			assert.Equal(t, nil, err)
		}
	}

	// Create 5 objects in each of 3 folders
	// ie 15 objects
	folders := []string{"a", "b", "c"}
	names := []string{}
	for _, folder := range folders {
		for i := 0; i < 5; i++ {
			n := fmt.Sprintf("list-test/%s/test%d.csv", folder, i)
			names = append(names, n)
		}
	}

	sort.Strings(names)

	createObjects(names)

	q := cloudstorage.NewQuery("list-test/")
	q.Sorted()
	objs, err := store.List(q)
	assert.Equal(t, nil, err)
	assert.Equal(t, 15, len(objs), "incorrect list len. wanted 15 got %d", len(objs))

	q = cloudstorage.NewQuery("list-test/b")
	q.Sorted()
	objs, err = store.List(q)
	assert.Equal(t, nil, err)
	assert.Equal(t, 5, len(objs), "incorrect list len. wanted 5 got %d", len(objs))

	for i, o := range objs {
		t.Logf("%d found %v", i, o.Name())
		assert.Equal(t, names[i+5], o.Name(), "unexpected name.")
	}

	// Now with iterator
	iter := store.Objects(context.Background(), q)

	objs = make(cloudstorage.Objects, 0)
	i := 0
	for {
		o, err := iter.Next()
		if err == iterator.Done {
			break
		}
		objs = append(objs, o)
		t.Logf("%d found %v", i, o.Name())
		assert.Equal(t, names[i+5], o.Name(), "unexpected name.")
		i++
	}

	assert.Equal(t, 5, len(objs), "incorrect list len.")

	q = cloudstorage.NewQueryForFolders("list-test/")
	folders, err = store.Folders(context.Background(), q)
	t.Logf("folders %v", folders)
	assert.Equal(t, nil, err)
	assert.Equal(t, 3, len(folders), "incorrect list len. wanted 3 folders. %v", folders)

	folders = []string{"a/a2", "b/b1", "b/b2"}
	names = []string{}
	for _, folder := range folders {
		for i := 0; i < 2; i++ {
			n := fmt.Sprintf("list-test/%s/test%d.csv", folder, i)
			names = append(names, n)
		}
	}

	sort.Strings(names)

	createObjects(names)

	q = cloudstorage.NewQueryForFolders("list-test/")
	folders, err = store.Folders(context.Background(), q)
	t.Logf("folders %v", folders)
	assert.Equal(t, nil, err)
	assert.Equal(t, 3, len(folders), "incorrect list len. wanted 3 folders. %v", folders)

	q = cloudstorage.NewQueryForFolders("list-test/b/")
	folders, err = store.Folders(context.Background(), q)
	t.Logf("folders %v", folders)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(folders), "incorrect list len. wanted 2 folders. %v", folders)
}

func Truncate(t TestingT, store cloudstorage.Store) {

	Clearstore(t, store)

	// Create a new object and write to it.
	obj, err := store.NewObject("test.csv")
	assert.Equal(t, nil, err)

	f1, err := obj.Open(cloudstorage.ReadWrite)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, f1, "the file was nil")

	testcsv := "Year,Make,Model\n2003,VW,EuroVan\n2001,Ford,Ranger\n"

	w1 := bufio.NewWriter(f1)
	n1, err := w1.WriteString(testcsv)
	assert.Equal(t, nil, err, "error. %d", n1)
	w1.Flush()

	err = obj.Close()
	assert.Equal(t, nil, err)

	// get the object and replace it...
	newtestcsv := "Year,Make,Model\n2013,VW,Jetta\n"
	obj2, err := store.Get("test.csv")
	assert.Equal(t, nil, err)

	f2, err := obj2.Open(cloudstorage.ReadWrite)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, f2, "the file was nil")

	// Truncating the file will zero out the file
	f2.Truncate(0)
	// We also want to start writing from the beginning of the file
	f2.Seek(0, 0)

	w2 := bufio.NewWriter(f2)
	n2, err := w2.WriteString(newtestcsv)
	assert.Equal(t, nil, err, "error. %d", n2)
	w2.Flush()

	err = obj2.Close()
	assert.Equal(t, nil, err)

	// Read the object back out of the cloud storage.
	obj3, err := store.Get("test.csv")
	assert.Equal(t, nil, err)

	f3, err := obj3.Open(cloudstorage.ReadOnly)
	assert.Equal(t, nil, err)

	bytes, err := ioutil.ReadAll(f3)
	assert.Equal(t, nil, err)

	assert.Equal(t, newtestcsv, string(bytes), "not the rows we expected.")
}

func NewObjectWithExisting(t TestingT, store cloudstorage.Store) {

	Clearstore(t, store)

	// Create a new object and write to it.
	obj, err := store.NewObject("test.csv")
	assert.Equal(t, nil, err)

	f, err := obj.Open(cloudstorage.ReadWrite)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, f, "the file was nil")

	testcsv := "Year,Make,Model\n2003,VW,EuroVan\n2001,Ford,Ranger\n"

	w := bufio.NewWriter(f)
	n, err := w.WriteString(testcsv)
	assert.Equal(t, nil, err, "error. %d", n)
	w.Flush()

	err = obj.Close()
	assert.Equal(t, nil, err)

	// Ensure calling NewObject on an existing object returns an error,
	// because the object exits.
	obj2, err := store.NewObject("test.csv")
	assert.Equal(t, cloudstorage.ErrObjectExists, err, "error.")
	assert.Equal(t, nil, obj2, "object shoudl be nil.")

	// Read the object back out of the cloud storage.
	obj3, err := store.Get("test.csv")
	assert.Equal(t, nil, err)

	f3, err := obj3.Open(cloudstorage.ReadOnly)
	assert.Equal(t, nil, err)

	bytes, err := ioutil.ReadAll(f3)
	assert.Equal(t, nil, err)

	assert.Equal(t, testcsv, string(bytes))
}

func TestReadWriteCloser(t TestingT, store cloudstorage.Store) {

	Clearstore(t, store)

	object := "prefix/iorw.test"
	data := fmt.Sprintf("pid:%v:time:%v", os.Getpid(), time.Now().Nanosecond())

	wc, err := store.NewWriter(object, nil)
	assert.Equal(t, nil, err)
	buf1 := bytes.NewBufferString(data)
	_, err = buf1.WriteTo(wc)
	assert.Equal(t, nil, err)
	err = wc.Close()
	assert.Equal(t, nil, err)

	rc, err := store.NewReader(object)
	assert.Equal(t, nil, err)
	buf2 := bytes.Buffer{}
	_, err = buf2.ReadFrom(rc)
	assert.Equal(t, nil, err)
	assert.Equal(t, data, buf2.String(), "round trip data don't match")
}
