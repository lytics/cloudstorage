package cloudstorage

import (
	"os"
	"time"
)

type Store interface {
	//NewObject creates a new empty object backed by the cloud store
	//  This new object isn't' synced/created in the backing store
	//  until the object is Closed/Sync'ed.
	NewObject(o string) (Object, error)

	//Get returns the object from the cloud store.   The object
	//  isn't opened already, see Object.Open()
	Get(o string) (Object, error)
	//List takes a prefix query and returns an array of unopened objects
	// that have the given prefix.
	List(query Query) (Objects, error)
	//Delete removes the object from the cloud store.   Any Objects which have
	// had Open() called should work as normal.
	Delete(o string) error

	String() string
}

//Object is a handle to a cloud stored file/object.  Calling Open will pull the remote file onto
// your local filesystem for reading/writing.  Calling Sync/Close will push the local copy
// backup to the cloud store.
type Object interface {
	Name() string
	String() string

	Updated() time.Time
	MetaData() map[string]string
	SetMetaData(meta map[string]string)

	StorageSource() string
	//Open copies the remote file to a local cache and opens the cached version
	// for read/writing.  Calling Close/Sync will push the copy back to the
	// backing store.
	Open(readonly AccessLevel) (*os.File, error)
	//Release will remove the locally cached copy of the file.  You most call Close
	// before releasing.  Release will call os.Remove(local_copy_file) so opened
	//filehandles need to be closed.
	Release() error
	//Implement io.ReadWriteCloser Open most be called before using these
	// functions.
	Read(p []byte) (n int, err error)
	Write(p []byte) (n int, err error)
	Sync() error
	Close() error
}
