package cloudstorage

import (
	"io"
	"os"
	"time"

	"golang.org/x/net/context"
)

// Store interface to define the Storage Interface abstracting
// the GCS, S3, LocalFile interfaces
type Store interface {
	// List takes a prefix query and returns an array of unopened objects
	// that have the given prefix.
	List(query Query) (Objects, error)
	// Iterator based api to get Objects
	Objects(ctx context.Context, q Query) ObjectIterator
	// Folders creates list of folders
	Folders(ctx context.Context, q Query) ([]string, error)

	// NewReader creates a new Reader to read the contents of the
	// object.
	// ObjectNotFound will be returned if the object is not found.
	NewReader(o string) (io.ReadCloser, error)
	NewReaderWithContext(ctx context.Context, o string) (io.ReadCloser, error)

	// NewWriter returns a io.Writer that writes to a Cloud object
	// associated with this backing Store object.
	//
	// A new object will be created if an object with this name already exists.
	// Otherwise any previous object with the same name will be replaced.
	// The object will not be available (and any previous object will remain)
	// until Close has been called
	NewWriter(o string, metadata map[string]string) (io.WriteCloser, error)
	NewWriterWithContext(ctx context.Context, o string, metadata map[string]string) (io.WriteCloser, error)

	// NewObject creates a new empty object backed by the cloud store
	// This new object isn't' synced/created in the backing store
	// until the object is Closed/Sync'ed.
	NewObject(o string) (Object, error)

	// Get returns the object from the cloud store.   The object
	// isn't opened already, see Object.Open()
	// ObjectNotFound will be returned if the object is not found.
	Get(o string) (Object, error)

	// Delete removes the object from the cloud store.
	Delete(o string) error

	String() string
}

// Object is a handle to a cloud stored file/object.  Calling Open will pull the remote file onto
// your local filesystem for reading/writing.  Calling Sync/Close will push the local copy
// backup to the cloud store.
type Object interface {
	Name() string
	String() string

	Updated() time.Time
	MetaData() map[string]string
	SetMetaData(meta map[string]string)

	StorageSource() string
	// Open copies the remote file to a local cache and opens the cached version
	// for read/writing.  Calling Close/Sync will push the copy back to the
	// backing store.
	Open(readonly AccessLevel) (*os.File, error)
	// Release will remove the locally cached copy of the file.  You most call Close
	// before releasing.  Release will call os.Remove(local_copy_file) so opened
	// filehandles need to be closed.
	Release() error
	// Implement io.ReadWriteCloser Open most be called before using these
	// functions.
	Read(p []byte) (n int, err error)
	Write(p []byte) (n int, err error)
	Sync() error
	Close() error

	// Delete removes the object from the cloud store.
	Delete() error
}

// ObjectIterator interface to page through objects
type ObjectIterator interface {
	Next() (Object, error)
}
