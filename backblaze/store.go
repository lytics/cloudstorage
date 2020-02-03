package backblaze

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/araddon/gou"
	"github.com/pborman/uuid"

	bz "gopkg.in/kothar/go-backblaze.v0"

	"github.com/dvriesman/cloudstorage"
)

const (

	//StoreType - Storage Type
	StoreType = "backblaze"

	//Account = Auth mechanism
	Account = "account"

	//Key = Auth mechanism
	Key = "key"

	//AuthKey = Auth Type
	AuthKey cloudstorage.AuthMethod = "account_key"
)

var (
	// Retries number of times to retry upon failures.
	Retries = 3

	// ErrNoAccount error messsage
	ErrNoAccount = fmt.Errorf("no valid backblaze account informed")

	// ErrNoAccessKey error messsage
	ErrNoAccessKey = fmt.Errorf("no valid backblaze key informed")

	// ErrNotImplemented error messsage
	ErrNotImplemented = fmt.Errorf("this method was not implemented yet")

	// ErrNotSupported error messsage
	ErrNotSupported = fmt.Errorf("this feature is not supported for backblaze")

	// ErrNoAuth error for no findable auth
	ErrNoAuth = fmt.Errorf("No auth provided")

	// ErrObjectNotFound indicate object not found
	ErrObjectNotFound = fmt.Errorf("not_found")
)

func init() {
	// Register this Driver (backblaze) in cloudstorage driver registry.
	cloudstorage.Register(StoreType, func(conf *cloudstorage.Config) (cloudstorage.Store, error) {
		client, err := NewClient(conf)
		if err != nil {
			return nil, err
		}
		return NewStore(client, conf)
	})
}

type (
	// FS Simple wrapper for accessing backblaze blob files, it doesn't currently implement a
	// Reader/Writer interface so not useful for stream reading of large files yet.
	FS struct {
		ID        string
		client    *bz.B2
		bucket    string
		cachepath string
	}

	blobRef struct {
		fs         *FS
		name       string
		bucket     *bz.Bucket
		updated    time.Time
		opened     bool
		cachedcopy *os.File
		cachepath  string
		rc         io.ReadCloser
		readonly   bool
		metadata   map[string]string
	}
)

// NewClient create new AWS s3 Client.  Uses cloudstorage.Config to read
// necessary config settings such as bucket, region, auth.
func NewClient(conf *cloudstorage.Config) (*bz.B2, error) {

	switch conf.AuthMethod {
	case AuthKey:

		account := conf.Settings.String(Account)
		if account == "" {
			return nil, ErrNoAccount
		}
		key := conf.Settings.String(Key)
		if account == "" {
			return nil, ErrNoAccessKey
		}

		b2, err := bz.NewB2(bz.Credentials{
			AccountID:      account,
			ApplicationKey: key,
		})
		if err != nil {
			gou.Warnf("could not get backblaze client %v", err)
			return nil, err
		}

		return b2, err
	}

	return nil, ErrNoAuth
}

// NewStore Create Backblaze client of type cloudstorage.Store
func NewStore(c *bz.B2, conf *cloudstorage.Config) (*FS, error) {

	if conf.TmpDir == "" {
		return nil, fmt.Errorf("unable to create cachepath. config.tmpdir=%q", conf.TmpDir)
	}

	err := os.MkdirAll(conf.TmpDir, 0775)
	if err != nil {
		return nil, fmt.Errorf("unable to create cachepath. config.tmpdir=%q err=%v", conf.TmpDir, err)
	}

	uid := uuid.NewUUID().String()
	uid = strings.Replace(uid, "-", "", -1)

	return &FS{
		client:    c,
		bucket:    conf.Bucket,
		cachepath: conf.TmpDir,
		ID:        uid,
	}, nil
}

// Type is he Store Type [google, s3, azure, localfs, etc]
func (f *FS) Type() string {
	return StoreType
}

// Client gets access to the underlying native Client for Backblaze
func (f *FS) Client() interface{} {
	return f.client
}

// Get returns an object (file) from the cloud store. The object
// isn't opened already, see Object.Open()
// ObjectNotFound will be returned if the object is not found.
func (f *FS) Get(ctx context.Context, o string) (cloudstorage.Object, error) {

	bucket, err := f.client.Bucket(f.bucket)
	if err != nil {
		return nil, err
	}

	cf := cloudstorage.CachePathObj(f.cachepath, o, f.ID)

	blobRef := &blobRef{fs: f,
		bucket:    bucket,
		name:      o,
		cachepath: cf,
		metadata:  map[string]string{cloudstorage.ContentTypeKey: cloudstorage.ContentType(o)},
	}

	return blobRef, nil

}

// Objects returns an object Iterator to allow paging through object
// which keeps track of page cursors.  Query defines the specific set
// of filters to apply to request.
func (f *FS) Objects(ctx context.Context, q cloudstorage.Query) (cloudstorage.ObjectIterator, error) {
	return nil, ErrNotImplemented
}

// List file/objects filter by given query.  This just wraps the object-iterator
// returning full list of objects.
func (f *FS) List(ctx context.Context, q cloudstorage.Query) (*cloudstorage.ObjectsResponse, error) {
	return nil, ErrNotImplemented
}

// Folders creates list of folders
func (f *FS) Folders(ctx context.Context, q cloudstorage.Query) ([]string, error) {
	return nil, ErrNotImplemented
}

// NewReader creates a new Reader to read the contents of the object.
// ErrObjectNotFound will be returned if the object is not found.
func (f *FS) NewReader(o string) (io.ReadCloser, error) {
	return f.NewReaderWithContext(context.Background(), o)
}

// NewReaderWithContext with context (for cancelation, etc)
func (f *FS) NewReaderWithContext(ctx context.Context, o string) (io.ReadCloser, error) {
	return nil, ErrNotSupported
}

// String default descriptor.
func (f *FS) String() string {
	return "backblaze"
}

// NewWriter returns a io.Writer that writes to a Cloud object
// associated with this backing Store object.
//
// A new object will be created if an object with this name already exists.
// Otherwise any previous object with the same name will be replaced.
// The object will not be available (and any previous object will remain)
// until Close has been called
func (f *FS) NewWriter(o string, metadata map[string]string) (io.WriteCloser, error) {
	return f.NewWriterWithContext(context.Background(), o, metadata)
}

// NewWriterWithContext but with context.
func (f *FS) NewWriterWithContext(ctx context.Context, o string, metadata map[string]string) (io.WriteCloser, error) {
	return nil, ErrNotSupported
}

// NewObject creates a new empty object backed by the cloud store
// This new object isn't' synced/created in the backing store
// until the object is Closed/Sync'ed.
func (f *FS) NewObject(o string) (cloudstorage.Object, error) {

	bucket, err := f.client.Bucket(f.bucket)
	if err != nil {
		return nil, err
	}

	cf := cloudstorage.CachePathObj(f.cachepath, o, f.ID)

	blobRef := &blobRef{fs: f,
		bucket:    bucket,
		name:      o,
		cachepath: cf,
		metadata:  map[string]string{cloudstorage.ContentTypeKey: cloudstorage.ContentType(o)},
	}

	return blobRef, nil
}

// Delete removes the object from the cloud store.
func (f *FS) Delete(ctx context.Context, o string) error {
	return ErrNotSupported
}

// Name of object/file.
func (b *blobRef) Name() string {
	return b.name
}

// String is default descriptor.
func (b *blobRef) String() string {
	return fmt.Sprintf("backblaze://%s/", b.bucket.Name)
}

// Updated timestamp.
func (b *blobRef) Updated() time.Time {
	return b.updated
}

// MetaData is map of arbitrary name/value pairs about object.
func (b *blobRef) MetaData() map[string]string {
	return b.metadata
}

// SetMetaData allows you to set key/value pairs.
func (b *blobRef) SetMetaData(meta map[string]string) {
	b.metadata = meta
}

// StorageSource is the type of store.
func (b *blobRef) StorageSource() string {
	return StoreType
}

// Open copies the remote file to a local cache and opens the cached version
// for read/writing.  Calling Close/Sync will push the copy back to the
// backing store.
func (b *blobRef) Open(readonly cloudstorage.AccessLevel) (*os.File, error) {

	if b.opened {
		return nil, fmt.Errorf("the store object is already opened. %s", b.name)
	}

	var errs = make([]error, 0)
	var cachedcopy *os.File
	var err error
	var xreadonly = readonly == cloudstorage.ReadOnly

	err = os.MkdirAll(path.Dir(b.cachepath), 0775)
	if err != nil {
		return nil, fmt.Errorf("error occurred creating cachedcopy dir. cachepath=%s object=%s err=%v", b.cachepath, b.name, err)
	}

	err = cloudstorage.EnsureDir(b.cachepath)
	if err != nil {
		return nil, fmt.Errorf("error occurred creating cachedcopy's dir. cachepath=%s err=%v", b.cachepath, err)
	}

	cachedcopy, err = os.Create(b.cachepath)
	if err != nil {
		return nil, fmt.Errorf("error occurred creating file. local=%s err=%v", b.cachepath, err)
	}

	for try := 0; try < Retries; try++ {

		if b.rc == nil {
			_, rc, err := b.bucket.DownloadFileByName(b.name)
			if err != nil {
				if err.Error() == "not_found: bucket "+b.bucket.Name+" does not have file: "+b.name {
					// New, this is fine
				} else {
					// lets re-try
					errs = append(errs, fmt.Errorf("error getting object err=%v", err))
					cloudstorage.Backoff(try)
					continue
				}
			}
			if rc != nil {
				b.rc = rc
			}
		}

		if b.rc != nil {
			// we have a preexisting object, so lets download it..
			defer b.rc.Close()

			if _, err := cachedcopy.Seek(0, os.SEEK_SET); err != nil {
				return nil, fmt.Errorf("error seeking to start of cachedcopy err=%v", err) //don't retry on local fs errors
			}

			_, err = io.Copy(cachedcopy, b.rc)
			if err != nil {
				errs = append(errs, fmt.Errorf("error coping bytes. err=%v", err))
				//recreate the cachedcopy file incase it has incomplete data
				if err := os.Remove(b.cachepath); err != nil {
					return nil, fmt.Errorf("error resetting the cachedcopy err=%v", err) //don't retry on local fs errors
				}
				if cachedcopy, err = os.Create(b.cachepath); err != nil {
					return nil, fmt.Errorf("error creating a new cachedcopy file. local=%s err=%v", b.cachepath, err)
				}

				cloudstorage.Backoff(try)
				continue
			}
		}

		if xreadonly {
			cachedcopy.Close()
			cachedcopy, err = os.Open(b.cachepath)
			if err != nil {
				name := "unknown"
				if cachedcopy != nil {
					name = cachedcopy.Name()
				}
				return nil, fmt.Errorf("error opening file. local=%s object=%s tfile=%v err=%v", b.cachepath, b.name, name, err)
			}
		} else {
			if _, err := cachedcopy.Seek(0, os.SEEK_SET); err != nil {
				return nil, fmt.Errorf("error seeking to start of cachedcopy err=%v", err) //don't retry on local fs errors
			}
		}

		b.cachedcopy = cachedcopy
		b.readonly = xreadonly
		b.opened = true
		return b.cachedcopy, nil

	}

	return nil, fmt.Errorf("fetch error retry cnt reached: obj=%s tfile=%v errs:[%v]", b.name, b.cachepath, errs)
}

// Release will remove the locally cached copy of the file.  You most call Close
// before releasing.  Release will call os.Remove(local_copy_file) so opened
// filehandles need to be closed.
func (b *blobRef) Release() error {
	if b.cachedcopy != nil {
		gou.Debugf("release %q vs %q", b.cachedcopy.Name(), b.cachepath)
		b.cachedcopy.Close()
		return os.Remove(b.cachepath)
	}
	os.Remove(b.cachepath)
	return nil
}



// Implement io.ReadWriteCloser Open most be called before using these
// functions.
func (b *blobRef) Read(p []byte) (n int, err error) {
	return b.cachedcopy.Read(p)
}

func (b *blobRef) Write(p []byte) (n int, err error) {
	if b.cachedcopy == nil {
		_, err := b.Open(cloudstorage.ReadWrite)
		if err != nil {
			return 0, err
		}
	}
	return b.cachedcopy.Write(p)
}

func (b *blobRef) Sync() error {
	if !b.opened {
		return fmt.Errorf("object isn't opened object:%s", b.name)
	}
	if b.readonly {
		return fmt.Errorf("trying to Sync a readonly object:%s", b.name)
	}

	cachedcopy, err := os.OpenFile(b.cachepath, os.O_RDWR, 0664)
	if err != nil {
		return fmt.Errorf("couldn't open localfile for sync'ing. local=%s err=%v", b.cachepath, err)
	}
	defer cachedcopy.Close()

	if _, err := cachedcopy.Seek(0, os.SEEK_SET); err != nil {
		return fmt.Errorf("error seeking to start of cachedcopy err=%v", err) //don't retry on local filesystem errors
	}

	// Upload the file
	if _, err = b.bucket.UploadFile(b.name, b.metadata, cachedcopy); err != nil {
		gou.Warnf("could not upload %v", err)
		return fmt.Errorf("failed to upload file, %v", err)
	}
	return nil
}

func (b *blobRef) Close() error {
	if !b.opened {
		return nil
	}
	defer func() {
		os.Remove(b.cachepath)
		b.cachedcopy = nil
		b.opened = false
	}()

	serr := b.cachedcopy.Sync()
	cerr := b.cachedcopy.Close()
	if serr != nil || cerr != nil {
		return fmt.Errorf("error on sync and closing localfile. %s sync=%v, err=%v", b.cachepath, serr, cerr)
	}

	if b.opened && !b.readonly {
		err := b.Sync()
		if err != nil {
			gou.Errorf("error on sync %v", err)
			return err
		}
	}
	return nil
}

// File returns the cached/local copy of the file
func (b *blobRef) File() *os.File {
	return b.cachedcopy
}

// Delete removes the object from the cloud store and local cache.
func (b *blobRef) Delete() error {
	return ErrNotImplemented
}

func (b *blobRef) AcquireLease(uid string) (string, error) {
	return "", nil
}