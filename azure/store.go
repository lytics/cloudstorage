package azure

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	az "github.com/Azure/azure-sdk-for-go/storage"
	u "github.com/araddon/gou"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"

	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/csbufio"
)

const (
	// StoreType = "azure" this is used to define the storage type to create
	// from cloudstorage.NewStore(config)
	StoreType = "azure"

	// Configuration Keys.  These are the names of keys
	// to look for in the json map[string]string to extract for config.

	// ConfKeyAuthKey config key name of the azure api key for auth
	ConfKeyAuthKey = "azure_key"

	// Authentication Source's

	// AuthKey is for using azure api key
	AuthKey cloudstorage.AuthMethod = "azure_key"
)

var (
	// Retries number of times to retry upon failures.
	Retries = 3
	// PageSize is default page size
	PageSize = 2000

	// ErrNoAzureSession no valid session
	ErrNoAzureSession = fmt.Errorf("no valid azure session was created")
	// ErrNoAccessKey error for no azure_key
	ErrNoAccessKey = fmt.Errorf("no settings.azure_key")
	// ErrNoAuth error for no findable auth
	ErrNoAuth = fmt.Errorf("No auth provided")
)

func init() {
	// Register this Driver (azure) in cloudstorage driver registry.
	cloudstorage.Register(StoreType, func(conf *cloudstorage.Config) (cloudstorage.Store, error) {
		client, sess, err := NewClient(conf)
		if err != nil {
			return nil, err
		}
		return NewStore(client, sess, conf)
	})
}

type (
	// FS Simple wrapper for accessing azure blob files, it doesn't currently implement a
	// Reader/Writer interface so not useful for stream reading of large files yet.
	FS struct {
		PageSize   int
		ID         string
		baseClient *az.Client
		client     *az.BlobStorageClient
		endpoint   string
		bucket     string
		cachepath  string
	}

	object struct {
		fs         *FS
		o          *az.Blob
		cachedcopy *os.File
		rc         io.ReadCloser

		name      string    // aka "id" in azure
		updated   time.Time // LastModified in azure
		metadata  map[string]string
		bucket    string
		readonly  bool
		opened    bool
		cachepath string

		//infoOnce sync.Once
		infoErr error
	}
)

// NewClient create new AWS s3 Client.  Uses cloudstorage.Config to read
// necessary config settings such as bucket, region, auth.
func NewClient(conf *cloudstorage.Config) (*az.Client, *az.BlobStorageClient, error) {

	switch conf.AuthMethod {
	case AuthKey:
		accessKey := conf.Settings.String(ConfKeyAuthKey)
		if accessKey == "" {
			u.Warnf("no access key %v", conf.Settings)
			return nil, nil, ErrNoAccessKey
		}
		basicClient, err := az.NewBasicClient(conf.Project, accessKey)
		if err != nil {
			u.Warnf("could not get azure client %v", err)
			return nil, nil, err
		}
		client := basicClient.GetBlobService()
		return &basicClient, &client, err
	}

	return nil, nil, ErrNoAuth
}

// NewStore Create AWS S3 storage client of type cloudstorage.Store
func NewStore(c *az.Client, blobClient *az.BlobStorageClient, conf *cloudstorage.Config) (*FS, error) {

	if conf.TmpDir == "" {
		return nil, fmt.Errorf("unable to create cachepath. config.tmpdir=%q", conf.TmpDir)
	}
	err := os.MkdirAll(path.Dir(conf.TmpDir), 0775)
	if err != nil {
		return nil, fmt.Errorf("unable to create cachepath. config.tmpdir=%q err=%v", conf.TmpDir, err)
	}

	uid := uuid.NewUUID().String()
	uid = strings.Replace(uid, "-", "", -1)

	return &FS{
		baseClient: c,
		client:     blobClient,
		bucket:     conf.Bucket,
		cachepath:  conf.TmpDir,
		ID:         uid,
		PageSize:   10000,
	}, nil
}

// Type of store = "azure"
func (f *FS) Type() string {
	return StoreType
}

// Client gets access to the underlying google cloud storage client.
func (f *FS) Client() interface{} {
	return f.client
}

// String function to provide azure://..../file   path
func (f *FS) String() string {
	return fmt.Sprintf("azure://%s/", f.bucket)
}

// NewObject of Type azure.
func (f *FS) NewObject(objectname string) (cloudstorage.Object, error) {
	obj, err := f.Get(context.Background(), objectname)
	if err != nil && err != cloudstorage.ErrObjectNotFound {
		return nil, err
	} else if obj != nil {
		return nil, cloudstorage.ErrObjectExists
	}

	cf := cloudstorage.CachePathObj(f.cachepath, objectname, f.ID)

	return &object{
		fs:         f,
		name:       objectname,
		metadata:   map[string]string{cloudstorage.ContentTypeKey: cloudstorage.ContentType(objectname)},
		bucket:     f.bucket,
		cachedcopy: nil,
		cachepath:  cf,
	}, nil
}

// Get a single File Object
func (f *FS) Get(ctx context.Context, objectpath string) (cloudstorage.Object, error) {

	obj, err := f.getObject(ctx, objectpath)
	if err != nil {
		return nil, err
	} else if obj == nil {
		return nil, cloudstorage.ErrObjectNotFound
	}

	return obj, nil
}

// get single object
func (f *FS) getObject(ctx context.Context, objectname string) (*object, error) {

	blob := f.client.GetContainerReference(f.bucket).GetBlobReference(objectname)
	err := blob.GetProperties(nil)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return nil, cloudstorage.ErrObjectNotFound
		}
		return nil, err
	}
	o := &object{
		name: objectname,
		fs:   f,
		o:    blob,
	}

	o.o.Properties.Etag = cloudstorage.CleanETag(o.o.Properties.Etag)
	o.updated = time.Time(o.o.Properties.LastModified)
	o.cachepath = cloudstorage.CachePathObj(f.cachepath, o.name, f.ID)

	return o, nil
	//return newObjectFromHead(f, objectname, res), nil
}

func (f *FS) getOpenObject(ctx context.Context, objectname string) (io.ReadCloser, error) {
	rc, err := f.client.GetContainerReference(f.bucket).GetBlobReference(objectname).Get(nil)
	if err != nil && strings.Contains(err.Error(), "404") {
		return nil, cloudstorage.ErrObjectNotFound
	} else if err != nil {
		return nil, err
	}
	return rc, nil
}

func convertMetaData(m map[string]*string) (map[string]string, error) {
	result := make(map[string]string, len(m))
	for key, value := range m {
		if value != nil {
			result[strings.ToLower(key)] = *value
		} else {
			result[strings.ToLower(key)] = ""
		}

	}
	return result, nil
}

// List objects from this store.
func (f *FS) List(ctx context.Context, q cloudstorage.Query) (*cloudstorage.ObjectsResponse, error) {

	itemLimit := uint(f.PageSize)
	if q.PageSize > 0 {
		itemLimit = uint(q.PageSize)
	}

	params := az.ListBlobsParameters{
		Prefix:     q.Prefix,
		MaxResults: itemLimit,
		Marker:     q.Marker,
	}

	blobs, err := f.client.GetContainerReference(f.bucket).ListBlobs(params)
	if err != nil {
		return nil, err
	}
	objResp := &cloudstorage.ObjectsResponse{
		Objects: make(cloudstorage.Objects, len(blobs.Blobs)),
	}

	for i, o := range blobs.Blobs {
		objResp.Objects[i] = newObject(f, &o)
	}
	objResp.NextMarker = blobs.NextMarker
	q.Marker = blobs.NextMarker

	return objResp, nil
}

// Objects returns an iterator over the objects in the google bucket that match the Query q.
// If q is nil, no filtering is done.
func (f *FS) Objects(ctx context.Context, q cloudstorage.Query) (cloudstorage.ObjectIterator, error) {
	return cloudstorage.NewObjectPageIterator(ctx, f, q), nil
}

// Folders get folders list.
func (f *FS) Folders(ctx context.Context, q cloudstorage.Query) ([]string, error) {

	q.Delimiter = "/"

	// Think we should just put 1 here right?
	itemLimit := uint(f.PageSize)
	if q.PageSize > 0 {
		itemLimit = uint(q.PageSize)
	}

	params := az.ListBlobsParameters{
		Prefix:     q.Prefix,
		MaxResults: itemLimit,
		Delimiter:  "/",
	}

	for {
		select {
		case <-ctx.Done():
			// If has been closed
			return nil, ctx.Err()
		default:
			// if q.Marker != "" {
			// 	params.Marker = &q.Marker
			// }
			blobs, err := f.client.GetContainerReference(f.bucket).ListBlobs(params)
			if err != nil {
				u.Warnf("leaving %v", err)
				return nil, err
			}
			if len(blobs.BlobPrefixes) > 0 {
				return blobs.BlobPrefixes, nil
			}
			return nil, nil
		}
	}
}

/*
// Copy from src to destination
func (f *FS) Copy(ctx context.Context, src, des cloudstorage.Object) error {

	so, ok := src.(*object)
	if !ok {
		return fmt.Errorf("Copy source file expected s3 but got %T", src)
	}
	do, ok := des.(*object)
	if !ok {
		return fmt.Errorf("Copy destination expected s3 but got %T", des)
	}

	oh := so.b.Object(so.name)
	dh := do.b.Object(do.name)

	_, err := dh.CopierFrom(oh).Run(ctx)
	return err
}

// Move which is a Copy & Delete
func (f *FS) Move(ctx context.Context, src, des cloudstorage.Object) error {

	so, ok := src.(*object)
	if !ok {
		return fmt.Errorf("Move source file expected s3 but got %T", src)
	}
	do, ok := des.(*object)
	if !ok {
		return fmt.Errorf("Move destination expected s3 but got %T", des)
	}

	oh := so.b.Object(so.name)
	dh := do.b.Object(des.name)

	if _, err := dh.CopierFrom(oh).Run(ctx); err != nil {
		return err
	}

	return oh.Delete(ctx)
}
*/
// NewReader create file reader.
func (f *FS) NewReader(o string) (io.ReadCloser, error) {
	return f.NewReaderWithContext(context.Background(), o)
}

// NewReaderWithContext create new File reader with context.
func (f *FS) NewReaderWithContext(ctx context.Context, objectname string) (io.ReadCloser, error) {
	ioc, err := f.client.GetContainerReference(f.bucket).GetBlobReference(objectname).Get(nil)
	if err != nil {
		// translate the string error to typed error
		if strings.Contains(err.Error(), "404") {
			return nil, cloudstorage.ErrObjectNotFound
		}
		return nil, err
	}
	return ioc, nil
}

// NewWriter create Object Writer.
func (f *FS) NewWriter(objectName string, metadata map[string]string) (io.WriteCloser, error) {
	return f.NewWriterWithContext(context.Background(), objectName, metadata)
}

// NewWriterWithContext create writer with provided context and metadata.
func (f *FS) NewWriterWithContext(ctx context.Context, name string, metadata map[string]string) (io.WriteCloser, error) {

	name = strings.Replace(name, " ", "+", -1)

	pr, pw := io.Pipe()
	bw := csbufio.NewWriter(pw)
	o := &object{name: name, metadata: metadata}

	go func() {
		// TODO:  this needs to be managed, ie shutdown signals, close, handler err etc.

		// Upload the file to azure.
		// Do a multipart upload
		err := f.uploadMultiPart(o, pr)
		if err != nil {
			u.Warnf("could not upload %v", err)
		}
	}()

	return bw, nil
}

const (
	// constants related to chunked uploads
	initialChunkSize = 4 * 1024 * 1024
	maxChunkSize     = 100 * 1024 * 1024
	maxParts         = 50000
)

func makeBlockID(id uint64) string {
	bytesID := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesID, id)
	return base64.StdEncoding.EncodeToString(bytesID)
}

// uploadMultiPart start an upload
func (f *FS) uploadMultiPart(o *object, r io.Reader) error {

	//chunkSize, err := calcBlockSize(size)
	// if err != nil {
	// 	return err
	// }
	var buf = make([]byte, initialChunkSize)

	var blocks []az.Block
	var rawID uint64

	blob := f.client.GetContainerReference(f.bucket).GetBlobReference(o.name)

	// TODO: performance improvement to mange uploads in separate
	// go-routine than the reader
	for {
		n, err := r.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			u.Warnf("unknown err=%v", err)
			return err
		}

		blockID := makeBlockID(rawID)
		chunk := buf[:n]

		if err := blob.PutBlock(blockID, chunk, nil); err != nil {
			return err
		}

		blocks = append(blocks, az.Block{
			ID:     blockID,
			Status: az.BlockStatusLatest,
		})
		rawID++
	}

	err := blob.PutBlockList(blocks, nil)
	if err != nil {
		u.Warnf("could not put block list %v", err)
		return err
	}

	err = blob.GetProperties(nil)
	if err != nil {
		u.Warnf("could not load blog properties %v", err)
		return err
	}

	blob.Metadata = o.metadata

	err = blob.SetMetadata(nil)
	if err != nil {
		u.Warnf("can't set metadata err=%v", err)
		return err
	}
	return nil
}

// Delete requested object path string.
func (f *FS) Delete(ctx context.Context, name string) error {
	err := f.client.GetContainerReference(f.bucket).GetBlobReference(name).Delete(nil)
	if err != nil && strings.Contains(err.Error(), "404") {
		return cloudstorage.ErrObjectNotFound
	}
	return err
}

func newObject(f *FS, o *az.Blob) *object {
	obj := &object{
		fs:        f,
		o:         o,
		name:      o.Name,
		bucket:    f.bucket,
		cachepath: cloudstorage.CachePathObj(f.cachepath, o.Name, f.ID),
	}
	obj.o.Properties.Etag = cloudstorage.CleanETag(obj.o.Properties.Etag)
	return obj
}
func newObjectFromHead(f *FS, name string, o *s3.HeadObjectOutput) *object {
	obj := &object{
		fs:        f,
		name:      name,
		bucket:    f.bucket,
		cachepath: cloudstorage.CachePathObj(f.cachepath, name, f.ID),
	}
	if o.LastModified != nil {
		obj.updated = *o.LastModified
	}
	// metadata?
	obj.metadata, _ = convertMetaData(o.Metadata)
	return obj
}

func (o *object) StorageSource() string {
	return StoreType
}
func (o *object) Name() string {
	return o.name
}
func (o *object) String() string {
	return o.name
}
func (o *object) Updated() time.Time {
	return o.updated
}
func (o *object) MetaData() map[string]string {
	return o.metadata
}
func (o *object) SetMetaData(meta map[string]string) {
	o.metadata = meta
}

func (o *object) Delete() error {
	return o.fs.Delete(context.Background(), o.name)
}

func (o *object) Open(accesslevel cloudstorage.AccessLevel) (*os.File, error) {
	if o.opened {
		return nil, fmt.Errorf("the store object is already opened. %s", o.name)
	}

	var errs []error = make([]error, 0)
	var cachedcopy *os.File = nil
	var err error
	var readonly = accesslevel == cloudstorage.ReadOnly

	err = os.MkdirAll(path.Dir(o.cachepath), 0775)
	if err != nil {
		return nil, fmt.Errorf("error occurred creating cachedcopy dir. cachepath=%s object=%s err=%v", o.cachepath, o.name, err)
	}

	err = cloudstorage.EnsureDir(o.cachepath)
	if err != nil {
		return nil, fmt.Errorf("error occurred creating cachedcopy's dir. cachepath=%s err=%v", o.cachepath, err)
	}

	cachedcopy, err = os.Create(o.cachepath)
	if err != nil {
		return nil, fmt.Errorf("error occurred creating file. local=%s err=%v", o.cachepath, err)
	}

	for try := 0; try < Retries; try++ {
		if o.rc == nil {
			rc, err := o.fs.getOpenObject(context.Background(), o.name)
			if err != nil {
				if err == cloudstorage.ErrObjectNotFound {
					// New, this is fine
				} else {
					// lets re-try
					errs = append(errs, fmt.Errorf("error getting object err=%v", err))
					cloudstorage.Backoff(try)
					continue
				}
			}

			if rc != nil {
				o.rc = rc
			}
		}

		if o.rc != nil {
			// we have a preexisting object, so lets download it..
			defer o.rc.Close()

			if _, err := cachedcopy.Seek(0, os.SEEK_SET); err != nil {
				return nil, fmt.Errorf("error seeking to start of cachedcopy err=%v", err) //don't retry on local fs errors
			}

			_, err = io.Copy(cachedcopy, o.rc)
			if err != nil {
				errs = append(errs, fmt.Errorf("error coping bytes. err=%v", err))
				//recreate the cachedcopy file incase it has incomplete data
				if err := os.Remove(o.cachepath); err != nil {
					return nil, fmt.Errorf("error resetting the cachedcopy err=%v", err) //don't retry on local fs errors
				}
				if cachedcopy, err = os.Create(o.cachepath); err != nil {
					return nil, fmt.Errorf("error creating a new cachedcopy file. local=%s err=%v", o.cachepath, err)
				}

				cloudstorage.Backoff(try)
				continue
			}
		}

		if readonly {
			cachedcopy.Close()
			cachedcopy, err = os.Open(o.cachepath)
			if err != nil {
				name := "unknown"
				if cachedcopy != nil {
					name = cachedcopy.Name()
				}
				return nil, fmt.Errorf("error opening file. local=%s object=%s tfile=%v err=%v", o.cachepath, o.name, name, err)
			}
		}

		o.cachedcopy = cachedcopy
		o.readonly = readonly
		o.opened = true
		return o.cachedcopy, nil
	}

	return nil, fmt.Errorf("fetch error retry cnt reached: obj=%s tfile=%v errs:[%v]", o.name, o.cachepath, errs)
}

func (o *object) File() *os.File {
	return o.cachedcopy
}
func (o *object) Read(p []byte) (n int, err error) {
	return o.cachedcopy.Read(p)
}
func (o *object) Write(p []byte) (n int, err error) {
	return o.cachedcopy.Write(p)
}

func (o *object) Sync() error {

	if !o.opened {
		return fmt.Errorf("object isn't opened object:%s", o.name)
	}
	if o.readonly {
		return fmt.Errorf("trying to Sync a readonly object:%s", o.name)
	}

	cachedcopy, err := os.OpenFile(o.cachepath, os.O_RDWR, 0664)
	if err != nil {
		return fmt.Errorf("couldn't open localfile for sync'ing. local=%s err=%v", o.cachepath, err)
	}
	defer cachedcopy.Close()

	if _, err := cachedcopy.Seek(0, os.SEEK_SET); err != nil {
		return fmt.Errorf("error seeking to start of cachedcopy err=%v", err) //don't retry on local filesystem errors
	}

	// Upload the file
	if err = o.fs.uploadMultiPart(o, cachedcopy); err != nil {
		u.Warnf("could not upload %v", err)
		return fmt.Errorf("failed to upload file, %v", err)
	}
	return nil
}

func (o *object) Close() error {
	if !o.opened {
		return nil
	}
	defer func() {
		os.Remove(o.cachepath)
		o.cachedcopy = nil
		o.opened = false
	}()

	serr := o.cachedcopy.Sync()
	cerr := o.cachedcopy.Close()
	if serr != nil || cerr != nil {
		return fmt.Errorf("error on sync and closing localfile. %s sync=%v, err=%v", o.cachepath, serr, cerr)
	}

	if o.opened && !o.readonly {
		err := o.Sync()
		if err != nil {
			u.Errorf("error on sync %v", err)
			return err
		}
	}
	return nil
}

func (o *object) Release() error {
	if o.cachedcopy != nil {
		o.cachedcopy.Close()
	}
	return os.Remove(o.cachepath)
}
