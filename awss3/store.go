package awss3

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"

	"github.com/lytics/cloudstorage"
)

const (
	// StoreType = "s3"
	StoreType = "s3"

	ConfKeyAccessKey    = "access_key"
	ConfKeyAccessSecret = "access_secret"
	ConfKeyARN          = "arn"
	ConfKeyDisableSSL   = "disable_ssl"
	// Authentication Source's

	// AuthAccessKey is for using aws access key/secret pairs
	AuthAccessKey cloudstorage.AuthMethod = "aws_access_key"
)

var (
	// Retries number of times to retry.
	Retries int = 55

	// Ensure we implement ObjectIterator
	_ cloudstorage.ObjectIterator = (*ObjectIterator)(nil)

	// ErrNoS3Session no valid session
	ErrNoS3Session = fmt.Errorf("no valid aws session was created")
	// ErrNoAccessKey
	ErrNoAccessKey = fmt.Errorf("no settings.access_key")
	// ErrNoAccessSecret
	ErrNoAccessSecret = fmt.Errorf("no settings.access_secret")
	// ErrNoAuth
	ErrNoAuth = fmt.Errorf("No auth provided")
)

func init() {
	cloudstorage.Register(StoreType, func(conf *cloudstorage.Config) (cloudstorage.Store, error) {
		client, err := NewClient(conf)
		if err != nil {
			return nil, err
		}
		return NewStore(client, conf)
	})
}

// NewClient create new AWS s3 Client.
func NewClient(conf *cloudstorage.Config) (client *s3.S3, err error) {

	awsConf := aws.NewConfig().
		WithHTTPClient(http.DefaultClient).
		WithMaxRetries(aws.UseServiceDefaultRetries).
		WithLogger(aws.NewDefaultLogger()).
		WithLogLevel(aws.LogOff).
		WithSleepDelay(time.Sleep)

	if conf.Region != "" {
		awsConf.WithRegion(conf.Region)
	} else {
		awsConf.WithRegion("us-east-1")
	}

	switch conf.AuthMethod {
	case AuthAccessKey:
		accessKey := conf.Settings.String(ConfKeyAccessKey)
		if accessKey == "" {
			return nil, ErrNoAccessKey
		}
		secretKey := conf.Settings.String(ConfKeyAccessSecret)
		if secretKey == "" {
			return nil, ErrNoAccessSecret
		}
		awsConf.WithCredentials(credentials.NewStaticCredentials(accessKey, secretKey, ""))
	default:
		return nil, ErrNoAuth
	}

	if conf.BaseUrl != "" {
		awsConf.WithEndpoint(conf.BaseUrl).WithS3ForcePathStyle(true)
	}

	disableSSL := conf.Settings.Bool(ConfKeyDisableSSL)
	if disableSSL {
		awsConf.WithDisableSSL(true)
	}

	sess := session.New(awsConf)
	if sess == nil {
		return nil, ErrNoS3Session
	}

	s3Client := s3.New(sess)

	return s3Client, nil
}

// FS Simple wrapper for accessing s3 files, it doesn't currently implement a
// Reader/Writer interface so not useful for stream reading of large files yet.
type FS struct {
	client    *s3.S3
	endpoint  string
	bucket    string
	cachepath string
	PageSize  int
	Id        string
}

// NewStore Create AWS S3 storage client.
func NewStore(c *s3.S3, conf *cloudstorage.Config) (*FS, error) {

	// , bucket, cachepath string, pagesize int
	// conf.Bucket, conf.TmpDir, cloudstorage.MaxResults
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
		client:    c,
		bucket:    conf.Bucket,
		cachepath: conf.TmpDir,
		Id:        uid,
		PageSize:  cloudstorage.MaxResults,
	}, nil
}

// Type of store = "s3"
func (f *FS) Type() string {
	return StoreType
}

// Client gets access to the underlying google cloud storage client.
func (f *FS) Client() interface{} {
	return f.client
}

// String function to provide s3://..../file   path
func (f *FS) String() string {
	return fmt.Sprintf("s3://%s/", f.bucket)
}

/*
func (f *FS) b() *s3.Bucket {
	return f.client.Bucket(f.bucket)
}
*/
// NewObject of Type s3.
func (f *FS) NewObject(objectname string) (cloudstorage.Object, error) {
	obj, err := f.Get(objectname)
	if err != nil && err != cloudstorage.ErrObjectNotFound {
		return nil, err
	} else if obj != nil {
		return nil, cloudstorage.ErrObjectExists
	}

	cf := cloudstorage.CachePathObj(f.cachepath, objectname, f.Id)

	return &object{
		name:       objectname,
		metadata:   map[string]string{cloudstorage.ContextTypeKey: cloudstorage.ContentType(objectname)},
		bucket:     f.bucket,
		cachedcopy: nil,
		cachepath:  cf,
	}, nil
}

// Get Gets a single File Object
func (f *FS) Get(objectpath string) (cloudstorage.Object, error) {

	obj, err := f.getObject(objectpath)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") {
			return nil, cloudstorage.ErrObjectNotFound
		}
		return nil, err
	}

	if obj == nil {
		return nil, cloudstorage.ErrObjectNotFound
	}

	return newObject(f, obj), nil
}

// get single object
func (f *FS) getObject(objectname string) (*object, error) {

	res, err := f.client.GetObject(&s3.GetObjectInput{
		Key:    aws.String(objectname),
		Bucket: aws.String(f.bucket),
	})
	if err != nil {
		// translate the string error to typed error
		if strings.Contains(err.Error(), "NoSuchKey") {
			return nil, cloudstorage.ErrObjectNotFound
		}
		return nil, err
	}
	defer res.Body.Close()

	obj := &object{
		name:       objectname,
		metadata:   map[string]string{cloudstorage.ContextTypeKey: cloudstorage.ContentType(objectname)},
		bucket:     f.bucket,
		cachedcopy: nil,
		cachepath:  cloudstorage.CachePathObj(f.cachepath, objectname, f.Id),
		fs:         f,
	}
	/*
		properties: properties{
			ETag:         &etag,
			Key:          &id,
			LastModified: res.LastModified,
			Owner:        nil,
			Size:         res.ContentLength,
			StorageClass: res.StorageClass,
			Metadata:     md,
		},
	*/
	return obj, nil
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
func (f *FS) List(q cloudstorage.Query) (cloudstorage.Objects, error) {

	res, err := f.listObjects(q, Retries)
	if err != nil {
		return nil, err
	}

	if res == nil {
		return make(cloudstorage.Objects, 0), nil
	}

	res = q.ApplyFilters(res)

	return res, nil
}

// Objects returns an iterator over the objects in the google bucket that match the Query q.
// If q is nil, no filtering is done.
func (f *FS) Objects(ctx context.Context, q cloudstorage.Query) cloudstorage.ObjectIterator {
	qry := &storage.Query{Prefix: q.Prefix}
	iter := f.b().Objects(ctx, qry)
	return &ObjectIterator{f, ctx, iter}
}

// ListObjects iterates to find a list of objects
func (f *FS) listObjects(q cloudstorage.Query, retries int) (cloudstorage.Objects, error) {
	var lasterr error

	for i := 0; i < retries; i++ {
		objects := make(cloudstorage.Objects, 0)
		iter := f.b().Objects(context.Background(), q)
	iterLoop:
		for {
			oa, err := iter.Next()
			switch err {
			case nil:
				objects = append(objects, newObject(f, oa))
			case iterator.Done:
				return objects, nil
			default:
				lasterr = err
				backoff(i)
				break iterLoop
			}
		}
	}
	return nil, lasterr
}

// Folders get folders list.
func (f *FS) Folders(ctx context.Context, csq cloudstorage.Query) ([]string, error) {
	var q = &storage.Query{Delimiter: csq.Delimiter, Prefix: csq.Prefix}
	iter := f.b().Objects(ctx, q)
	folders := make([]string, 0)
	for {
		select {
		case <-ctx.Done():
			// If has been closed
			return folders, ctx.Err()
		default:
			o, err := iter.Next()
			if err == nil {
				if o.Prefix != "" {
					folders = append(folders, o.Prefix)
				}
			} else if err == iterator.Done {
				return folders, nil
			} else if err == context.Canceled || err == context.DeadlineExceeded {
				// Return to user
				return nil, err
			}
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
// NewReader create GCS file reader.
func (f *FS) NewReader(o string) (io.ReadCloser, error) {
	return f.NewReaderWithContext(context.Background(), o)
}

// NewReaderWithContext create new GCS File reader with context.
func (f *FS) NewReaderWithContext(ctx context.Context, o string) (io.ReadCloser, error) {
	rc, err := f.gcsb().Object(o).NewReader(ctx)
	if err == storage.ErrObjectNotExist {
		return rc, cloudstorage.ErrObjectNotFound
	}
	return rc, err
}

// NewWriter create GCS Object Writer.
func (f *FS) NewWriter(o string, metadata map[string]string) (io.WriteCloser, error) {
	return f.NewWriterWithContext(context.Background(), o, metadata)
}

// NewWriterWithContext create writer with provided context and metadata.
func (f *FS) NewWriterWithContext(ctx context.Context, o string, metadata map[string]string) (io.WriteCloser, error) {
	wc := f.gcsb().Object(o).NewWriter(ctx)
	if metadata != nil {
		wc.Metadata = metadata
		//contenttype is only used for viewing the file in a browser. (i.e. the GCS Object browser).
		ctype := cloudstorage.EnsureContextType(o, metadata)
		wc.ContentType = ctype
	}
	return wc, nil
}

// Delete requested object path string.
func (f *FS) Delete(obj string) error {
	err := f.gcsb().Object(obj).Delete(context.Background())
	if err != nil {
		return err
	}
	return nil
}

type object struct {
	name       string
	updated    time.Time
	metadata   map[string]string
	fs         *FS
	o          *s3.Object
	bucket     string
	cachedcopy *os.File
	readonly   bool
	opened     bool
	cachepath  string
}

func newObject(f *FS, bucket string, o *storage.ObjectAttrs) *object {
	return &object{
		name:      o.Name,
		updated:   o.Updated,
		metadata:  o.Metadata,
		bucket:    bucket,
		cachepath: cloudstorage.CachePathObj(f.cachepath, o.Name, f.Id),
	}
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
	o.Release()
	return o.b.Object(o.name).Delete(context.Background())
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
		return nil, fmt.Errorf("error occurred creating cachedcopy dir. cachepath=%s object=%s err=%v",
			o.cachepath, o.name, err)
	}

	err = cloudstorage.EnsureDir(o.cachepath)
	if err != nil {
		return nil, fmt.Errorf("error occurred creating cachedcopy's dir. cachepath=%s err=%v",
			o.cachepath, err)
	}

	cachedcopy, err = os.Create(o.cachepath)
	if err != nil {
		return nil, fmt.Errorf("error occurred creating file. local=%s err=%v",
			o.cachepath, err)
	}

	for try := 0; try < GCSRetries; try++ {
		if o.googleObject == nil {
			gobj, err := o.gcsb.Object(o.name).Attrs(context.Background())
			if err != nil {
				if strings.Contains(err.Error(), "doesn't exist") {
					// New, this is fine
				} else {
					errs = append(errs, fmt.Errorf("error storage.NewReader err=%v", err))
					backoff(try)
					continue
				}
			}

			if gobj != nil {
				o.googleObject = gobj
			}
		}

		if o.googleObject != nil {
			//we have a preexisting object, so lets download it..
			rc, err := o.gcsb.Object(o.name).NewReader(context.Background())
			if err != nil {
				errs = append(errs, fmt.Errorf("error storage.NewReader err=%v", err))
				backoff(try)
				continue
			}
			defer rc.Close()

			if _, err := cachedcopy.Seek(0, os.SEEK_SET); err != nil {
				return nil, fmt.Errorf("error seeking to start of cachedcopy err=%v", err) //don't retry on local fs errors
			}

			_, err = io.Copy(cachedcopy, rc)
			if err != nil {
				errs = append(errs, fmt.Errorf("error coping bytes. err=%v", err))
				//recreate the cachedcopy file incase it has incomplete data
				if err := os.Remove(o.cachepath); err != nil {
					return nil, fmt.Errorf("error resetting the cachedcopy err=%v", err) //don't retry on local fs errors
				}
				if cachedcopy, err = os.Create(o.cachepath); err != nil {
					return nil, fmt.Errorf("error creating a new cachedcopy file. local=%s err=%v", o.cachepath, err)
				}

				backoff(try)
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

	var errs = make([]string, 0)

	cachedcopy, err := os.OpenFile(o.cachepath, os.O_RDWR, 0664)
	if err != nil {
		return fmt.Errorf("couldn't open localfile for sync'ing. local=%s err=%v",
			o.cachepath, err)
	}
	defer cachedcopy.Close()

	for try := 0; try < GCSRetries; try++ {
		if _, err := cachedcopy.Seek(0, os.SEEK_SET); err != nil {
			return fmt.Errorf("error seeking to start of cachedcopy err=%v", err) //don't retry on local filesystem errors
		}
		rd := bufio.NewReader(cachedcopy)

		wc := o.gcsb.Object(o.name).NewWriter(context.Background())

		if o.metadata != nil {
			wc.Metadata = o.metadata
			//contenttype is only used for viewing the file in a browser. (i.e. the GCS Object browser).
			ctype := cloudstorage.EnsureContextType(o.name, o.metadata)
			wc.ContentType = ctype
		}

		if _, err = io.Copy(wc, rd); err != nil {
			errs = append(errs, fmt.Sprintf("copy to remote object error:%v", err))
			err2 := wc.CloseWithError(err)
			if err2 != nil {
				errs = append(errs, fmt.Sprintf("CloseWithError error:%v", err2))
			}
			backoff(try)
			continue
		}

		if err = wc.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("close gcs writer error:%v", err))
			backoff(try)
			continue
		}

		return nil
	}

	errmsg := strings.Join(errs, ",")
	return fmt.Errorf("S3 sync error after retry: (oname=%s cpath:%v) errors[%v]", o.name, o.cachepath, errmsg)
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

// backoff sleeps a random amount so we can.
// retry failed requests using a randomized exponential backoff:
// wait a random period between [0..1] seconds and retry; if that fails,
// wait a random period between [0..2] seconds and retry; if that fails,
// wait a random period between [0..4] seconds and retry, and so on,
// with an upper bounds to the wait period being 16 seconds.
// http://play.golang.org/p/l9aUHgiR8J
func backoff(try int) {
	nf := math.Pow(2, float64(try))
	nf = math.Max(1, nf)
	nf = math.Min(nf, 16)
	r := rand.Int31n(int32(nf))
	d := time.Duration(r) * time.Second
	time.Sleep(d)
}
