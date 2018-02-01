package awss3

import (
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	u "github.com/araddon/gou"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"

	"github.com/lytics/cloudstorage"
)

const (
	// StoreType = "s3" this is used to define the storage type to create
	// from cloudstorage.NewStore(config)
	StoreType = "s3"

	// Configuration Keys.  These are the names of keys
	// to look for in the json map[string]string to extract for config.

	// ConfKeyAccessKey config key name of the aws access_key(id) for auth
	ConfKeyAccessKey = "access_key"
	// ConfKeyAccessSecret config key name of the aws acccess secret
	ConfKeyAccessSecret = "access_secret"
	// ConfKeyARN config key name of the aws ARN name of user
	ConfKeyARN = "arn"
	// ConfKeyDisableSSL config key name of disabling ssl flag
	ConfKeyDisableSSL = "disable_ssl"
	// Authentication Source's

	// AuthAccessKey is for using aws access key/secret pairs
	AuthAccessKey cloudstorage.AuthMethod = "aws_access_key"
)

var (
	// Retries number of times to retry upon failures.
	Retries = 55
	// PageSize is default page size
	PageSize = 2000

	// ErrNoS3Session no valid session
	ErrNoS3Session = fmt.Errorf("no valid aws session was created")
	// ErrNoAccessKey error for no access_key
	ErrNoAccessKey = fmt.Errorf("no settings.access_key")
	// ErrNoAccessSecret error for no settings.access_secret
	ErrNoAccessSecret = fmt.Errorf("no settings.access_secret")
	// ErrNoAuth error for no findable auth
	ErrNoAuth = fmt.Errorf("No auth provided")
)

func init() {
	// Register this Driver (s3) in cloudstorage driver registry.
	cloudstorage.Register(StoreType, func(conf *cloudstorage.Config) (cloudstorage.Store, error) {
		client, sess, err := NewClient(conf)
		if err != nil {
			return nil, err
		}
		return NewStore(client, sess, conf)
	})
}

type (
	// FS Simple wrapper for accessing s3 files, it doesn't currently implement a
	// Reader/Writer interface so not useful for stream reading of large files yet.
	FS struct {
		PageSize  int
		ID        string
		client    *s3.S3
		sess      *session.Session
		endpoint  string
		bucket    string
		cachepath string
	}

	object struct {
		fs         *FS
		o          *s3.GetObjectOutput
		cachedcopy *os.File

		name      string    // aka "key" in s3
		updated   time.Time // LastModifyied in s3
		metadata  map[string]string
		bucket    string
		readonly  bool
		opened    bool
		cachepath string

		infoOnce sync.Once
		infoErr  error
	}
)

// NewClient create new AWS s3 Client.  Uses cloudstorage.Config to read
// necessary config settings such as bucket, region, auth.
func NewClient(conf *cloudstorage.Config) (*s3.S3, *session.Session, error) {

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
			return nil, nil, ErrNoAccessKey
		}
		secretKey := conf.Settings.String(ConfKeyAccessSecret)
		if secretKey == "" {
			return nil, nil, ErrNoAccessSecret
		}
		awsConf.WithCredentials(credentials.NewStaticCredentials(accessKey, secretKey, ""))
	default:
		return nil, nil, ErrNoAuth
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
		u.Errorf("no session")
		return nil, nil, ErrNoS3Session
	}

	s3Client := s3.New(sess)

	return s3Client, sess, nil
}

// NewStore Create AWS S3 storage client of type cloudstorage.Store
func NewStore(c *s3.S3, sess *session.Session, conf *cloudstorage.Config) (*FS, error) {

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
		sess:      sess,
		bucket:    conf.Bucket,
		cachepath: conf.TmpDir,
		ID:        uid,
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

// NewObject of Type s3.
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

	res, err := f.client.GetObjectWithContext(ctx, &s3.GetObjectInput{
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
	// TODO:  this is messed up, we need to leave it open for some?
	// should we not even have a helper function?
	res.Body.Close()

	return newObjectFromResponse(f, objectname, res), nil
}

func (f *FS) getS3OpenObject(ctx context.Context, objectname string) (*s3.GetObjectOutput, error) {

	if f == nil {
		u.WarnT(10)
	}
	//u.Infof("%T get object bucket=%q object=%q", f, f.bucket, objectname)
	res, err := f.client.GetObjectWithContext(ctx, &s3.GetObjectInput{
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
	// NOTE:  caller must close
	//res.Body.Close()
	//u.Infof("result %#v", res)

	return res, nil
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

	itemLimit := int64(f.PageSize)
	if q.PageSize > 0 {
		itemLimit = int64(q.PageSize)
	}

	//u.Infof("List bucket=%q marker=%q prefix=%q pageSize=%d", f.bucket, q.Marker, q.Prefix, itemLimit)

	params := &s3.ListObjectsInput{
		Bucket:  aws.String(f.bucket),
		Marker:  &q.Marker,
		MaxKeys: &itemLimit,
		Prefix:  &q.Prefix,
	}

	resp, err := f.client.ListObjects(params)
	if err != nil {
		u.Warnf("err = %v", err)
		return nil, err
	}
	//u.Debugf("got contents %v", len(resp.Contents))

	objResp := &cloudstorage.ObjectsResponse{
		Objects: make(cloudstorage.Objects, len(resp.Contents)),
	}

	for i, o := range resp.Contents {
		objResp.Objects[i] = newObject(f, o)
	}

	if *resp.IsTruncated {
		q.Marker = *resp.Contents[len(resp.Contents)-1].Key
	}

	return objResp, nil
}

// Objects returns an iterator over the objects in the google bucket that match the Query q.
// If q is nil, no filtering is done.
func (f *FS) Objects(ctx context.Context, q cloudstorage.Query) (cloudstorage.ObjectIterator, error) {
	return cloudstorage.NewObjectPageIterator(ctx, f, q), nil
}

// Folders get folders list.
func (f *FS) Folders(ctx context.Context, q cloudstorage.Query) ([]string, error) {

	//q.Prefix = "/"
	q.Delimiter = "/"

	itemLimit := int64(f.PageSize)
	if q.PageSize > 0 {
		itemLimit = int64(q.PageSize)
	}
	//itemLimit := int64(0)

	params := &s3.ListObjectsInput{
		Bucket:    aws.String(f.bucket),
		MaxKeys:   &itemLimit,
		Prefix:    &q.Prefix,
		Delimiter: &q.Delimiter,
	}

	//u.Debugf("folders %+v  %+v", params, q)

	folders := make([]string, 0)

	for {
		select {
		case <-ctx.Done():
			// If has been closed
			u.Warnf("exit because done from folders")
			return folders, ctx.Err()
		default:
			if q.Marker != "" {
				params.Marker = &q.Marker
			}
			resp, err := f.client.ListObjectsWithContext(ctx, params)
			//u.Infof("resp: %#v err=%v", resp, err)
			//u.Infof("common: %v", resp.CommonPrefixes)
			if err != nil {
				return nil, err
			}
			for _, cp := range resp.CommonPrefixes {
				folders = append(folders, strings.Trim(*cp.Prefix, `/`))
			}
			return folders, nil
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

// NewReaderWithContext create new File reader with context.
func (f *FS) NewReaderWithContext(ctx context.Context, objectname string) (io.ReadCloser, error) {
	res, err := f.client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Key:    aws.String(objectname),
		Bucket: aws.String(f.bucket),
	})
	if err != nil {
		// translate the string error to typed error
		u.Errorf("erro   %q", err.Error())
		if strings.Contains(err.Error(), "NoSuchKey") {
			return nil, cloudstorage.ErrObjectNotFound
		}
		return nil, err
	}
	return res.Body, nil
}

// NewWriter create Object Writer.
func (f *FS) NewWriter(objectName string, metadata map[string]string) (io.WriteCloser, error) {
	return f.NewWriterWithContext(context.Background(), objectName, metadata)
}

// NewWriterWithContext create writer with provided context and metadata.
func (f *FS) NewWriterWithContext(ctx context.Context, objectName string, metadata map[string]string) (io.WriteCloser, error) {

	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(f.sess)

	pr, pw := io.Pipe()

	go func() {
		// TODO:  this needs to be managed, ie shutdown signals, close, handler err etc.

		// Upload the file to S3.
		_, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(objectName),
			Body:   pr,
		})
		if err != nil {
			u.Warnf("could not upload %v", err)
		}
	}()

	return pw, nil
}

// Delete requested object path string.
func (f *FS) Delete(obj string) error {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(obj),
	}

	_, err := f.client.DeleteObject(params)
	if err != nil {
		return err
	}
	return nil
}

func newObject(f *FS, o *s3.Object) *object {
	obj := &object{
		fs:        f,
		name:      *o.Key,
		bucket:    f.bucket,
		cachepath: cloudstorage.CachePathObj(f.cachepath, *o.Key, f.ID),
	}
	if o.LastModified != nil {
		obj.updated = *o.LastModified
	}
	return obj
}
func newObjectFromResponse(f *FS, name string, o *s3.GetObjectOutput) *object {
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
	//u.Infof("newobj name=%q etag=%s  updated:%v", name, *o.ETag, obj.updated)
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
	return o.fs.Delete(o.name)
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
		if o.o == nil {
			obj, err := o.fs.getS3OpenObject(context.Background(), o.name)
			if err != nil {
				if err == cloudstorage.ErrObjectNotFound {
					// New, this is fine
				} else {
					// lets re-try
					errs = append(errs, fmt.Errorf("error getting object err=%v", err))
					backoff(try)
					continue
				}
			}

			if obj != nil {
				o.o = obj
			}
		}

		if o.o != nil {
			// we have a preexisting object, so lets download it..
			defer o.o.Body.Close()

			if _, err := cachedcopy.Seek(0, os.SEEK_SET); err != nil {
				return nil, fmt.Errorf("error seeking to start of cachedcopy err=%v", err) //don't retry on local fs errors
			}

			_, err = io.Copy(cachedcopy, o.o.Body)
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

/*
func (o *object) Sync() error {
	u.Warnf("Could not sync")
	u.WarnT(12)
	return cloudstorage.ErrNotImplemented
}
*/

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

	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(o.fs.sess)

	if _, err := cachedcopy.Seek(0, os.SEEK_SET); err != nil {
		return fmt.Errorf("error seeking to start of cachedcopy err=%v", err) //don't retry on local filesystem errors
	}

	// Upload the file to S3.
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(o.fs.bucket),
		Key:    aws.String(o.name),
		Body:   cachedcopy,
	})
	if err != nil {
		u.Warnf("could not upload %v", err)
		return fmt.Errorf("failed to upload file, %v", err)
	}
	//u.Debugf("object.Close() upload result: %#v", result)

	// if o.metadata != nil {
	// 	wc.Metadata = o.metadata
	// 	// contenttype is only used for viewing the file in a browser. (i.e. the GCS Object browser).
	// 	ctype := cloudstorage.EnsureContextType(o.name, o.metadata)
	// 	wc.ContentType = ctype
	// }

	// if err = wc.Close(); err != nil {
	// 	errs = append(errs, fmt.Sprintf("close gcs writer error:%v", err))
	// 	backoff(try)
	// 	continue
	// }
	return nil
}

func (o *object) Close() error {
	if !o.opened {
		u.Warnf("returning because not closed")
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
		u.Warnf("err ")
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
