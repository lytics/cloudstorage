package google

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/araddon/gou"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"

	"github.com/lytics/cloudstorage"
)

func init() {
	cloudstorage.Register(StoreType, provider)
}
func provider(conf *cloudstorage.Config) (cloudstorage.Store, error) {
	googleclient, err := NewGoogleClient(conf)
	if err != nil {
		return nil, err
	}
	return gcsCommonClient(googleclient.Client(), conf)
}

// StoreType = "gcs"
const StoreType = "gcs"

var (
	// GCSRetries number of times to retry for GCS.
	GCSRetries int = 55

	// Ensure we implement ObjectIterator
	_               cloudstorage.ObjectIterator = (*objectIterator)(nil)
	compressionMime                             = "gzip"
)

// GcsFS Simple wrapper for accessing smaller GCS files, it doesn't currently implement a
// Reader/Writer interface so not useful for stream reading of large files yet.
type GcsFS struct {
	gcs               *storage.Client
	bucket            string
	cachepath         string
	PageSize          int
	Id                string
	enableCompression bool
}

// NewGCSStore Create Google Cloud Storage Store.
func NewGCSStore(gcs *storage.Client, bucket, cachepath string, enableCompression bool, pagesize int) (*GcsFS, error) {
	err := os.MkdirAll(path.Dir(cachepath), 0775)
	if err != nil {
		return nil, fmt.Errorf("unable to create path. path=%s err=%v", cachepath, err)
	}

	uid := uuid.NewUUID().String()
	uid = strings.Replace(uid, "-", "", -1)

	return &GcsFS{
		gcs:               gcs,
		bucket:            bucket,
		cachepath:         cachepath,
		Id:                uid,
		PageSize:          pagesize,
		enableCompression: enableCompression,
	}, nil
}

// Type of store = "gcs"
func (g *GcsFS) Type() string {
	return StoreType
}

// Client gets access to the underlying google cloud storage client.
func (g *GcsFS) Client() interface{} {
	return g.gcs
}

// String function to provide gs://..../file   path
func (g *GcsFS) String() string {
	return fmt.Sprintf("gs://%s/", g.bucket)
}

func (g *GcsFS) gcsb() *storage.BucketHandle {
	return g.gcs.Bucket(g.bucket)
}

func (o *object) DisableCompression() {
	o.enableCompression = false
}

// NewObject of Type GCS.
func (g *GcsFS) NewObject(objectname string) (cloudstorage.Object, error) {
	obj, err := g.Get(context.Background(), objectname)
	if err != nil && err != cloudstorage.ErrObjectNotFound {
		return nil, err
	} else if obj != nil {
		return nil, cloudstorage.ErrObjectExists
	}

	cf := cloudstorage.CachePathObj(g.cachepath, objectname, g.Id)

	return &object{
		name:              objectname,
		metadata:          map[string]string{cloudstorage.ContentTypeKey: cloudstorage.ContentType(objectname)},
		gcsb:              g.gcsb(),
		bucket:            g.bucket,
		cachedcopy:        nil,
		cachepath:         cf,
		enableCompression: g.enableCompression,
	}, nil
}

// Get Gets a single File Object
func (g *GcsFS) Get(ctx context.Context, objectpath string) (cloudstorage.Object, error) {

	gobj, err := g.gcsb().Object(objectpath).Attrs(context.Background()) // .Objects(context.Background(), q)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") {
			return nil, cloudstorage.ErrObjectNotFound
		}
		return nil, err
	}

	if gobj == nil {
		return nil, cloudstorage.ErrObjectNotFound
	}

	return newObject(g, gobj), nil
}

// Objects returns an iterator over the objects in the google bucket that match the Query q.
// If q is nil, no filtering is done.
func (g *GcsFS) Objects(ctx context.Context, csq cloudstorage.Query) (cloudstorage.ObjectIterator, error) {
	var q = &storage.Query{Prefix: csq.Prefix}
	if csq.StartOffset != "" {
		q.StartOffset = csq.StartOffset
	}
	if csq.EndOffset != "" {
		q.EndOffset = csq.EndOffset
	}
	iter := g.gcsb().Objects(ctx, q)
	return &objectIterator{g, ctx, iter}, nil
}

// List returns an iterator over the objects in the google bucket that match the Query q.
// If q is nil, no filtering is done.
func (g *GcsFS) List(ctx context.Context, csq cloudstorage.Query) (*cloudstorage.ObjectsResponse, error) {
	iter, err := g.Objects(ctx, csq)
	if err != nil {
		return nil, err
	}
	return cloudstorage.ObjectResponseFromIter(iter)
}

// Folders get folders list.
func (g *GcsFS) Folders(ctx context.Context, csq cloudstorage.Query) ([]string, error) {
	var q = &storage.Query{Delimiter: csq.Delimiter, Prefix: csq.Prefix}
	iter := g.gcsb().Objects(ctx, q)
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

// Copy from src to destination
func (g *GcsFS) Copy(ctx context.Context, src, des cloudstorage.Object) error {

	srcgcs, ok := src.(*object)
	if !ok {
		return fmt.Errorf("Copy source file expected GCS but got %T", src)
	}
	desgcs, ok := des.(*object)
	if !ok {
		return fmt.Errorf("Copy destination expected GCS but got %T", des)
	}

	oh := srcgcs.gcsb.Object(srcgcs.name)
	dh := desgcs.gcsb.Object(desgcs.name)

	_, err := dh.CopierFrom(oh).Run(ctx)
	return err
}

// Move which is a Copy & Delete
func (g *GcsFS) Move(ctx context.Context, src, des cloudstorage.Object) error {

	srcgcs, ok := src.(*object)
	if !ok {
		return fmt.Errorf("Move source file expected GCS but got %T", src)
	}
	desgcs, ok := des.(*object)
	if !ok {
		return fmt.Errorf("Move destination expected GCS but got %T", des)
	}

	oh := srcgcs.gcsb.Object(srcgcs.name)
	dh := desgcs.gcsb.Object(desgcs.name)

	if _, err := dh.CopierFrom(oh).Run(ctx); err != nil {
		return err
	}

	return oh.Delete(ctx)
}

// NewReader create GCS file reader.
func (g *GcsFS) NewReader(o string) (io.ReadCloser, error) {
	return g.NewReaderWithContext(context.Background(), o)
}

// NewReaderWithContext create new GCS File reader with context.
func (g *GcsFS) NewReaderWithContext(ctx context.Context, o string) (io.ReadCloser, error) {
	obj := g.gcsb().Object(o).ReadCompressed(true)
	attrs, err := obj.Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		return nil, cloudstorage.ErrObjectNotFound
	} else if err != nil {
		return nil, err
	}
	// we check ContentType here because files uploaded compressed without an
	// explicit ContentType set get autodetected as "application/x-gzip" instead
	// of "application/octet-stream", but files with the gzip ContentType get
	// auto-decompressed regardless of your Accept-Encoding header
	if attrs.ContentEncoding == compressionMime && attrs.ContentType != "application/x-gzip" {
		rc, err := obj.NewReader(ctx)
		if err == storage.ErrObjectNotExist {
			return nil, cloudstorage.ErrObjectNotFound
		} else if err != nil {
			return nil, err
		}
		gr, err := gzip.NewReader(rc)
		if err != nil {
			return nil, err
		}
		return gr, err
	}

	rc, err := obj.NewReader(ctx)
	if err == storage.ErrObjectNotExist {
		return rc, cloudstorage.ErrObjectNotFound
	}
	return rc, err
}

// NewWriter create GCS Object Writer.
func (g *GcsFS) NewWriter(o string, metadata map[string]string) (io.WriteCloser, error) {
	return g.NewWriterWithContext(context.Background(), o, metadata)
}

type gzipWriteCloser struct {
	ctx context.Context
	w   io.WriteCloser
	c   io.Closer
}

// newGZIPWriteCloser is a io.WriteCloser that closes both the gzip writer and also the passed in writer
func newGZIPWriteCloser(ctx context.Context, rc io.WriteCloser) io.WriteCloser {
	return &gzipWriteCloser{ctx, gzip.NewWriter(rc), rc}
}

func (b *gzipWriteCloser) Write(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	return b.w.Write(p)
}

func (b *gzipWriteCloser) Close() error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if err := b.w.Close(); err != nil {
		return err
	}
	return b.c.Close()
}

// NewWriterWithContext create writer with provided context and metadata.
func (g *GcsFS) NewWriterWithContext(ctx context.Context, o string, metadata map[string]string, opts ...cloudstorage.Opts) (io.WriteCloser, error) {
	obj := g.gcsb().Object(o)
	disableCompression := false
	if len(opts) > 0 {
		if opts[0].DisableCompression {
			disableCompression = true
		}
		if opts[0].IfNotExists {
			obj = obj.If(storage.Conditions{DoesNotExist: true})
		}
	}
	wc := obj.NewWriter(ctx)
	if metadata != nil {
		wc.Metadata = metadata
		//contenttype is only used for viewing the file in a browser. (i.e. the GCS Object browser).
		ctype := cloudstorage.EnsureContextType(o, metadata)
		wc.ContentType = ctype
	}
	if g.enableCompression && !disableCompression {
		wc.ContentEncoding = compressionMime
		return newGZIPWriteCloser(ctx, wc), nil
	}
	return wc, nil
}

// Delete requested object path string.
func (g *GcsFS) Delete(ctx context.Context, obj string) error {
	err := g.gcsb().Object(obj).Delete(ctx)
	if err != nil {
		return err
	}
	return nil
}

// objectIterator iterator to match store interface for iterating
// through all GcsObjects that matched query.
type objectIterator struct {
	g    *GcsFS
	ctx  context.Context
	iter *storage.ObjectIterator
}

func (*objectIterator) Close() {}

// Next iterator to go to next object or else returns error for done.
func (it *objectIterator) Next() (cloudstorage.Object, error) {
	retryCt := 0
	for {
		select {
		case <-it.ctx.Done():
			// If has been closed
			return nil, it.ctx.Err()
		default:
			o, err := it.iter.Next()
			if err == nil {
				return newObject(it.g, o), nil
			} else if err == iterator.Done {
				return nil, err
			} else if err == context.Canceled || err == context.DeadlineExceeded {
				// Return to user
				return nil, err
			}
			if retryCt < 5 {
				cloudstorage.Backoff(retryCt)
			} else {
				return nil, err
			}
			retryCt++
		}
	}
}

type object struct {
	name              string
	updated           time.Time
	metadata          map[string]string
	googleObject      *storage.ObjectAttrs
	gcsb              *storage.BucketHandle
	bucket            string
	cachedcopy        *os.File
	readonly          bool
	opened            bool
	cachepath         string
	enableCompression bool
}

func newObject(g *GcsFS, o *storage.ObjectAttrs) *object {
	metadata := o.Metadata
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata["content_length"] = strconv.FormatInt(o.Size, 10)
	metadata["attrs_content_type"] = o.ContentType
	metadata["attrs_cache_control"] = o.CacheControl
	metadata["content_encoding"] = o.ContentEncoding

	return &object{
		name:              o.Name,
		updated:           o.Updated,
		metadata:          metadata,
		gcsb:              g.gcsb(),
		bucket:            g.bucket,
		cachepath:         cloudstorage.CachePathObj(g.cachepath, o.Name, g.Id),
		enableCompression: g.enableCompression,
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
	return o.gcsb.Object(o.name).Delete(context.Background())
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
					cloudstorage.Backoff(try)
					continue
				}
			}

			if gobj != nil {
				o.googleObject = gobj
			}
		}

		if o.googleObject != nil {
			//we have a preexisting object, so lets download it..
			rc, err := o.gcsb.Object(o.name).ReadCompressed(true).NewReader(context.Background())
			if err != nil {
				errs = append(errs, fmt.Errorf("error storage.NewReader err=%v", err))
				cloudstorage.Backoff(try)
				continue
			}
			defer rc.Close()

			if _, err := cachedcopy.Seek(0, io.SeekStart); err != nil {
				return nil, fmt.Errorf("error seeking to start of cachedcopy err=%v", err) // don't retry on local fs errors
			}

			var writtenBytes int64
			// we check ContentType here because files uploaded compressed without an
			// explicit ContentType set get autodetected as "application/x-gzip" instead
			// of "application/octet-stream", but files with the gzip ContentType get
			// auto-decompressed regardless of your Accept-Encoding header
			if o.googleObject.ContentEncoding == compressionMime && o.googleObject.ContentType != "application/x-gzip" {
				cr, err := gzip.NewReader(rc)
				if err != nil {
					return nil, fmt.Errorf("error decompressing data err=%v", err) // don't retry on decompression errors
				}
				writtenBytes, err = io.Copy(cachedcopy, cr)
				if err != nil && (strings.HasPrefix(err.Error(), "gzip: ")) {
					return nil, fmt.Errorf("error copying/decompressing data err=%v", err) // don't retry on decompression errors
				}
			} else {
				writtenBytes, err = io.Copy(cachedcopy, rc)
			}
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

			if o.googleObject.ContentEncoding != compressionMime { // compression checks crc
				// make sure the whole object was downloaded from google
				if contentLength, ok := o.metadata["content_length"]; ok {
					if contentLengthInt, err := strconv.ParseInt(contentLength, 10, 64); err == nil {
						if contentLengthInt != writtenBytes {
							return nil, fmt.Errorf("partial file download error. tfile=%v", o.name)
						}
					} else {
						return nil, fmt.Errorf("content_length is not a number. tfile=%v", o.name)
					}
				}
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
		} else {
			if _, err := cachedcopy.Seek(0, os.SEEK_SET); err != nil {
				return nil, fmt.Errorf("error seeking to start of cachedcopy err=%v", err) //don't retry on local fs errors
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
	if o.cachedcopy == nil {
		_, err := o.Open(cloudstorage.ReadWrite)
		if err != nil {
			return 0, err
		}
	}
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

		if o.enableCompression {
			wc.ContentEncoding = compressionMime
			cw := gzip.NewWriter(wc)
			if _, err = io.Copy(cw, rd); err != nil {
				errs = append(errs, fmt.Sprintf("copy to remote object error:%v", err))
				cloudstorage.Backoff(try)
				continue
			}

			if err = cw.Close(); err != nil {
				errs = append(errs, fmt.Sprintf("close compression writer error:%v", err))
				cloudstorage.Backoff(try)
				continue
			}

			if err = wc.Close(); err != nil {
				errs = append(errs, fmt.Sprintf("Close writer error:%v", err))
				cloudstorage.Backoff(try)
				continue
			}
		} else {
			if _, err = io.Copy(wc, rd); err != nil {
				errs = append(errs, fmt.Sprintf("copy to remote object error:%v", err))
				err2 := wc.CloseWithError(err)
				if err2 != nil {
					errs = append(errs, fmt.Sprintf("CloseWithError error:%v", err2))
				}
				cloudstorage.Backoff(try)
				continue
			}

			if err = wc.Close(); err != nil {
				errs = append(errs, fmt.Sprintf("close gcs writer error:%v", err))
				cloudstorage.Backoff(try)
				continue
			}
		}

		return nil
	}

	errmsg := strings.Join(errs, ",")
	return fmt.Errorf("GCS sync error after retry: (oname=%s cpath:%v) errors[%v]", o.name, o.cachepath, errmsg)
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

	if !o.readonly {
		err := o.cachedcopy.Sync()
		if err != nil {
			return err
		}
	}

	err := o.cachedcopy.Close()
	if err != nil {
		if !strings.Contains(err.Error(), "already closed") {
			gou.Warnf("error closing cached copy %v", err)
			return fmt.Errorf("error on sync and closing localfile. %q err=%v", o.cachepath, err)
		}
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
		gou.Debugf("release %q vs %q", o.cachedcopy.Name(), o.cachepath)
		o.cachedcopy.Close()
		o.cachedcopy = nil
		o.opened = false
		return os.Remove(o.cachepath)
	}
	// most likely this doesn't exist so don't return error
	os.Remove(o.cachepath)
	return nil
}
