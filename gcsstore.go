package cloudstorage

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path"
	"strings"
	"time"

	"github.com/lytics/cloudstorage/logging"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

const GCSFSStorageSource = "gcsFS"

var GCSRetries int = 55

//GcsFS Simple wrapper for accessing smaller GCS files, it doesn't currently implement a
// Reader/Writer interface so not useful for stream reading of large files yet.
type GcsFS struct {
	gcs       *storage.Client
	bucket    string
	cachepath string
	PageSize  int //TODO pipe this in from eventstore
	Id        string

	Log logging.Logger
}

func NewGCSStore(gcs *storage.Client, bucket, cachepath string, pagesize int, l logging.Logger) (*GcsFS, error) {
	err := os.MkdirAll(path.Dir(cachepath), 0775)
	if err != nil {
		return nil, fmt.Errorf("unable to create path. path=%s err=%v", cachepath, err)
	}

	uid := uuid.NewUUID().String()
	uid = strings.Replace(uid, "-", "", -1)

	return &GcsFS{
		gcs:       gcs,
		bucket:    bucket,
		cachepath: cachepath,
		Id:        uid,
		PageSize:  pagesize,
		Log:       l,
	}, nil
}

func (g *GcsFS) String() string {
	return fmt.Sprintf("gs://%s/", g.bucket)
}

func (g *GcsFS) gcsb() *storage.BucketHandle {
	return g.gcs.Bucket(g.bucket)
}

/*

removed as part of the effort to simply the interface

func (g *GcsFS) WriteObject(o string, meta map[string]string, b []byte) error {
	wc := storage.NewWriter(g.googlectx, g.bucket, o)

	if meta != nil {
		wc.Metadata = meta
		//contenttype is only used for viewing the file in a browser. (i.e. the GCS Object browser).
		ctype := ensureContextType(o, meta)
		wc.ContentType = ctype
	}

	if _, err := wc.Write(b); err != nil {
		g.Log.Printf("couldn't save object. %s err=%v", o, err)
		return err
	}

	if err := wc.Close(); err != nil {
		return err
	}

	return nil
}

*/

func (g *GcsFS) NewObject(objectname string) (Object, error) {
	obj, err := g.Get(objectname)
	if err != nil && err != ObjectNotFound {
		return nil, err
	} else if obj != nil {
		return nil, ObjectExists
	}

	cf := cachepathObj(g.cachepath, objectname, g.Id)

	return &gcsFSObject{
		name:       objectname,
		metadata:   map[string]string{ContextTypeKey: contentType(objectname)},
		gcsb:       g.gcsb(),
		bucket:     g.bucket,
		cachedcopy: nil,
		cachepath:  cf,
		log:        g.Log,
	}, nil
}

func (g *GcsFS) Get(objectpath string) (Object, error) {
	var q = &storage.Query{Prefix: objectpath, MaxResults: 1}

	gobjects, err := g.listObjects(q, GCSRetries)
	if err != nil {
		g.Log.Errorf("couldn't list objects. prefix=%s err=%v", q.Prefix, err)
		return nil, err
	}

	if gobjects == nil || len(gobjects.Results) == 0 {
		return nil, ObjectNotFound
	}

	gobj := gobjects.Results[0]
	res := &gcsFSObject{
		name:         gobj.Name,
		updated:      gobj.Updated,
		metadata:     gobj.Metadata,
		gcsb:         g.gcsb(),
		googleObject: gobj,
		bucket:       g.bucket,
		cachepath:    cachepathObj(g.cachepath, gobj.Name, g.Id),
		log:          g.Log,
	}
	return res, nil
}

func (g *GcsFS) List(query Query) (Objects, error) {

	var q = &storage.Query{Prefix: query.Prefix, MaxResults: g.PageSize}

	gobjects, err := g.listObjects(q, GCSRetries)
	if err != nil {
		g.Log.Errorf("couldn't list objects. prefix=%s err=%v", q.Prefix, err)
		return nil, err
	}

	if gobjects == nil {
		return make(Objects, 0), nil
	}

	if gobjects.Next != nil {
		q = gobjects.Next
		for q != nil {
			gobjectsB, err := g.listObjects(q, GCSRetries)
			if err != nil {
				g.Log.Errorf("couldn't list the remaining pages of objects. prefix=%s err=%v", q.Prefix, err)
				return nil, err
			}

			concatGCSObjects(gobjects, gobjectsB)

			if gobjectsB != nil {
				q = gobjectsB.Next
			} else {
				q = nil
			}
		}
	}

	res := make(Objects, 0)

	for _, gobj := range gobjects.Results {
		if len(gobj.Metadata) == 0 {
			gobj.Metadata = make(map[string]string)
		}
		if _, ok := gobj.Metadata["Content-Length"]; !ok {
			gobj.Metadata["Content-Length"] = fmt.Sprintf("%v", gobj.Size)
		}
		gobj.Metadata["md5"] = string(gobj.MD5)
		o := &gcsFSObject{
			name:      gobj.Name,
			updated:   gobj.Updated,
			metadata:  gobj.Metadata,
			gcsb:      g.gcsb(),
			bucket:    g.bucket,
			cachepath: cachepathObj(g.cachepath, gobj.Name, g.Id),
			log:       g.Log,
		}
		res = append(res, o)
	}

	res = query.applyFilters(res)

	return res, nil
}

//ListObjects is a wrapper around storeage.ListObjects, that retries on a GCS error.  GCS isn't a prefect system :p, and returns an error
//  about once every 2 weeks.
func (g *GcsFS) listObjects(q *storage.Query, retries int) (*storage.ObjectList, error) {
	var lasterr error = nil
	//GCS sometimes returns a 500 error, so we'll just retry...
	for i := 0; i < retries; i++ {
		objects, err := g.gcsb().List(context.Background(), q)
		if err != nil {
			g.Log.Errorf("error listing objects for the bucket. try:%d store:%s q.prefix:%v err:%v", i, g, q.Prefix, err)
			lasterr = err
			backoff(i)
			continue
		}
		return objects, nil
	}
	return nil, lasterr
}

func concatGCSObjects(a, b *storage.ObjectList) *storage.ObjectList {
	for _, obj := range b.Results {
		a.Results = append(a.Results, obj)
	}
	for _, prefix := range b.Prefixes {
		a.Prefixes = append(a.Prefixes, prefix)
	}
	return a
}

func (g *GcsFS) Delete(obj string) error {
	err := g.gcsb().Object(obj).Delete(context.Background())
	if err != nil {
		g.Log.Errorf("error deleting object. object=%s%s err=%v", g, obj, err)
		return err
	}
	return nil
}

type gcsFSObject struct {
	name         string
	updated      time.Time
	metadata     map[string]string
	googleObject *storage.ObjectAttrs

	gcsb   *storage.BucketHandle
	bucket string

	cachedcopy *os.File
	readonly   bool
	opened     bool

	cachepath string
	log       logging.Logger
}

func (o *gcsFSObject) StorageSource() string {
	return GCSFSStorageSource
}
func (o *gcsFSObject) Name() string {
	return o.name
}
func (o *gcsFSObject) String() string {
	return o.name
}
func (o *gcsFSObject) Updated() time.Time {
	return o.updated
}
func (o *gcsFSObject) MetaData() map[string]string {
	return o.metadata
}
func (o *gcsFSObject) SetMetaData(meta map[string]string) {
	o.metadata = meta
}

func (o *gcsFSObject) Open(accesslevel AccessLevel) (*os.File, error) {
	if o.opened {
		return nil, fmt.Errorf("the store object is already opened. %s", o.name)
	}

	var errs []error = make([]error, 0)
	var cachedcopy *os.File = nil
	var err error
	var readonly = accesslevel == ReadOnly

	err = os.MkdirAll(path.Dir(o.cachepath), 0775)
	if err != nil {
		return nil, fmt.Errorf("error occurred creating cachedcopy dir. cachepath=%s object=%s err=%v",
			o.cachepath, o.name, err)
	}

	err = ensureDir(o.cachepath)
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
			var q = &storage.Query{Prefix: o.name, MaxResults: 1}
			objects, err := o.gcsb.List(context.Background(), q)
			if err != nil {
				errs = append(errs, fmt.Errorf("error storage.NewReader err=%v", err))
				o.log.Debugf("%v", errs)
				backoff(try)
				continue
			}

			if objects.Results != nil && len(objects.Results) != 0 {
				o.googleObject = objects.Results[0]
			}
		}

		if o.googleObject != nil {
			//we have a preexisting object, so lets download it..
			rc, err := o.gcsb.Object(o.name).NewReader(context.Background())
			if err != nil {
				errs = append(errs, fmt.Errorf("error storage.NewReader err=%v", err))
				o.log.Debugf("%v", errs)
				backoff(try)
				continue
			}
			defer rc.Close()

			_, err = io.Copy(cachedcopy, rc)
			if err != nil {
				errs = append(errs, fmt.Errorf("error coping bytes. err=%v", err))
				o.log.Debugf("%v", errs)
				backoff(try)
				continue
			}
		}

		if readonly {
			cachedcopy.Close()
			cachedcopy, err = os.Open(o.cachepath)
			if err != nil {
				return nil, fmt.Errorf("error occurred opening file. local=%s object=%s tfile=%v err=%v",
					o.cachepath, o.name, cachedcopy.Name(), err)
			}
		}

		o.cachedcopy = cachedcopy
		o.readonly = readonly
		o.opened = true
		return o.cachedcopy, nil
	}

	return nil, fmt.Errorf("fetch error retry cnt reached: obj=%s tfile=%v errs:[%v]",
		o.name, o.cachepath, errs)
}

func (o *gcsFSObject) File() *os.File {
	return o.cachedcopy
}
func (o *gcsFSObject) Read(p []byte) (n int, err error) {
	return o.cachedcopy.Read(p)
}
func (o *gcsFSObject) Write(p []byte) (n int, err error) {
	return o.cachedcopy.Write(p)
}

func (o *gcsFSObject) Sync() error {

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
			ctype := ensureContextType(o.name, o.metadata)
			wc.ContentType = ctype
		}

		if _, err = io.Copy(wc, rd); err != nil {
			errs = append(errs, fmt.Sprintf("couldn't copy localcache file to remote object. object:%s err=%v", o.name, err))
			backoff(try)
			continue
		}

		if err = wc.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("couldn't close gcs writer. object:%s err=%v", o.name, err))
			backoff(try)
			continue
		}

		return nil
	}

	errmsg := strings.Join(errs, ",")

	return fmt.Errorf("unable to sync file: errors[%v]", errmsg)
}

func (o *gcsFSObject) Close() error {
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

func (o *gcsFSObject) Release() error {
	if o.cachedcopy != nil {
		o.cachedcopy.Close()
	}
	return os.Remove(o.cachepath)
}

//backoff sleeps a random amount so we can.
//retry failed requests using a randomized exponential backoff:
//wait a random period between [0..1] seconds and retry; if that fails,
//wait a random period between [0..2] seconds and retry; if that fails,
//wait a random period between [0..4] seconds and retry, and so on,
//with an upper bounds to the wait period being 16 seconds.
//http://play.golang.org/p/l9aUHgiR8J
func backoff(try int) {
	nf := math.Pow(2, float64(try))
	nf = math.Max(1, nf)
	nf = math.Min(nf, 16)
	r := rand.Int31n(int32(nf))
	d := time.Duration(r) * time.Second
	time.Sleep(d)
}
