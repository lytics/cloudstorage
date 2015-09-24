package cloudstorage

import (
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

const GCSFSStorageSource = "gcsFS"

var GCSRetries int = 55

//GcsFS Simple wrapper for accessing smaller GCS files, it doesn't currently implement a
// Reader/Writer interface so not useful for stream reading of large files yet.
type GcsFS struct {
	googlectx context.Context
	bucket    string
	cachepath string
	PageSize  int //TODO pipe this in from eventstore
	Id        string

	Log *log.Logger
}

func NewGCSStore(gctx context.Context, bucket, cachepath string, pagesize int, l *log.Logger) *GcsFS {
	err := os.MkdirAll(path.Dir(cachepath), 0775)
	if err != nil {
		l.Printf("unable to create path. path=%s err=%v", cachepath, err)
	}

	uid := uuid.NewUUID().String()
	uid = strings.Replace(uid, "-", "", -1)

	return &GcsFS{
		googlectx: gctx,
		bucket:    bucket,
		cachepath: cachepath,
		Id:        uid,
		PageSize:  pagesize,
		Log:       l,
	}
}

func (g *GcsFS) String() string {
	return fmt.Sprintf("gs://%s/", g.bucket)
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

	wc.ACL = []storage.ACLRule{{storage.AllAuthenticatedUsers, storage.RoleReader}}

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

func (g *GcsFS) NewObject(name string) (Object, error) {
	return &gcsFSObject{
		name:       name,
		metadata:   map[string]string{ContextTypeKey: contentType(name)},
		googlectx:  g.googlectx,
		bucket:     g.bucket,
		cachedcopy: nil,
		cachepath:  cachepathObj(g.cachepath, name, g.Id),
		log:        g.Log,
	}, nil
}

func (g *GcsFS) Get(o string) (Object, error) {
	var q = &storage.Query{Prefix: o, MaxResults: g.PageSize}

	gobjects, err := g.listObjects(q, GCSRetries)
	if err != nil {
		g.Log.Printf("couldn't list objects. prefix=%s err=%v", q.Prefix, err)
		return nil, err
	}

	if gobjects == nil || len(gobjects.Results) == 0 {
		return nil, ObjectNotFound
	}

	gobj := gobjects.Results[0]
	res := &gcsFSObject{
		name:      gobj.Name,
		metadata:  gobj.Metadata,
		googlectx: g.googlectx,
		bucket:    g.bucket,
		cachepath: cachepathObj(g.cachepath, gobj.Name, g.Id),
		log:       g.Log,
	}
	return res, nil
}

func (g *GcsFS) List(query Query) (Objects, error) {

	var q = &storage.Query{Prefix: query.Prefix, MaxResults: g.PageSize}

	gobjects, err := g.listObjects(q, GCSRetries)
	if err != nil {
		g.Log.Printf("couldn't list objects. prefix=%s err=%v", q.Prefix, err)
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
				g.Log.Printf("couldn't list the remaining pages of objects. prefix=%s err=%v", q.Prefix, err)
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
		o := &gcsFSObject{
			name:      gobj.Name,
			metadata:  gobj.Metadata,
			googlectx: g.googlectx,
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
func (g *GcsFS) listObjects(q *storage.Query, retries int) (*storage.Objects, error) {
	var lasterr error = nil
	//GCS sometimes returns a 500 error, so we'll just retry...
	for i := 0; i < retries; i++ {
		objects, err := storage.ListObjects(g.googlectx, g.bucket, q)
		if err != nil {
			g.Log.Printf("error listing objects for the bucket. try:%d store:%s q.prefix:%v err:%v", i, g, q.Prefix, err)
			lasterr = err
			backoff(i)
			continue
		}
		return objects, nil
	}
	return nil, lasterr
}

func concatGCSObjects(a, b *storage.Objects) *storage.Objects {
	for _, obj := range b.Results {
		a.Results = append(a.Results, obj)
	}
	for _, prefix := range b.Prefixes {
		a.Prefixes = append(a.Prefixes, prefix)
	}
	return a
}

func (g *GcsFS) Delete(obj string) error {
	err := storage.DeleteObject(g.googlectx, g.bucket, obj)
	if err != nil {
		g.Log.Printf("error deleting object. object=%s%s err=%v", g, obj, err)
		return err
	}
	return nil
}

type gcsFSObject struct {
	name     string
	metadata map[string]string
	//GoogleObject *storage.Object

	googlectx context.Context
	bucket    string

	cachedcopy *os.File
	readonly   bool
	opened     bool

	cachepath string
	log       *log.Logger
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
func (o *gcsFSObject) MetaData() map[string]string {
	return o.metadata
}
func (o *gcsFSObject) SetMetaData(meta map[string]string) {
	o.metadata = meta
}

func (o *gcsFSObject) Open(readonly bool) error {
	if o.opened {
		return fmt.Errorf("the store object is already opened. %s", o.name)
	}

	var errs []error = make([]error, 0)
	var cachedcopy *os.File = nil
	var err error

	err = os.MkdirAll(path.Dir(o.cachepath), 0775)
	if err != nil {
		return fmt.Errorf("gcsfs: error occurred creating cachedcopy dir. cachepath=%s object=%s err=%v",
			o.cachepath, o.name, err)
	}

	for try := 0; try < GCSRetries; try++ {
		cachedcopy, err = os.Create(o.cachepath)
		if err != nil {
			return fmt.Errorf("gcsfs: error occurred creating file. local=%s err=%v",
				o.cachepath, err)
		}

		rc, err := storage.NewReader(o.googlectx, o.bucket, o.name)
		if err != nil {
			errs = append(errs, fmt.Errorf("error storage.NewReader err=%v", err))
			o.log.Printf("%v", errs)
			backoff(try)
			continue
		}
		defer rc.Close()

		_, err = io.Copy(cachedcopy, rc)
		if err != nil {
			errs = append(errs, fmt.Errorf("error coping bytes. err=%v", err))
			o.log.Printf("%v", errs)
			backoff(try)
			continue
		}

		if readonly {
			cachedcopy.Close()
			cachedcopy, err = os.Open(o.cachepath)
			if err != nil {
				return fmt.Errorf("gcsfs: error occurred open file. local=%s object=%s tfile=%v err=%v",
					o.cachepath, o.name, cachedcopy.Name(), err)
			}
		}

		o.cachedcopy = cachedcopy
		o.readonly = readonly
		o.opened = true
		return nil
	}

	return fmt.Errorf("gcsfs: fetch error retry cnt reached: obj=%s tfile=%v errs:[%v]",
		o.name, o.cachepath, errs)
}

func (o *gcsFSObject) CachedCopy() *os.File {
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
		return fmt.Errorf("object isn't opened %s", o.name)
	}
	if o.readonly {
		return fmt.Errorf("trying to Sync a readonly object %s", o.name)
	}

	cachedcopy, err := os.OpenFile(o.cachepath, os.O_WRONLY, 0664)
	if err != nil {
		return fmt.Errorf("gcsfs: couldn't open localfile for sync'ing. local=%s err=%v",
			o.cachepath, err)
	}
	defer cachedcopy.Close()

	wc := storage.NewWriter(o.googlectx, o.bucket, o.name)

	wc.ACL = []storage.ACLRule{{storage.AllAuthenticatedUsers, storage.RoleReader}}

	if o.metadata != nil {
		wc.Metadata = o.metadata
		//contenttype is only used for viewing the file in a browser. (i.e. the GCS Object browser).
		ctype := ensureContextType(o.name, o.metadata)
		wc.ContentType = ctype
	}

	if _, err = io.Copy(wc, cachedcopy); err != nil {
		return fmt.Errorf("gcsfs: couldn't copy object. %s err=%v", o.name, err)
	}

	if err = wc.Close(); err != nil {
		return fmt.Errorf("gcsfs: couldn't close gcs writer. %s err=%v", o.name, err)
	}

	return nil
}

func (o *gcsFSObject) Close() error {
	if !o.opened {
		return nil
	}

	err := o.cachedcopy.Sync()
	if err != nil {
		return err
	}

	err = o.cachedcopy.Close()
	if err != nil {
		return fmt.Errorf("gcsfs: error on close localfile. %s err=%v", o.cachepath, err)
	}

	if o.opened && !o.readonly {
		err := o.Sync()
		if err != nil {
			return err
		}
	}

	o.cachedcopy = nil
	o.opened = false

	return nil
}

func (o *gcsFSObject) Release() error {
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
