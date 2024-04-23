package localfs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/araddon/gou"
	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/csbufio"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

func init() {
	cloudstorage.Register(StoreType, localProvider)
}
func localProvider(conf *cloudstorage.Config) (cloudstorage.Store, error) {
	store, err := NewLocalStore(conf.Bucket, conf.LocalFS, conf.TmpDir)
	if err != nil {
		return nil, err
	}
	return store, nil
}

var (
	// Ensure Our LocalStore implement CloudStorage interfaces
	_ cloudstorage.StoreReader = (*LocalStore)(nil)
)

const (
	// AuthFileSystem Authentication Method
	AuthFileSystem cloudstorage.AuthMethod = "localfiles"

	// StoreType name of our Local Storage provider = "localfs"
	StoreType = "localfs"
)

// LocalStore is client to local-filesystem store.
type LocalStore struct {
	storepath string // possibly is relative  ./tables
	cachepath string
	Id        string
}

// NewLocalStore create local store from storage path on local filesystem, and cachepath.
func NewLocalStore(bucket, storepath, cachepath string) (*LocalStore, error) {

	if storepath == "" {
		return nil, fmt.Errorf("storepath=%q cannot be empty", storepath)
	}

	if storepath == cachepath {
		return nil, fmt.Errorf("storepath=%q cannot be the same as cachepath=%q", storepath, cachepath)
	}

	storepath = filepath.Join(storepath, bucket)

	err := os.MkdirAll(storepath, 0775)
	if err != nil {
		return nil, fmt.Errorf("unable to create path. path=%s err=%v", storepath, err)
	}

	err = os.MkdirAll(cachepath, 0775)
	if err != nil {
		return nil, fmt.Errorf("unable to create path. path=%s err=%v", cachepath, err)
	}

	uid := uuid.NewUUID().String()
	uid = strings.Replace(uid, "-", "", -1)

	return &LocalStore{
		storepath: storepath,
		cachepath: cachepath,
		Id:        uid,
	}, nil
}

// Type is store type = "localfs"
func (l *LocalStore) Type() string {
	return StoreType
}
func (l *LocalStore) Client() interface{} {
	return l
}

// NewObject create new object of given name.
func (l *LocalStore) NewObject(objectname string) (cloudstorage.Object, error) {
	obj, err := l.Get(context.Background(), objectname)
	if err != nil && err != cloudstorage.ErrObjectNotFound {
		return nil, err
	} else if obj != nil {
		return nil, cloudstorage.ErrObjectExists
	}

	of := path.Join(l.storepath, objectname)
	err = cloudstorage.EnsureDir(of)
	if err != nil {
		return nil, err
	}

	cf := cloudstorage.CachePathObj(l.cachepath, objectname, l.Id)

	metadata, err := readmeta(of + ".metadata")
	if err != nil {
		return nil, err
	}

	return &object{
		name:      objectname,
		storepath: of,
		cachepath: cf,
		metadata:  metadata,
	}, nil
}

// List objects at Query location.
func (l *LocalStore) List(ctx context.Context, query cloudstorage.Query) (*cloudstorage.ObjectsResponse, error) {
	resp := cloudstorage.NewObjectsResponse()
	objects := make(map[string]*object)
	metadatas := make(map[string]map[string]string)

	spath := l.storepath
	filePre := query.Prefix
	li := strings.LastIndex(query.Prefix, "/")
	if li > 0 {
		spath = path.Join(spath, query.Prefix[:li])
	}
	if !cloudstorage.Exists(spath) {
		return resp, nil
	}

	err := filepath.Walk(spath, func(fo string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		obj := strings.Replace(fo, l.storepath, "", 1)

		if f.IsDir() {
			return nil
		} else if filepath.Ext(f.Name()) == ".metadata" {
			metadata, err := readmeta(f.Name())
			if err != nil {
				return err
			}
			mdkey := strings.Replace(obj, ".metadata", "", 1)
			metadatas[mdkey] = metadata
		} else {
			oname := strings.TrimPrefix(obj, "/")
			if filePre != "" && !strings.HasPrefix(oname, filePre) {
				return nil
			}

			if (query.StartOffset != "" && oname < query.StartOffset) ||
				(query.EndOffset != "" && oname >= query.EndOffset) {
				return nil
			}

			objects[obj] = &object{
				name:      oname,
				updated:   f.ModTime(),
				storepath: fo,
				cachepath: cloudstorage.CachePathObj(l.cachepath, oname, l.Id),
			}
		}
		return err
	})

	if err != nil {
		return nil, fmt.Errorf("localfile: error occurred listing files. searchpath=%v err=%v", spath, err)
	}

	for objname, obj := range objects {
		if md, ok := metadatas[objname]; ok {
			obj.metadata = md
		}
		resp.Objects = append(resp.Objects, obj)
	}

	resp.Objects = query.ApplyFilters(resp.Objects)

	return resp, nil
}

// Objects returns an iterator over the objects in the local folder that match the Query q.
// If q is nil, no filtering is done.
func (l *LocalStore) Objects(ctx context.Context, csq cloudstorage.Query) (cloudstorage.ObjectIterator, error) {
	resp, err := l.List(ctx, csq)
	if err != nil {
		return nil, err
	}
	return &objectIterator{objects: resp.Objects}, nil
}

// Folders list of folders for given path query.
func (l *LocalStore) Folders(ctx context.Context, csq cloudstorage.Query) ([]string, error) {
	spath := path.Join(l.storepath, csq.Prefix)
	if !cloudstorage.Exists(spath) {
		return []string{}, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	folders := make([]string, 0)
	files, _ := os.ReadDir(spath)
	for _, f := range files {
		if f.IsDir() {
			folders = append(folders, fmt.Sprintf("%s/", path.Join(csq.Prefix, f.Name())))
		}
	}
	return folders, nil
}

// NewReader create local file-system store reader.
func (l *LocalStore) NewReader(o string) (io.ReadCloser, error) {
	return l.NewReaderWithContext(context.Background(), o)
}
func (l *LocalStore) pathForObject(o string) (string, error) {
	fo := path.Join(l.storepath, o)
	if !cloudstorage.Exists(fo) {
		return "", cloudstorage.ErrObjectNotFound
	}
	stat, err := os.Stat(fo)
	if err != nil {
		return "", err
	}
	if stat.IsDir() {
		return "", cloudstorage.ErrObjectNotFound
	}
	return fo, nil
}

func (l *LocalStore) NewReaderWithContext(ctx context.Context, o string) (io.ReadCloser, error) {
	fo, err := l.pathForObject(o)
	if err != nil {
		return nil, err
	}
	return csbufio.OpenReader(ctx, fo)
}

func (l *LocalStore) NewWriter(o string, metadata map[string]string) (io.WriteCloser, error) {
	return l.NewWriterWithContext(context.Background(), o, metadata)
}
func (l *LocalStore) NewWriterWithContext(ctx context.Context, o string, metadata map[string]string, opts ...cloudstorage.Opts) (io.WriteCloser, error) {
	fo := path.Join(l.storepath, o)

	err := cloudstorage.EnsureDir(fo)
	if err != nil {
		return nil, err
	}

	if len(metadata) == 0 {
		metadata = make(map[string]string)
	}

	fmd := fo + ".metadata"
	if err := writemeta(fmd, metadata); err != nil {
		return nil, err
	}

	flag := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if len(opts) > 0 && opts[0].IfNotExists {
		flag = flag | os.O_EXCL
	}
	f, err := os.OpenFile(fo, flag, 0665)
	if err != nil {
		return nil, err
	}

	return csbufio.NewWriter(ctx, f), nil
}

func (l *LocalStore) Get(ctx context.Context, o string) (cloudstorage.Object, error) {
	fo, err := l.pathForObject(o)
	if err != nil {
		return nil, err
	}

	var updated time.Time
	if stat, err := os.Stat(fo); err == nil {
		updated = stat.ModTime()
	}

	metadata, err := readmeta(fo + ".metadata")
	if err != nil {
		return nil, err
	}

	return &object{
		name:      o,
		updated:   updated,
		storepath: fo,
		metadata:  metadata,
		cachepath: cloudstorage.CachePathObj(l.cachepath, o, l.Id),
	}, nil
}

// Delete the object from underlying store.
func (l *LocalStore) Delete(ctx context.Context, obj string) error {
	fo := path.Join(l.storepath, obj)
	if err := os.Remove(fo); err != nil {
		return fmt.Errorf("removing file=%s: %w", fo, err)
	}
	mf := fo + ".metadata"
	if cloudstorage.Exists(mf) {
		if err := os.Remove(mf); err != nil {
			return fmt.Errorf("removing file=%s: %w", mf, err)
		}
	}

	// When the last item in a folder is deleted, the folder
	// should also be deleted. This matches the behavior in GCS.
	return l.deleteParentDirs(fo)
}

// deleteParentDirs deletes all the parent dirs of some filepath
// if those dirs are empty.
func (l *LocalStore) deleteParentDirs(filePath string) error {

	for dirName := path.Dir(filePath); len(dirName) > 0; dirName = path.Dir(dirName) {
		if dirName == l.storepath {
			// top level, stop deleting
			return nil
		}
		err := os.Remove(dirName)
		if errors.Is(err, os.ErrNotExist) {
			// it's already deleted; nothing to do.
			return nil
		}
		// There is no equivalent os.ErrNotEmpty in this version of go.
		var perr *fs.PathError
		if ok := errors.As(err, &perr); ok {
			if sysErr, ok := perr.Err.(syscall.Errno); ok && sysErr == syscall.ENOTEMPTY {
				// not empty; quit.
				return nil
			}
		}
		// unknown error, return it.
		if err != nil {
			return fmt.Errorf("failed to remove store dir=%s err=%w", dirName, err)
		}
		// we deleted an empty folder, so continue
	}
	return nil
}

func (l *LocalStore) String() string {
	return fmt.Sprintf("[id:%s file://%s/]", l.Id, l.storepath)
}

type objectIterator struct {
	objects cloudstorage.Objects
	err     error
	cursor  int
}

func (l *objectIterator) Next() (cloudstorage.Object, error) {
	if l.err != nil {
		return nil, l.err
	}
	if l.cursor >= len(l.objects) {
		return nil, iterator.Done
	}
	o := l.objects[l.cursor]
	l.cursor++
	return o, nil
}
func (l *objectIterator) Close() {}

type object struct {
	name     string
	updated  time.Time
	metadata map[string]string

	storepath string
	cachepath string

	cachedcopy *os.File
	readonly   bool
	opened     bool
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
	if err := o.Release(); err != nil {
		gou.Errorf("could not release %v", err)
	}
	if err := os.Remove(o.storepath); err != nil {
		return err
	}
	mf := o.storepath + ".metadata"
	if cloudstorage.Exists(mf) {
		if err := os.Remove(mf); err != nil {
			return err
		}
	}
	return nil
}

func (o *object) Open(accesslevel cloudstorage.AccessLevel) (*os.File, error) {
	if o.opened {
		return nil, fmt.Errorf("the store object is already opened. %s", o.storepath)
	}

	var readonly = accesslevel == cloudstorage.ReadOnly

	storecopy, err := os.OpenFile(o.storepath, os.O_RDWR|os.O_CREATE, 0665)
	if err != nil {
		return nil, fmt.Errorf("localfs: local=%q could not create storecopy err=%v", o.storepath, err)
	}
	defer storecopy.Close()

	err = cloudstorage.EnsureDir(o.cachepath)
	if err != nil {
		return nil, fmt.Errorf("localfs: cachepath=%s could not create cachedcopy dir err=%v", o.cachepath, err)
	}

	cachedcopy, err := os.Create(o.cachepath)
	if err != nil {
		return nil, fmt.Errorf("localfs: cachepath=%s could not create cachedcopy err=%v", o.cachepath, err)
	}

	_, err = io.Copy(cachedcopy, storecopy)
	if err != nil {
		return nil, fmt.Errorf("localfs: storepath=%s cachedcopy=%v could not copy from store to cache err=%v", o.storepath, cachedcopy.Name(), err)
	}

	if readonly {
		cachedcopy.Close()
		cachedcopy, err = os.Open(o.cachepath)
		if err != nil {
			return nil, fmt.Errorf("localfs: storepath=%s cachedcopy=%v could not opencache err=%v", o.storepath, cachedcopy.Name(), err)
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

func (o *object) File() *os.File {
	return o.cachedcopy
}
func (o *object) Read(p []byte) (n int, err error) {
	return o.cachedcopy.Read(p)
}

// Write the given bytes to object.  Won't be writen until Close() or Sync() called.
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
		return fmt.Errorf("object isn't opened %s", o.name)
	}
	if o.readonly {
		return fmt.Errorf("trying to Sync a readonly object %s", o.name)
	}

	cachedcopy, err := os.OpenFile(o.cachepath, os.O_RDONLY, 0664)
	if err != nil {
		return err
	}
	defer cachedcopy.Close()

	storecopy, err := os.OpenFile(o.storepath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0664)
	if err != nil {
		return err
	}
	defer storecopy.Close()

	if len(o.metadata) == 0 {
		o.metadata = make(map[string]string)
	}

	_, err = io.Copy(storecopy, cachedcopy)
	if err != nil {
		return err
	}

	fmd := o.storepath + ".metadata"
	return writemeta(fmd, o.metadata)
}

func readmeta(filename string) (map[string]string, error) {
	metadata := make(map[string]string)
	b, err := os.ReadFile(filename)
	if err == nil {
		err = json.Unmarshal(b, &metadata)
		if err != nil {
			return nil, err
		}
	} else {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	return metadata, nil
}

func writemeta(filename string, meta map[string]string) error {
	bm, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}

	err = os.WriteFile(filename, bm, 0664)
	if err != nil {
		return err
	}
	return nil
}

func (o *object) Close() error {
	if !o.opened {
		return nil
	}

	defer func() {
		if o.cachedcopy != nil {
			n := o.cachedcopy.Name()
			os.Remove(n)
		}

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
		if !strings.Contains(err.Error(), os.ErrClosed.Error()) {
			return err
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
		o.cachedcopy.Close()
		o.cachedcopy = nil
		o.opened = false
		err := os.Remove(o.cachepath)
		if err != nil {
			return err
		}
	}
	// most likely this doesn't exist so don't return error
	os.Remove(o.cachepath)
	return nil
}
