package cloudstorage

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/lytics/cloudstorage/logging"
	"github.com/pborman/uuid"
)

const LocalFSStorageSource = "localFS"

type Localstore struct {
	Log       logging.Logger
	storepath string
	cachepath string
	Id        string
}

func NewLocalStore(storepath, cachepath string, l logging.Logger) (*Localstore, error) {

	if storepath == cachepath {
		return nil, fmt.Errorf("storepath cannot be the same as cachepath")
	}

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

	return &Localstore{storepath: storepath, cachepath: cachepath, Id: uid, Log: l}, nil
}

func (l *Localstore) NewObject(objectname string) (Object, error) {
	obj, err := l.Get(objectname)
	if err != nil && err != ObjectNotFound {
		return nil, err
	} else if obj != nil {
		return nil, ObjectExists
	}

	of := path.Join(l.storepath, objectname)
	err = ensureDir(of)
	if err != nil {
		return nil, err
	}

	cf := cachepathObj(l.cachepath, objectname, l.Id)

	return &localFSObject{
		name:      objectname,
		storepath: of,
		cachepath: cf,
	}, nil
}

/*

removed as part of the effort to simply the interface

func (l *Localstore) WriteObject(o string, meta map[string]string, b []byte) error {
	fo := path.Join(l.storepath, o)

	err := os.MkdirAll(path.Dir(fo), 0775)
	if err != nil {
		l.Log.Printf("unable to create path. file=%s err=%v", fo, err)
		return err
	}

	err = ioutil.WriteFile(fo, b, 0664)
	if err != nil {
		return err
	}

	if meta != nil && len(meta) > 0 {
		fmd := fo + ".metadata"
		writemeta(fmd, meta)
	}

	return nil
}
*/

func (l *Localstore) List(query Query) (Objects, error) {
	objects := make(map[string]*localFSObject)
	metadatas := make(map[string]map[string]string)

	spath := path.Join(l.storepath, query.Prefix)
	if !exists(spath) {
		return make(Objects, 0), nil
	}

	err := filepath.Walk(spath, func(fo string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		obj := strings.Replace(fo, l.storepath, "", 1)

		if f.IsDir() {
			return nil
		} else if filepath.Ext(f.Name()) == ".metadata" {
			b, err := ioutil.ReadFile(fo)
			if err != nil {
				return err
			}
			md := make(map[string]string)
			err = json.Unmarshal(b, &md)
			if err != nil {
				return err
			}

			mdkey := strings.Replace(obj, ".metadata", "", 1)
			metadatas[mdkey] = md
		} else {
			oname := strings.TrimPrefix(obj, "/")
			objects[obj] = &localFSObject{
				name:      oname,
				updated:   f.ModTime(),
				storepath: fo,
				cachepath: cachepathObj(l.cachepath, oname, l.Id),
			}
		}
		return err
	})

	if err != nil {
		return nil, fmt.Errorf("localfile: error occurred listing files. searchpath=%v err=%v", spath, err)
	}

	res := make(Objects, 0)

	for objname, obj := range objects {
		if md, ok := metadatas[objname]; ok {
			obj.metadata = md
		}
		res = append(res, obj)
	}

	res = query.applyFilters(res)

	return res, nil
}

func (l *Localstore) Get(o string) (Object, error) {
	fo := path.Join(l.storepath, o)

	if !exists(fo) {
		return nil, ObjectNotFound
	}
	var updated time.Time
	if stat, err := os.Stat(fo); err == nil {
		updated = stat.ModTime()
	}

	return &localFSObject{
		name:      o,
		updated:   updated,
		storepath: fo,
		cachepath: cachepathObj(l.cachepath, o, l.Id),
	}, nil
}

func (l *Localstore) Delete(obj string) error {
	fo := path.Join(l.storepath, obj)
	os.Remove(fo)
	mf := fo + ".metadata"
	if exists(mf) {
		os.Remove(mf)
	}
	return nil
}

func (l *Localstore) String() string {
	return fmt.Sprintf("[id:%s file://%s/]", l.Id, l.storepath)
}

type localFSObject struct {
	name     string
	updated  time.Time
	metadata map[string]string

	storepath string
	cachepath string

	cachedcopy *os.File
	readonly   bool
	opened     bool
}

func (o *localFSObject) StorageSource() string {
	return LocalFSStorageSource
}
func (o *localFSObject) Name() string {
	return o.name
}
func (o *localFSObject) String() string {
	return o.name
}
func (o *localFSObject) Updated() time.Time {
	return o.updated
}
func (o *localFSObject) MetaData() map[string]string {
	return o.metadata
}
func (o *localFSObject) SetMetaData(meta map[string]string) {
	o.metadata = meta
}

func (o *localFSObject) Open(accesslevel AccessLevel) (*os.File, error) {
	if o.opened {
		return nil, fmt.Errorf("the store object is already opened. %s", o.storepath)
	}

	var readonly = accesslevel == ReadOnly

	storecopy, err := os.OpenFile(o.storepath, os.O_RDWR|os.O_CREATE, 0665)
	if err != nil {
		return nil, fmt.Errorf("localfile: error occurred opening storecopy file. local=%s err=%v",
			o.storepath, err)
	}
	defer storecopy.Close()

	err = ensureDir(o.cachepath)
	if err != nil {
		return nil, fmt.Errorf("localfile: error occurred creating cachedcopy's dir. cachepath=%s err=%v",
			o.cachepath, err)
	}

	cachedcopy, err := os.Create(o.cachepath)
	if err != nil {
		return nil, fmt.Errorf("localfile: error occurred opening cachedcopy file. cachepath=%s err=%v",
			o.cachepath, err)
	}

	_, err = io.Copy(cachedcopy, storecopy)
	if err != nil {
		return nil, fmt.Errorf("localfile: error occurred reading the bytes returned from localfile. storepath=%s tfile=%v err=%v",
			o.storepath, cachedcopy.Name(), err)
	}

	if readonly {
		cachedcopy.Close()
		cachedcopy, err = os.Open(o.cachepath)
		if err != nil {
			return nil, fmt.Errorf("localfile: error occurred opening file. storepath=%s tfile=%v err=%v",
				o.storepath, cachedcopy.Name(), err)
		}
	}

	o.cachedcopy = cachedcopy
	o.readonly = readonly
	o.opened = true
	return o.cachedcopy, nil
}

func (o *localFSObject) File() *os.File {
	return o.cachedcopy
}
func (o *localFSObject) Read(p []byte) (n int, err error) {
	return o.cachedcopy.Read(p)
}
func (o *localFSObject) Write(p []byte) (n int, err error) {
	return o.cachedcopy.Write(p)
}

func (o *localFSObject) Sync() error {
	if !o.opened {
		return fmt.Errorf("object isn't opened %s", o.name)
	}
	if o.readonly {
		return fmt.Errorf("trying to Sync a readonly object %s", o.name)
	}

	cachedcopy, err := os.OpenFile(o.cachepath, os.O_RDONLY, 0664)
	if err != nil {
		return fmt.Errorf("gcsfs: couldn't open localfile for sync'ing. local=%s err=%v",
			o.cachepath, err)
	}
	defer cachedcopy.Close()

	storecopy, err := os.OpenFile(o.storepath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0664)
	if err != nil {
		return fmt.Errorf("localfile: error occurred open file. local=%s err=%v",
			o.storepath, err)
	}
	defer storecopy.Close()

	_, err = io.Copy(storecopy, cachedcopy)
	if err != nil {
		return fmt.Errorf("localfile: error occurred in sync copying of local to store. local=%s object=%s tfile=%v err=%v",
			o.storepath, o.name, cachedcopy.Name(), err)
	}

	if o.metadata != nil && len(o.metadata) > 0 {
		o.metadata = make(map[string]string)
	}

	fmd := o.storepath + ".metadata"
	writemeta(fmd, o.metadata)

	return nil
}

func writemeta(filename string, meta map[string]string) error {
	bm, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(filename, bm, 0664)
	if err != nil {
		return err
	}
	return nil
}

func (o *localFSObject) Close() error {
	if !o.opened {
		return nil
	}

	err := o.cachedcopy.Sync()
	if err != nil {
		return err
	}

	err = o.cachedcopy.Close()
	if err != nil {
		return err
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

func (o *localFSObject) Release() error {
	return os.Remove(o.cachepath)
}
