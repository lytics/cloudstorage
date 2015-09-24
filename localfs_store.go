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

	"github.com/lytics/lio/src/common"
	"github.com/pborman/uuid"
)

const LocalFSStorageSource = "localFS"

type Localstore struct {
	Log       common.Logger
	storepath string
	cachepath string
	Id        string
}

func NewLocalStore(storepath, cachepath string, log common.Logger) *Localstore {
	err := os.MkdirAll(path.Dir(storepath), 0775)
	if err != nil {
		log.Errorf("unable to create path. path=%s err=%v", storepath, err)
	}

	err = os.MkdirAll(path.Dir(cachepath), 0775)
	if err != nil {
		log.Errorf("unable to create path. path=%s err=%v", cachepath, err)
	}

	uid := uuid.NewUUID().String()
	uid = strings.Replace(uid, "-", "", -1)

	return &Localstore{storepath: storepath, cachepath: cachepath, Id: uid, Log: log}
}

func (l *Localstore) NewObject(name string) (Object, error) {
	return &localFSObject{
		name:      name,
		storepath: path.Join(l.storepath, name),
		cachepath: cachepathObj(l.cachepath, name, l.Id),
	}, nil
}

/*

removed as part of the effort to simply the interface

func (l *Localstore) WriteObject(o string, meta map[string]string, b []byte) error {
	fo := path.Join(l.storepath, o)

	err := os.MkdirAll(path.Dir(fo), 0775)
	if err != nil {
		l.Log.Errorf("unable to create path. file=%s err=%v", fo, err)
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
				l.Log.Debugf("localfile: unable to open metadata file. err:%v", err)
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
			objects[obj] = &localFSObject{
				name:      obj,
				storepath: fo,
				cachepath: cachepathObj(l.cachepath, obj, l.Id),
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
			ensureContextType(obj.name, md)
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

	return &localFSObject{
		name:      o,
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
func (o *localFSObject) MetaData() map[string]string {
	return o.metadata
}
func (o *localFSObject) SetMetaData(meta map[string]string) {
	o.metadata = meta
}
func (o *localFSObject) Open(readonly bool) error {
	if o.opened {
		return fmt.Errorf("the store object is already opened. %s", o.storepath)
	}

	storecopy, err := os.OpenFile(o.storepath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("localfile: error occurred open storecopy file. local=%s err=%v",
			o.storepath, err)
	}
	defer storecopy.Close()

	err = os.MkdirAll(path.Dir(o.cachepath), 0775)
	if err != nil {
		return fmt.Errorf("localfile: error occurred creating cachedcopy dir. cachepath=%s object=%s err=%v",
			o.cachepath, o.name, err)
	}

	cachedcopy, err := os.Create(o.cachepath)
	if err != nil {
		return fmt.Errorf("localfile: error occurred open cachedcopy file. cachepath=%s object=%s err=%v",
			o.cachepath, o.name, err)
	}

	_, err = io.Copy(cachedcopy, storecopy)
	if err != nil {
		return fmt.Errorf("localfile: error occurred reading the bytes returned from localfile. storepath=%s object=%s tfile=%v err=%v",
			o.storepath, o.name, cachedcopy.Name(), err)
	}

	if readonly {
		cachedcopy.Close()
		cachedcopy, err = os.Open(o.cachepath)
		if err != nil {
			return fmt.Errorf("localfile: error occurred open file. storepath=%s object=%s tfile=%v err=%v",
				o.storepath, o.name, cachedcopy.Name(), err)
		}
	}

	o.cachedcopy = cachedcopy
	o.readonly = readonly
	o.opened = true
	return nil
}
func (o *localFSObject) CachedCopy() *os.File {
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

	storecopy, err := os.OpenFile(o.storepath, os.O_APPEND|os.O_RDWR, 0664)
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
		fmd := o.storepath + ".metadata"
		writemeta(fmd, o.metadata)
	}
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
