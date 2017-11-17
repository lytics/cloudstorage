// Cloud Storage interface to make Local, Google, s3 file storage
// share a common interface to aid testing local as well as
// running in the cloud
package cloudstorage

import (
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

const (
	// StoreCacheFileExt = ".cache"
	StoreCacheFileExt = ".cache"
	// ContextTypeKey
	ContextTypeKey = "content_type"
	// maxResults default number of objects to retrieve during a list-objects request,
	// if more objects exist, then they will need to be paged
	maxResults = 3000
)

var (
	// ErrObjectNotFound Error of not finding a file(object)
	ErrObjectNotFound = fmt.Errorf("object not found")
	// ErrObjectExists error trying to create an already existing file.
	ErrObjectExists = fmt.Errorf("object already exists in backing store (use store.Get)")
)

// NewStore create new Store from Storage Config/Context.
func NewStore(conf *Config) (Store, error) {

	if conf.PageSize == 0 {
		conf.PageSize = maxResults
	}

	if conf.TmpDir == "" {
		conf.TmpDir = os.TempDir()
	}

	switch conf.TokenSource {
	case GCEDefaultOAuthToken, GCEMetaKeySource, LyticsJWTKeySource, GoogleJWTKeySource:
		googleclient, err := NewGoogleClient(conf)
		if err != nil {
			return nil, err
		}
		return gcsCommonClient(googleclient.Client(), conf)
	case LocalFileSource:
		store, err := NewLocalStore(conf.LocalFS, conf.TmpDir)
		if err != nil {
			return nil, err
		}
		return store, nil
	default:
		return nil, fmt.Errorf("bad sourcetype: %v", conf.TokenSource)
	}
}

func gcsCommonClient(client *http.Client, conf *Config) (Store, error) {
	gcs, err := storage.NewClient(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	store, err := NewGCSStore(gcs, conf.Bucket, conf.TmpDir, maxResults)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func contentType(name string) string {
	contenttype := ""
	ext := filepath.Ext(name)
	if contenttype == "" {
		contenttype = mime.TypeByExtension(ext)
		if contenttype == "" {
			contenttype = "application/octet-stream"
		}
	}
	return contenttype
}

func ensureContextType(o string, md map[string]string) string {
	ctype, ok := md[ContextTypeKey]
	if !ok {
		ext := filepath.Ext(o)
		if ctype == "" {
			ctype = mime.TypeByExtension(ext)
			if ctype == "" {
				ctype = "application/octet-stream"
			}
		}
		md[ContextTypeKey] = ctype
	}
	return ctype
}

func exists(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false
	}
	return true
}

func cachepathObj(cachepath, oname, storeid string) string {
	obase := path.Base(oname)
	opath := path.Dir(oname)
	ext := path.Ext(oname)
	ext2 := fmt.Sprintf("%s.%s%s", ext, storeid, StoreCacheFileExt)
	var obase2 string
	if ext == "" {
		obase2 = obase + ext2
	} else {
		obase2 = strings.Replace(obase, ext, ext2, 1)
	}
	return path.Join(cachepath, opath, obase2)
}

func ensureDir(filename string) error {
	fdir := path.Dir(filename)
	if fdir != "" && fdir != filename {
		d, err := os.Stat(fdir)
		if err == nil {
			if !d.IsDir() {
				return fmt.Errorf("filename's dir exists but isn't' a directory: filename:%v dir:%v", filename, fdir)
			}
		} else if os.IsNotExist(err) {
			err := os.MkdirAll(fdir, 0775)
			if err != nil {
				return fmt.Errorf("unable to create path. : filename:%v dir:%v err:%v", filename, fdir, err)
			}
		}
	}
	return nil
}

// Copy source to destination.
func Copy(s Store, ctx context.Context, src, des Object) error {
	// for Providers that offer fast path, and use the backend copier
	if src.StorageSource() == des.StorageSource() {
		if cp, ok := s.(StoreCopy); ok {
			return cp.Copy(ctx, src, des)
		}
	}

	// Slow path, copy locally then up to des
	fout, err := des.Open(ReadWrite)
	if err != nil {
		return err
	}

	fin, err := src.Open(ReadOnly)
	if _, err = io.Copy(fout, fin); err != nil {
		return err
	}
	defer src.Close()

	return des.Close() //this will flush and sync the file.
}

// Move source object to destination.
func Move(ctx context.Context, src, des Object) error {
	//for GCS, take the fast path, and use the backend copier
	if src.StorageSource() == GCSFSStorageSource && des.StorageSource() == GCSFSStorageSource {
		srcgcs, ok := src.(*gcsFSObject)
		if !ok {
			return fmt.Errorf("error StoreageSource is declared as GCS, but cast failed???")
		}
		desgcs, ok := des.(*gcsFSObject)
		if !ok {
			return fmt.Errorf("error StoreageSource is declared as GCS, but cast failed???")
		}

		oh := srcgcs.gcsb.Object(srcgcs.name)
		dh := desgcs.gcsb.Object(desgcs.name)

		if _, err := dh.CopierFrom(oh).Run(ctx); err != nil {
			return err
		}

		if err := oh.Delete(ctx); err != nil {
			return err
		}

		return nil
	}

	// Slow path, copy locally then up to des
	fout, err := des.Open(ReadWrite)
	if err != nil {
		return err
	}

	fin, err := src.Open(ReadOnly)
	if _, err = io.Copy(fout, fin); err != nil {
		return err
	}
	if err := src.Close(); err != nil {
		return err
	}
	if err := src.Delete(); err != nil {
		return err
	}

	return des.Close() //this will flush and sync the file.
}
