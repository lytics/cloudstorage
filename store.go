package cloudstorage

import (
	"fmt"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/lytics/cloudstorage/logging"
	"golang.org/x/net/context"
	"google.golang.org/cloud"
	"google.golang.org/cloud/storage"
)

const StoreCacheFileExt = ".cache"

var ObjectNotFound = fmt.Errorf("object not found")
var ObjectExists = fmt.Errorf("object already exists in backing store (use store.Get)")

var LogConstructor = func(prefix string) logging.Logger {
	return logging.NewStdLogger(true, logging.DEBUG, prefix)
}

//maxResults default number of objects to retrieve during a list-objects request,
// if more objects exist, then they will need to be paged
const maxResults = 3000

func NewStore(csctx *CloudStoreContext) (Store, error) {

	if csctx.PageSize == 0 {
		csctx.PageSize = maxResults
	}

	if csctx.TmpDir == "" {
		csctx.TmpDir = os.TempDir()
	}

	switch csctx.TokenSource {

	case GCEDefaultOAuthToken:
		//   This token method uses the default OAuth token with GCS created by tools like gsutils, gcloud, etc...
		//   See github.com/lytics/lio/src/ext_svcs/google/google_transporter.go : BuildDefaultGoogleTransporter
		//   The only reason Im not doing this now is to avoid the overhead of testing it..
		//
		project := csctx.Project
		bucket := csctx.Bucket

		//TODO replace lio's logger witn one for the package.
		prefix := fmt.Sprintf("%s:(project=%s bucket=%s)", csctx.LogggingContext, project, bucket)
		l := LogConstructor(prefix)

		googleclient, err := BuildDefaultGoogleTransporter("")
		if err != nil {
			l.Errorf("error creating the GCEMetadataTransport and http client. project=%s gs://%s/ err=%v ",
				project, bucket, err)
			return nil, err
		}
		gcs, err := storage.NewClient(context.Background(), cloud.WithBaseHTTP(googleclient.Client()))
		if err != nil {
			l.Errorf("%v error creating google cloud storeage client. project:%s gs://%s/ err:%v ",
				csctx.LogggingContext, project, bucket, err)
			return nil, err
		}
		store, err := NewGCSStore(gcs, bucket, csctx.TmpDir, maxResults, l)
		if err != nil {
			l.Errorf("error creating the store. err=%v ", err)
			return nil, err
		}
		return store, nil
	case GCEMetaKeySource:
		project := csctx.Project
		bucket := csctx.Bucket

		//TODO replace lio's logger witn one for the package.
		prefix := fmt.Sprintf("%s:(project=%s bucket=%s)", csctx.LogggingContext, project, bucket)
		l := LogConstructor(prefix)

		googleclient, err := BuildGCEMetadatTransporter("")
		if err != nil {
			l.Errorf("error creating the GCEMetadataTransport and http client. project=%s gs://%s/ err=%v ",
				project, bucket, err)
			return nil, err
		}
		gcs, err := storage.NewClient(context.Background(), cloud.WithBaseHTTP(googleclient.Client()))
		if err != nil {
			l.Errorf("%v error creating google cloud storeage client. project:%s gs://%s/ err:%v ",
				csctx.LogggingContext, project, bucket, err)
			return nil, err
		}
		store, err := NewGCSStore(gcs, bucket, csctx.TmpDir, maxResults, l)
		if err != nil {
			l.Errorf("error creating the store. err=%v ", err)
			return nil, err
		}
		return store, nil
	case JWTKeySource:
		project := csctx.Project
		bucket := csctx.Bucket
		prefix := fmt.Sprintf("%s:(project=%s bucket=%s)", csctx.LogggingContext, project, bucket)
		l := LogConstructor(prefix)

		googleclient, err := BuildJWTTransporter(csctx.JwtConf)
		if err != nil {
			l.Errorf("error creating the JWTTransport and http client. project=%s gs://%s/ keylen:%d err=%v ",
				project, bucket, len(csctx.JwtConf.Private_keybase64), err)
			return nil, err
		}
		gcs, err := storage.NewClient(context.Background(), cloud.WithBaseHTTP(googleclient.Client()))
		if err != nil {
			l.Errorf("%v error creating google cloud storeage client. project:%s gs://%s/ err:%v ",
				csctx.LogggingContext, project, bucket, err)
			return nil, err
		}
		store, err := NewGCSStore(gcs, bucket, csctx.TmpDir, maxResults, l)
		if err != nil {
			l.Errorf("error creating the store. err=%v ", err)
			return nil, err
		}
		return store, nil
	case LocalFileSource:
		prefix := fmt.Sprintf("%s:", csctx.LogggingContext)
		l := LogConstructor(prefix)

		store, err := NewLocalStore(csctx.LocalFS, csctx.TmpDir, l)
		if err != nil {
			l.Errorf("error creating the store. err=%v ", err)
			return nil, err
		}
		return store, nil
	default:
		return nil, fmt.Errorf("bad sourcetype: %v", csctx.TokenSource)
	}
}

const ContextTypeKey = "content_type"

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
	obase2 := strings.Replace(obase, ext, ext2, 1)
	cn := path.Join(cachepath, opath, obase2)

	return cn
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
