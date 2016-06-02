package cloudstorage

import (
	"bytes"
	"fmt"
	"io"
	"mime"
	"net/http"
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
	case GCEDefaultOAuthToken, GCEMetaKeySource, LyticsJWTKeySource, GoogleJWTKeySource:
		googleclient, err := NewGoogleClient(csctx)
		if err != nil {
			return nil, err
		}
		return gcsCommonClient(googleclient.Client(), csctx)
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

func gcsCommonClient(client *http.Client, csctx *CloudStoreContext) (Store, error) {
	project := csctx.Project
	bucket := csctx.Bucket
	prefix := fmt.Sprintf("%s:(project=%s bucket=%s)", csctx.LogggingContext, project, bucket)
	l := LogConstructor(prefix)

	gcs, err := storage.NewClient(context.Background(), cloud.WithBaseHTTP(client))
	if err != nil {
		l.Errorf("%v error creating Google cloud storage client. project:%s gs://%s/ err:%v ",
			csctx.LogggingContext, project, bucket, err)
		return nil, err
	}
	store, err := NewGCSStore(gcs, bucket, csctx.TmpDir, maxResults, l)
	if err != nil {
		l.Errorf("error creating the store. err=%v ", err)
		return nil, err
	}
	return store, nil
}

func GetObject(gc *storage.Client, bucket, name string) (*bytes.Buffer, error) {
	// Get buckethandler
	gsbh := gc.Bucket(bucket)

	// Create Query
	q := storage.Query{Prefix: name, MaxResults: 1}

	// Get list of *the* object
	ol, err := List(gsbh, q)
	if err != nil {
		return nil, err
	}

	buff, errs := OpenObject(ol, gsbh)
	if buff == nil {
		if len(errs) >= 0 {
			i := 0
			errBuff := bytes.NewBufferString("GetObject Errors:\n")
			for _, e := range errs {
				i++
				errBuff.WriteString(e.Error())
				errBuff.WriteString("\n")
			}
			err := fmt.Errorf("%s", errBuff.String())
			return nil, err
		} else {
			return nil, fmt.Errorf("GetObject recieved nil byte buffer and no errors")
		}
	} else {
		//success
		return buff, nil
	}
}

// Opens the first object returned in a storage.ObjectList
// returns contents via byte Buffer
func OpenObject(objects *storage.ObjectList, gcsb *storage.BucketHandle) (*bytes.Buffer, []error) {
	var buff *bytes.Buffer = bytes.NewBuffer([]byte{})
	log := logging.NewStdLogger(true, 4, "OpenObject")
	var googleObject *storage.ObjectAttrs
	errs := make([]error, 0)

	for try := 0; try < GCSRetries; try++ {
		if objects.Results != nil && len(objects.Results) != 0 {
			googleObject = objects.Results[0]
		}

		if googleObject != nil {
			//we have a preexisting object, so lets download it..
			rc, err := gcsb.Object(googleObject.Name).NewReader(context.Background())
			if err != nil {
				errs = append(errs, fmt.Errorf("error storage.NewReader err=%v", err))
				log.Debugf("%v", errs)
				backoff(try)
				continue
			}
			defer rc.Close()

			_, err = io.Copy(buff, rc)
			if err != nil {
				errs = append(errs, fmt.Errorf("error coping bytes. err=%v", err))
				log.Debugf("%v", errs)
				backoff(try)
				continue
			}
		}
		return buff, nil
	}
	errs = append(errs, fmt.Errorf("OpenObject errors past limit!"))
	return nil, errs
}

// Iterates through pages of results from a Google Storage Bucket and
// lists objects which match the specified storage.Query
func List(gcsb *storage.BucketHandle, query storage.Query) (*storage.ObjectList, error) {

	gobjects, err := ListBucketObjectsReq(gcsb, &query, GCSRetries)
	if err != nil {
		fmt.Errorf("couldn't list objects. prefix=%s err=%v", query.Prefix, err)
		return nil, err
	}

	if gobjects == nil {
		return nil, nil
	}

	if gobjects.Next != nil {
		q := gobjects.Next
		for q != nil {
			gobjectsB, err := ListBucketObjectsReq(gcsb, q, GCSRetries)
			if err != nil {
				fmt.Errorf("couldn't list the remaining pages of objects. prefix=%s err=%v", q.Prefix, err)
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

	return gobjects, nil
}

//ListObjects is a wrapper around storeage.ListObjects, that retries on a GCS error.  GCS isn't a prefect system :p, and returns an error
//  about once every 2 weeks.
func ListBucketObjectsReq(gcsb *storage.BucketHandle, q *storage.Query, retries int) (*storage.ObjectList, error) {
	var lasterr error = nil
	//GCS sometimes returns a 500 error, so we'll just retry...
	for i := 0; i < retries; i++ {
		objects, err := gcsb.List(context.Background(), q)
		if err != nil {
			fmt.Errorf("error listing objects for the bucket. try:%d q.prefix:%v err:%v", i, q.Prefix, err)
			lasterr = err
			backoff(i)
			continue
		}
		return objects, nil
	}
	return nil, lasterr
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
