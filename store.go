package cloudstorage

import (
	"fmt"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/lytics/lio/src/common"
	"github.com/lytics/lio/src/config"
	"github.com/lytics/lio/src/ext_svcs/google"
	"google.golang.org/cloud"
)

const StoreCacheFileExt = ".storecache"

var ObjectNotFound = fmt.Errorf("object not found")

//maxResults default number of objects to retrieve during a list-objects request,
// if more objects exist, then they will need to be paged
const maxResults = 3000

func NewStore(conf *config.CloudStore, l common.Logger) (Store, error) {

	if conf.PageSize == 0 {
		conf.PageSize = maxResults
	}

	if conf.TmpDir == "" {
		conf.TmpDir = os.TempDir()
	}

	switch conf.TokenSource {

	//TODO
	// case config.GCEDefaultOAuthToken :
	//   This auth method would use the default OAuth token created by tools like gsutils, gcloud, etc...
	//   I plan on using this token with bulkutils so we can run bulkutils locally like we can current
	//   use gsutils to download files.
	//   See github.com/lytics/lio/src/ext_svcs/google/google_transporter.go : BuildDefaultGoogleTransporter
	//   The only reason Im not doing this now is to avoid the overhead of testing it..
	//
	case config.GCEMetaKeySource:
		project := conf.Project
		bucket := conf.Bucket
		prefix := fmt.Sprintf("%s:(project=%s bucket=%s)", l.Prefix(), project, bucket)
		l := common.NewPrefixLogger(prefix)

		googleclient, err := google.BuildGCEMetadatTransporter("")
		if err != nil {
			l.Errorf("error creating the GCEMetadataTransport and http client. project=%s gs://%s/ err=%v ",
				project, bucket, err)
			return nil, err
		}
		ctx := cloud.NewContext(project, googleclient.Client())
		return NewGCSStore(ctx, bucket, conf.TmpDir, maxResults, l), nil
	case config.JWTKeySource:
		project := conf.Project
		bucket := conf.Bucket
		prefix := fmt.Sprintf("%s:(project=%s bucket=%s)", l.Prefix(), project, bucket)
		l := common.NewPrefixLogger(prefix)

		googleclient, err := google.eventstore - gcs - localfiles(conf.JwtConf)
		if err != nil {
			l.Errorf("error creating the JWTTransport and http client. project=%s gs://%s/ keylen:%d err=%v ",
				project, bucket, len(conf.JwtConf.Private_keybase64), err)
			return nil, err
		}
		ctx := cloud.NewContext(project, googleclient.Client())
		return NewGCSStore(ctx, bucket, conf.TmpDir, maxResults, l), nil
	case config.LocalFileSource:
		return NewLocalStore(conf.LocalFS, conf.TmpDir, l), nil
	default:
		return nil, fmt.Errorf("bad sourcetype: %v", conf.TokenSource)
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
	ext2 := fmt.Sprintf(".%s%s", storeid, StoreCacheFileExt)
	obase2 := strings.Replace(obase, ext, ext2, 1)
	cn := path.Join(cachepath, opath, obase2)
	return cn
}
