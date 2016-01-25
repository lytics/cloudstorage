package cloudstorage

import (
	"encoding/base64"
	"fmt"
)

type TokenSource string

type AccessLevel int

const (
	LyticsJWTKeySource   TokenSource = "LyticsJWTkey"
	GoogleJWTKeySource   TokenSource = "GoogleJWTFile"
	GCEMetaKeySource     TokenSource = "gcemetadata"
	LocalFileSource      TokenSource = "localfiles"
	GCEDefaultOAuthToken TokenSource = "gcedefaulttoken"

	ReadOnly  AccessLevel = 0
	ReadWrite AccessLevel = 1
)

//Objects are just a collection of Object(s).  Used as the results for store.List commands.
type Objects []Object

func (o Objects) Len() int           { return len(o) }
func (o Objects) Less(i, j int) bool { return o[i].Name() < o[j].Name() }
func (o Objects) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

// the cloud store config parameters
//
//   # General settings
//   LogggingContext: logging context that's prefix to each logline.
//   TokenSource    : the methods of accessing the event archive.
//                    valid options are the xxxSource const above.
//   TmpDir         : where to cache local copies of the event files.
//   FilterStreams  : list of streams to filter out during archiving.
//   Topic          : the topic name to use when looking for archived events.
//                    all other topics will be assumed to be streaming only topics.
//
//   # GCS Archive settings
//   Project        :
//   Bucket         :
//   JwtConf        : required if TokenSource is JWTKey
//   PageSize       : what page size to use with Google
//
//   # Local Archive settings
//   LocalFS        : the location to use for archived events (i.e. the mocked cloud)

type CloudStoreContext struct {
	LogggingContext string

	TokenSource TokenSource

	//GCS Archive
	Project  string
	Bucket   string
	PageSize int // the page size to use with google api requests (default 1000)

	// used by LyticsJWTKeySource
	JwtConf *JwtConf
	// used by GoogleJWTKeySource
	JwtFile string
	Scope   string

	//LocalFS Archive
	LocalFS string // The location to use for archived events

	// The location to save locally cached seq files.
	TmpDir string
}

//For use with google/google_jwttransporter.go
// Which can be used by the google go sdk's
type JwtConf struct {
	//below are the fields from a Google Compute Engine's Credentials json file.
	Private_key_id    string
	Private_keybase64 string //TODO convert this to an encrypted key that only our code can decrypt.  Maybe using a key stored in metadata??
	Client_email      string
	Client_id         string
	Keytype           string
	Scopes            []string // what scope to use when the token is created.  for example https://github.com/google/google-api-go-client/blob/0d3983fb069cb6651353fc44c5cb604e263f2a93/storage/v1/storage-gen.go#L54
}

func (j *JwtConf) Validate() error {
	//convert Private_keybase64 to bytes.
	_, err := j.KeyBytes()
	if err != nil {
		return fmt.Errorf("Invalid EventStoreArchive.JwtConf.Private_keybase64  (error trying to decode base64 err: %v", err)
	}

	return nil
}

func (j *JwtConf) KeyBytes() ([]byte, error) {
	//convert Private_keybase64 to bytes.
	str := j.Private_keybase64
	return base64.StdEncoding.DecodeString(str)
}
