package cloudstorage

import (
	"encoding/base64"
	"fmt"
)

// TokenSource Is the source/location/type of token
type TokenSource string

// AccessLevel is the level of permissions on files
type AccessLevel int

const (
	// Authentication Source
	LyticsJWTKeySource   TokenSource = "LyticsJWTkey"
	GoogleJWTKeySource   TokenSource = "GoogleJWTFile"
	GCEMetaKeySource     TokenSource = "gcemetadata"
	LocalFileSource      TokenSource = "localfiles"
	GCEDefaultOAuthToken TokenSource = "gcedefaulttoken"

	// File Permissions Levels
	ReadOnly  AccessLevel = 0
	ReadWrite AccessLevel = 1
)

// ObjectIterator
type ObjectIterator interface {
}

// Objects are just a collection of Object(s).  Used as the results for store.List commands.
type Objects []Object

func (o Objects) Len() int           { return len(o) }
func (o Objects) Less(i, j int) bool { return o[i].Name() < o[j].Name() }
func (o Objects) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

// CloudStoreContext the cloud store config parameters
type CloudStoreContext struct {
	// logging context that's prefix to each logline.
	LogggingContext string
	// the methods of accessing the event archive. valid options are the xxxSource const above.
	TokenSource TokenSource

	// GCS Settings (project, bucket)
	Project string
	Bucket  string

	// the page size to use with google api requests (default 1000)
	PageSize int

	// used by LyticsJWTKeySource
	JwtConf *JwtConf
	// used by GoogleJWTKeySource
	JwtFile string
	// Permissions scope
	Scope string

	//LocalFS Archive
	LocalFS string // The location to use for archived events

	// The location to save locally cached seq files.
	TmpDir string
}

// JwtConf For use with google/google_jwttransporter.go
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

// Validate that this is a valid jwt conf set of tokens
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
