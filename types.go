package cloudstorage

import (
	"encoding/base64"
	"fmt"
)

const (
	// Authentication Source
	LyticsJWTKeySource   TokenSource = "LyticsJWTkey"
	GoogleJWTKeySource   TokenSource = "GoogleJWTFile"
	GCEMetaKeySource     TokenSource = "gcemetadata"
	LocalFileSource      TokenSource = "localfiles"
	GCEDefaultOAuthToken TokenSource = "gcedefaulttoken"

	// ReadOnly File Permissions Levels
	ReadOnly  AccessLevel = 0
	ReadWrite AccessLevel = 1
)

type (
	// TokenSource Is the source/location/type of token
	TokenSource string

	// AccessLevel is the level of permissions on files
	AccessLevel int

	// Objects are just a collection of Object(s).
	// Used as the results for store.List commands.
	Objects []Object

	// Config the cloud store config parameters
	Config struct {
		// StoreType [google,localfs,s3,azure]
		Type string
		// the methods of accessing the event archive.
		// valid options are the xxxSource const above.
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
		// LocalFS Archive
		LocalFS string // The location to use for archived events
		// The location to save locally cached seq files.
		TmpDir string
	}

	// JwtConf For use with google/google_jwttransporter.go
	// Which can be used by the google go sdk's
	JwtConf struct {
		PrivateKeyID     string `json:"private_key_id,omitempty"`
		PrivateKeyBase64 string `json:"private_key,omitempty"`
		ClientEmail      string `json:"client_email,omitempty"`
		ClientID         string `json:"client_id,omitempty"`
		Keytype          string `json:"type,omitempty"`
		// what scope to use when the token is created.
		// for example https://github.com/google/google-api-go-client/blob/0d3983fb069cb6651353fc44c5cb604e263f2a93/storage/v1/storage-gen.go#L54
		Scopes []string
	}
)

func (o Objects) Len() int           { return len(o) }
func (o Objects) Less(i, j int) bool { return o[i].Name() < o[j].Name() }
func (o Objects) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

// Validate that this is a valid jwt conf set of tokens
func (j *JwtConf) Validate() error {
	_, err := j.KeyBytes()
	if err != nil {
		return fmt.Errorf("Invalid JwtConf.PrivateKeyBase64  (error trying to decode base64 err: %v", err)
	}
	return nil
}

func (j *JwtConf) KeyBytes() ([]byte, error) {
	return base64.StdEncoding.DecodeString(j.PrivateKeyBase64)
}
