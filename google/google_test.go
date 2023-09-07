package google_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/google"
	"github.com/lytics/cloudstorage/testutils"
)

/*

# to use Google Cloud Storage ensure you create a google-cloud jwt token

export CS_GCS_JWTKEY="{\"project_id\": \"lio-testing\", \"private_key_id\": \"

*/

var config = &cloudstorage.Config{
	Type:       google.StoreType,
	AuthMethod: google.AuthGCEDefaultOAuthToken,
	Project:    "lio-testing",
	Bucket:     "liotesting-int-tests-nl",
}

func TestAll(t *testing.T) {
	jwtVal := os.Getenv("CS_GCS_JWTKEY")
	if jwtVal == "" {
		t.Skip("Not testing no CS_GCS_JWTKEY env var")
		return
	}

	config.TmpDir = t.TempDir()

	jc := &cloudstorage.JwtConf{}

	if err := json.Unmarshal([]byte(jwtVal), jc); err != nil {
		t.Fatalf("Could not read CS_GCS_JWTKEY %v", err)
		return
	}
	if jc.ProjectID != "" {
		config.Project = jc.ProjectID
	}
	config.JwtConf = jc

	store, err := cloudstorage.NewStore(config)
	if err != nil {
		t.Fatalf("Could not create store: config=%+v  err=%v", config, err)
	}
	testutils.RunTests(t, store, config)

	config.EnableCompression = true
	store, err = cloudstorage.NewStore(config)
	if err != nil {
		t.Fatalf("Could not create store: config=%+v  err=%v", config, err)
	}
	testutils.RunTests(t, store, config)
}

func TestConfigValidation(t *testing.T) {

	tmpDir := t.TempDir()

	// VALIDATE errors for AuthJWTKeySource
	config := &cloudstorage.Config{}
	_, err := cloudstorage.NewStore(config)
	if err == nil {
		t.Fatalf("expected an error for an empty config: config=%+v", config)
	}

	jc := &cloudstorage.JwtConf{}
	config.JwtConf = jc

	_, err = cloudstorage.NewStore(config)
	if err == nil {
		t.Fatalf("expected an error for an empty config: config=%+v", config)
	}

	config = &cloudstorage.Config{
		Type:       google.StoreType,
		AuthMethod: google.AuthJWTKeySource,
		Project:    "tbd",
		Bucket:     "liotesting-int-tests-nl",
		TmpDir:     filepath.Join(tmpDir, "localcache", "google"),
	}

	_, err = cloudstorage.NewStore(config)
	if err == nil {
		t.Fatalf("expected an error for a config without a JwtConfig: config=%+v", config)
	}

	config.Type = ""
	_, err = cloudstorage.NewStore(config)
	if err == nil {
		t.Fatalf("expected an error for a config without a Type: config=%+v", config)
	}
	if !strings.Contains(err.Error(), "Type is required on Config") {
		t.Fatalf("expected error `Type is required on Config`: err=%v", err)
	}

	config.Type = google.StoreType
	config.AuthMethod = ""
	_, err = cloudstorage.NewStore(config)
	if err == nil {
		t.Fatalf("expected an error for a config without a AuthMethod: config=%+v", config)
	}
	if !strings.Contains(err.Error(), "bad AuthMethod") {
		t.Fatalf("expected error `bad AuthMethod`: err=%v", err)
	}

	// VALIDATE errors for AuthGoogleJWTKeySource (used to load a config from a JWT file)
	config = &cloudstorage.Config{
		Type:       google.StoreType,
		AuthMethod: google.AuthGoogleJWTKeySource,
		Project:    "tbd",
		Bucket:     "tbd",
		TmpDir:     filepath.Join(tmpDir, "localcache", "google"),
		JwtFile:    "./jwt.json",
	}
	_, err = cloudstorage.NewStore(config)
	if err == nil {
		t.Fatalf("expected an error for a config without a scopes: config=%+v", config)
	}

	config.Scope = storage.ScopeReadWrite
	_, err = cloudstorage.NewStore(config)
	if err == nil {
		t.Fatalf("expected an error for a config that points to a non-existent file: config=%+v", config)
	}
}
