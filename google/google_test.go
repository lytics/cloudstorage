package google_test

import (
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/google"
	"github.com/lytics/cloudstorage/testutils"
)

/*

# to use Google Cloud Storage ensure you have application default authentication working

gcloud auth application-default login

*/

func TestAll(t *testing.T) {
	config := &cloudstorage.Config{
		Type:       google.StoreType,
		AuthMethod: google.AuthGCEDefaultOAuthToken,
		Project:    "lio-testing",
		Bucket:     "liotesting-int-tests-nl",
		TmpDir:     t.TempDir(),
	}

	store, err := cloudstorage.NewStore(config)
	if err != nil {
		if strings.Contains(err.Error(), "could not find default credentials") {
			t.Skip("could not find default credentials, skipping Google Storage tests")
		}
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
