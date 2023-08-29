package cloudstorage_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/localfs"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("/tmp", "TestStore")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	invalidConf := &cloudstorage.Config{}

	store, err := cloudstorage.NewStore(invalidConf)
	require.Error(t, err)
	require.Nil(t, store)

	missingStoreConf := &cloudstorage.Config{
		Type: "non-existent-store",
	}

	store, err = cloudstorage.NewStore(missingStoreConf)
	require.Error(t, err)
	require.Nil(t, store)

	// test missing temp dir, assign local temp
	localFsConf := &cloudstorage.Config{
		Type:       localfs.StoreType,
		AuthMethod: localfs.AuthFileSystem,
		LocalFS:    filepath.Join(tmpDir, "mockcloud"),
	}

	store, err = cloudstorage.NewStore(localFsConf)
	require.Nil(t, err)
	require.NotNil(t, store)
}

func TestJwtConf(t *testing.T) {
	configInput := `
	{
		"JwtConf": {
			"type": "service_account",
			"project_id": "testing",
			"private_key_id": "abcdefg",
			"private_key": "aGVsbG8td29ybGQ=",
			"client_email": "testing@testing.iam.gserviceaccount.com",
			"client_id": "117058426251532209964",
			"scopes": [
				"https://www.googleapis.com/auth/devstorage.read_write"
			]
		}
	}`

	// v := base64.StdEncoding.EncodeToString([]byte("hello-world"))
	// t.Logf("b64  %q", v)
	conf := &cloudstorage.Config{}
	err := json.Unmarshal([]byte(configInput), conf)
	require.Nil(t, err)
	conf.JwtConf.PrivateKey = "------helo-------\naGVsbG8td29ybGQ=\n-----------------end--------"
	require.NotNil(t, conf.JwtConf)
	require.Nil(t, conf.JwtConf.Validate())
	require.Equal(t, "aGVsbG8td29ybGQ=", conf.JwtConf.PrivateKey)
	require.Equal(t, "service_account", conf.JwtConf.Type)

	// note on this one the "keytype" & "private_keybase64"
	configInput = `
	{
		"JwtConf": {
			"keytype": "service_account",
			"project_id": "testing",
			"private_key_id": "abcdefg",
			"private_keybase64": "aGVsbG8td29ybGQ=",
			"client_email": "testing@testing.iam.gserviceaccount.com",
			"client_id": "117058426251532209964",
			"scopes": [
				"https://www.googleapis.com/auth/devstorage.read_write"
			]
		}
	}`
	conf = &cloudstorage.Config{}
	err = json.Unmarshal([]byte(configInput), conf)
	require.Nil(t, err)
	require.NotNil(t, conf.JwtConf)
	require.Nil(t, conf.JwtConf.Validate())
	require.Equal(t, "aGVsbG8td29ybGQ=", conf.JwtConf.PrivateKey)
	require.Equal(t, "service_account", conf.JwtConf.Type)
}
