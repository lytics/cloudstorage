package azure_test

import (
	"os"
	"testing"

	"github.com/araddon/gou"
	"github.com/stretchr/testify/require"

	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/azure"
	"github.com/lytics/cloudstorage/testutils"
)

/*
# to use azure tests ensure you have exported

export AZURE_KEY="aaa"
export AZURE_PROJECT="bbb"
export AZURE_BUCKET="cloudstorageunittests"
*/
var config = &cloudstorage.Config{
	Type:       azure.StoreType,
	AuthMethod: azure.AuthKey,
	Bucket:     os.Getenv("AZURE_BUCKET"),
	Settings:   make(gou.JsonHelper),
}

func TestConfig(t *testing.T) {
	if config.Bucket == "" {
		t.Logf("must provide AZURE_PROJECT, AZURE_KEY, AZURE_PROJECT  env vars")
		t.Skip()
		return
	}

	conf := &cloudstorage.Config{
		Type:     azure.StoreType,
		Project:  os.Getenv("AZURE_PROJECT"),
		Settings: make(gou.JsonHelper),
	}
	// Should error with empty config
	_, err := cloudstorage.NewStore(conf)
	require.Error(t, err)

	conf.AuthMethod = azure.AuthKey
	conf.Settings[azure.ConfKeyAuthKey] = ""
	_, err = cloudstorage.NewStore(conf)
	require.Error(t, err)

	conf.Settings[azure.ConfKeyAuthKey] = "bad"
	_, err = cloudstorage.NewStore(conf)
	require.Error(t, err)

	conf.Settings[azure.ConfKeyAuthKey] = os.Getenv("AZURE_KEY")
	client, sess, err := azure.NewClient(conf)
	require.NoError(t, err)
	require.NotNil(t, client)
	conf.TmpDir = ""
	_, err = azure.NewStore(client, sess, conf)
	require.Error(t, err)

	// Trying to find dir they don't have access to?
	conf.TmpDir = "/home/fake"
	_, err = cloudstorage.NewStore(conf)
	require.Error(t, err)
}

func TestAll(t *testing.T) {
	config.Project = os.Getenv("AZURE_PROJECT")
	if config.Project == "" {
		t.Logf("must provide AZURE_PROJECT")
		t.Skip()
		return
	}

	config.TmpDir = t.TempDir()

	config.Settings[azure.ConfKeyAuthKey] = os.Getenv("AZURE_KEY")
	store, err := cloudstorage.NewStore(config)
	if err != nil {
		t.Logf("No valid auth provided, skipping azure testing %v", err)
		t.Skip()
		return
	}
	client := store.Client()
	require.NotNil(t, client)

	testutils.RunTests(t, store, config)
}
