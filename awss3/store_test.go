package awss3_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/araddon/gou"
	"github.com/stretchr/testify/require"

	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/awss3"
	"github.com/lytics/cloudstorage/testutils"
)

/*

# to use aws tests ensure you have exported

export AWS_ACCESS_KEY="aaa"
export AWS_SECRET_KEY="bbb"
export AWS_BUCKET="bucket"

*/

func TestS3(t *testing.T) {
	if os.Getenv("AWS_SECRET_KEY") == "" || os.Getenv("AWS_ACCESS_KEY") == "" {
		t.Logf("No aws credentials, skipping")
		t.Skip()
		return
	}

	tmpDir, err := os.MkdirTemp("/tmp", "TestS3")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	conf := &cloudstorage.Config{
		Type: awss3.StoreType,
		Settings: gou.JsonHelper{
			"fake": "notused",
		},
	}
	// Should error with empty config
	_, err = cloudstorage.NewStore(conf)
	require.Error(t, err)

	conf.AuthMethod = awss3.AuthAccessKey
	conf.Settings[awss3.ConfKeyAccessKey] = ""
	conf.Settings[awss3.ConfKeyAccessSecret] = os.Getenv("AWS_SECRET_KEY")
	conf.Bucket = os.Getenv("AWS_BUCKET")
	conf.TmpDir = filepath.Join(tmpDir, "localcache", "aws")
	_, err = cloudstorage.NewStore(conf)
	require.Error(t, err)

	conf.Settings[awss3.ConfKeyAccessSecret] = ""
	_, err = cloudstorage.NewStore(conf)
	require.Error(t, err)

	// conf.Settings[awss3.ConfKeyAccessKey] = "bad"
	// conf.Settings[awss3.ConfKeyAccessSecret] = "bad"
	// _, err = cloudstorage.NewStore(conf)
	// require. NotEqual(t, nil, err)

	conf.BaseUrl = "s3.custom.endpoint.com"
	conf.Settings[awss3.ConfKeyAccessKey] = os.Getenv("AWS_ACCESS_KEY")
	conf.Settings[awss3.ConfKeyAccessSecret] = os.Getenv("AWS_SECRET_KEY")
	client, _, err := awss3.NewClient(conf)
	require.NoError(t, err)
	require.NotNil(t, client)

	conf.Settings[awss3.ConfKeyDisableSSL] = true
	client, sess, err := awss3.NewClient(conf)
	require.NoError(t, err)
	require.NotNil(t, client)

	conf.TmpDir = ""
	_, err = awss3.NewStore(client, sess, conf)
	require.Error(t, err)

	// Trying to find dir they don't have access to?
	conf.TmpDir = "/home/fake"
	_, err = cloudstorage.NewStore(conf)
	require.Error(t, err)
}

func TestAll(t *testing.T) {
	tmpDir, err := os.MkdirTemp("/tmp", "TestS3")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := &cloudstorage.Config{
		Type:       awss3.StoreType,
		AuthMethod: awss3.AuthAccessKey,
		Bucket:     os.Getenv("AWS_BUCKET"),
		TmpDir:     filepath.Join(tmpDir, "localcache", "aws"),
		Settings:   make(gou.JsonHelper),
		Region:     "us-east-1",
	}
	config.Settings[awss3.ConfKeyAccessKey] = os.Getenv("AWS_ACCESS_KEY")
	config.Settings[awss3.ConfKeyAccessSecret] = os.Getenv("AWS_SECRET_KEY")
	//gou.Debugf("config %v", config)
	if config.Bucket == "" || os.Getenv("AWS_SECRET_KEY") == "" || os.Getenv("AWS_ACCESS_KEY") == "" {
		t.Logf("No aws credentials, skipping")
		t.Skip()
		return
	}
	store, err := cloudstorage.NewStore(config)
	if err != nil {
		t.Logf("No valid auth provided, skipping awss3 testing %v", err)
		t.Skip()
		return
	}
	if store == nil {
		t.Fatalf("No store???")
	}
	testutils.RunTests(t, store, config)
}
