package backblaze_test

import (
	"bufio"
	"io/ioutil"
	"os"
	"testing"

	"github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/backblaze"
)

/*
# to use backblaze tests ensure you have exported

export BACKBLAZE_BUCKET="bucket"
export BACKBLAZE_ACCOUNT="alfa"
export BACKBLAZE_KEY="my-hex-key-afea1d"
*/

var config = &cloudstorage.Config{
	Type:       backblaze.StoreType,
	AuthMethod: backblaze.AuthKey,
	Bucket:     os.Getenv("BACKBLAZE_BUCKET"),
	TmpDir:     "/tmp/localcache/backblaze",
	Settings:   make(gou.JsonHelper),
}

func TestConfig(t *testing.T) {

	if config.Bucket == "" {
		t.Logf("must provide BACKBLAZE_BUCKET, BACKBLAZE_ACCOUNT,  BACKBLAZE_KEY  env vars")
		t.Skip()
		return
	}

	conf := &cloudstorage.Config{
		Type:       backblaze.StoreType,
		Bucket:     os.Getenv("BACKBLAZE_BUCKET"),
		AuthMethod: backblaze.AuthKey,
		TmpDir:     "/tmp/localcache/backblaze",
		Settings:   make(gou.JsonHelper),
	}

	// Should error with empty config
	_, err := cloudstorage.NewStore(conf)
	assert.NotEqual(t, nil, err)

	conf.AuthMethod = backblaze.AuthKey

	conf.Settings[backblaze.Account] = ""
	_, err = cloudstorage.NewStore(conf)
	assert.NotEqual(t, nil, err)

	conf.Settings[backblaze.Account] = os.Getenv("BACKBLAZE_ACCOUNT")
	client, err := backblaze.NewClient(conf)
	assert.NotEqual(t, nil, err)

	conf.Settings[backblaze.Key] = ""
	_, err = cloudstorage.NewStore(conf)
	assert.NotEqual(t, nil, err)

	conf.Settings[backblaze.Key] = os.Getenv("BACKBLAZE_KEY")
	client, err = backblaze.NewClient(conf)
	assert.Equal(t, nil, err)

	assert.NotEqual(t, nil, client)
	conf.TmpDir = ""

	_, err = backblaze.NewStore(client, conf)
	assert.NotEqual(t, nil, err)

	// Trying to find dir they don't have access to?
	conf.TmpDir = "/home/fake"
	_, err = cloudstorage.NewStore(conf)
	assert.NotEqual(t, nil, err)

}

func TestUpload(t *testing.T) {

	if config.Bucket == "" {
		t.Logf("must provide BACKBLAZE_BUCKET, BACKBLAZE_ACCOUNT,  BACKBLAZE_KEY  env vars")
		t.Skip()
		return
	}

	conf := &cloudstorage.Config{
		Type:       backblaze.StoreType,
		Bucket:     os.Getenv("BACKBLAZE_BUCKET"),
		AuthMethod: backblaze.AuthKey,
		TmpDir:     "/tmp/localcache/backblaze",
		Settings:   make(gou.JsonHelper),
	}

	conf.AuthMethod = backblaze.AuthKey
	conf.Settings[backblaze.Account] = os.Getenv("BACKBLAZE_ACCOUNT")
	conf.Settings[backblaze.Key] = os.Getenv("BACKBLAZE_KEY")

	store, err := cloudstorage.NewStore(conf)
	assert.Equal(t, nil, err)

	obj, err := store.NewObject("xyz")
	assert.Equal(t, nil, err)

	file, err := obj.Open(cloudstorage.ReadWrite)
	assert.Equal(t, nil, err)

	writer := bufio.NewWriter(file)
	writer.WriteString("My single little line file")
	writer.Flush()

	obj.Close()

}

func TestDownload(t *testing.T) {

	if config.Bucket == "" {
		t.Logf("must provide BACKBLAZE_BUCKET, BACKBLAZE_ACCOUNT,  BACKBLAZE_KEY  env vars")
		t.Skip()
		return
	}

	conf := &cloudstorage.Config{
		Type:       backblaze.StoreType,
		Bucket:     os.Getenv("BACKBLAZE_BUCKET"),
		AuthMethod: backblaze.AuthKey,
		TmpDir:     "/tmp/localcache/backblaze",
		Settings:   make(gou.JsonHelper),
	}

	conf.AuthMethod = backblaze.AuthKey
	conf.Settings[backblaze.Account] = os.Getenv("BACKBLAZE_ACCOUNT")
	conf.Settings[backblaze.Key] = os.Getenv("BACKBLAZE_KEY")

	store, err := cloudstorage.NewStore(conf)
	assert.Equal(t, nil, err)

	obj, err := store.NewObject("xyz")
	assert.Equal(t, nil, err)
	defer obj.Close()

	file, err := obj.Open(cloudstorage.ReadWrite)
	assert.Equal(t, nil, err)
	defer file.Close()

	bytes, err := ioutil.ReadAll(file)
	assert.Equal(t, nil, err)

	err = ioutil.WriteFile("/tmp/dat1", bytes, 0644)
	assert.Equal(t, nil, err)

}
