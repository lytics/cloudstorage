package awss3_test

import (
	"os"
	"testing"

	"github.com/araddon/gou"

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
func init() {
	gou.SetupLogging("debug")
	gou.SetColorOutput()
}

var config = &cloudstorage.Config{
	Type:       awss3.StoreType,
	AuthMethod: awss3.AuthAccessKey,
	Bucket:     os.Getenv("AWS_BUCKET"),
	TmpDir:     "/tmp/localcache/aws",
	Settings:   make(gou.JsonHelper),
}

func TestAll(t *testing.T) {
	config.Settings[awss3.ConfKeyAccessKey] = os.Getenv("AWS_ACCESS_KEY")
	config.Settings[awss3.ConfKeyAccessSecret] = os.Getenv("AWS_SECRET_KEY")
	//gou.Debugf("config %v", config)
	store, err := cloudstorage.NewStore(config)
	if err != nil {
		t.Logf("No valid auth provided, skipping awss3 testing %v", err)
		t.Skip()
		return
	}
	if store == nil {
		t.Fatalf("No store???")
	}
	testutils.RunTests(t, store)
}
