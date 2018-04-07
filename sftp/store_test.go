package sftp_test

import (
	"os"
	"testing"

	"github.com/araddon/gou"

	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/sftp"
	"github.com/lytics/cloudstorage/testutils"
)

/*

# to use sftp tests ensure you have exported

export SFTP_USER="aaa"
export SFTP_PASSWORD="bbb"
export SFTP_FOLDER="bucket"

*/

var config = &cloudstorage.Config{
	Type:       sftp.StoreType,
	AuthMethod: sftp.AuthUserPass,
	Bucket:     os.Getenv("SFTP_FOLDER"),
	TmpDir:     "/tmp/localcache/sftp",
	Settings:   make(gou.JsonHelper),
	LogPrefix:  "sftp-testing",
}

func TestAll(t *testing.T) {
	config.Settings[sftp.ConfKeyUser] = os.Getenv("SFTP_USER")
	config.Settings[sftp.ConfKeyPassword] = os.Getenv("SFTP_PASSWORD")
	config.Settings[sftp.ConfKeyHost] = os.Getenv("SFTP_HOST")
	config.Settings[sftp.ConfKeyPort] = "22"
	//gou.Debugf("config %v", config)
	store, err := cloudstorage.NewStore(config)
	if err != nil {
		t.Logf("No valid auth provided, skipping sftp testing %v", err)
		t.Skip()
		return
	}
	if store == nil {
		t.Fatalf("No store???")
	}
	testutils.RunTests(t, store)
}
