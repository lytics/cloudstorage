package sftp_test

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/araddon/gou"
	"github.com/stretchr/testify/require"

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
	Settings:   make(gou.JsonHelper),
	LogPrefix:  "sftp-testing",
}

func getKey() string {
	privateKey, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		return ""
	}
	// pub, err := ssh.NewPublicKey(&privateKey.PublicKey)
	// if err != nil {
	// 	return ""
	// }
	// return string(ssh.MarshalAuthorizedKey(pub))
	//return string(x509.MarshalPKCS1PrivateKey(privateKey))
	privateKeyPEM := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)}
	buf := bytes.NewBuffer(nil)
	if err := pem.Encode(buf, privateKeyPEM); err != nil {
		return ""
	}
	return buf.String()
}
func TestConfig(t *testing.T) {
	tmpDir := t.TempDir()

	sshConf, err := sftp.ConfigUserKey("user", getKey())
	require.NoError(t, err)
	require.NotNil(t, sshConf)

	conf := &cloudstorage.Config{
		Type:       sftp.StoreType,
		AuthMethod: sftp.AuthUserKey,
		Bucket:     os.Getenv("SFTP_FOLDER"),
		TmpDir:     filepath.Join(tmpDir, "localcache", "sftp"),
		Settings:   make(gou.JsonHelper),
		LogPrefix:  "sftp-testing",
	}
	conf.Settings[sftp.ConfKeyPrivateKey] = getKey()
	conf.Settings[sftp.ConfKeyUser] = os.Getenv("SFTP_USER")
	conf.Settings[sftp.ConfKeyHost] = os.Getenv("SFTP_HOST")
	conf.Settings[sftp.ConfKeyPort] = "22"
	_, err = sftp.NewStore(conf)
	require.Error(t, err)
}
func TestAll(t *testing.T) {
	tmpDir := t.TempDir()
	config.TmpDir = filepath.Join(tmpDir, "localcache", "sftp")

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
	testutils.RunTests(t, store, config)
}
