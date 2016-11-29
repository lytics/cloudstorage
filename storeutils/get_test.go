package storeutils

import (
	"testing"
)

func TestGetObject(t *testing.T) {
	if testGetFile == "" {
		t.Skip("TESTFILE EnvVar must be set to run test")
	}
	gsc := Setup(t)

	buff, err := GetObject(gsc, testBucket, testGetFile)
	if err != nil {
		t.Errorf("Error reading file %s: %v", testGetFile, err)
	}
	str := buff.String()
	if len(str) == 0 {
		t.Errorf("No bytes read from GCS")
	}
	t.Logf("%s", str)
}
