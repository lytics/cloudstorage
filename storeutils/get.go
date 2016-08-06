package storeutils

import (
	"bytes"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/lytics/cloudstorage/logging"
	"golang.org/x/net/context"
)

// Gets a single object's bytes based on bucket and name parameters
func GetObject(gc *storage.Client, bucket, name string) (*bytes.Buffer, error) {
	// Get buckethandler
	gsbh := gc.Bucket(bucket)

	// Create Query
	q := storage.Query{Prefix: name, MaxResults: 1}

	// Get list of *the* object
	ol, err := List(gsbh, q)
	if err != nil {
		return nil, err
	}

	buff, errs := OpenObject(ol, gsbh)
	if buff == nil {
		if len(errs) >= 0 {
			i := 0
			errBuff := bytes.NewBufferString("GetObject Errors:\n")
			for _, e := range errs {
				i++
				errBuff.WriteString(e.Error())
				errBuff.WriteString("\n")
			}
			err := fmt.Errorf("%s", errBuff.String())
			return nil, err
		} else {
			return nil, fmt.Errorf("GetObject recieved nil byte buffer and no errors")
		}
	} else {
		//success
		return buff, nil
	}
}

// Opens the first object returned in a storage.ObjectList
// returns contents via byte Buffer
func OpenObject(objects *storage.ObjectList, gcsb *storage.BucketHandle) (*bytes.Buffer, []error) {
	var buff *bytes.Buffer = bytes.NewBuffer([]byte{})
	log := logging.NewStdLogger(true, 4, "OpenObject")
	var googleObject *storage.ObjectAttrs
	errs := make([]error, 0)

	for try := 0; try < GCSRetries; try++ {
		if objects.Results != nil && len(objects.Results) != 0 {
			googleObject = objects.Results[0]
		}

		if googleObject != nil {
			//we have a preexisting object, so lets download it..
			rc, err := gcsb.Object(googleObject.Name).NewReader(context.Background())
			if err != nil {
				errs = append(errs, fmt.Errorf("error storage.NewReader of %s err=%v", googleObject.Name, err))
				log.Debugf("%v", errs)
				backoff(try)
				continue
			}
			defer rc.Close()

			_, err = io.Copy(buff, rc)
			if err != nil {
				errs = append(errs, fmt.Errorf("error coping bytes. err=%v", err))
				log.Debugf("%v", errs)
				backoff(try)
				continue
			}
		}
		return buff, nil
	}
	errs = append(errs, fmt.Errorf("OpenObject errors past limit!"))
	return nil, errs
}
