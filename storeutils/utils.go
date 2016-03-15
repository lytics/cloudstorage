package storeutils

import "github.com/lytics/cloudstorage"

//GetAndOpen is a convenience method that combines Store.Get() and Object.Open() into
// a single call.
func GetAndOpen(s cloudstorage.Store, o string, level cloudstorage.AccessLevel) (cloudstorage.Object, error) {
	obj, err := s.Get(o)
	if err == cloudstorage.ObjectNotFound {
		return nil, cloudstorage.ObjectNotFound
	} else if err != nil {
		return nil, err
	}

	_, err = obj.Open(level)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

//WriteObject convenience method like ioutil.WriteFile
//  The object will be created if it doesn't already exists.
//  If the object does exists it will be overwritten.
func WriteObject(s cloudstorage.Store, o string, meta map[string]string, b []byte) error {
	panic("not implemented")
}

type ObjectIterator interface {
	Next() cloudstorage.Object
	HasNext() bool
}
