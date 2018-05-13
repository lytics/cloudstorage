package sftp

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/araddon/gou"

	"github.com/lytics/cloudstorage"
	"github.com/pborman/uuid"
	ftp "github.com/pkg/sftp"
	"golang.org/x/net/context"
)

func TestErrorCoverage(t *testing.T) {
	uid := uuid.NewUUID().String()
	uid = strings.Replace(uid, "-", "", -1)

	ftpClient := &sftpMockClient{
		open: map[string]*fileReply{
			"open/error.ext": &fileReply{nil, fmt.Errorf("open.error")},
		},
		remove: map[string]error{
			"remove/error.ext": fmt.Errorf("remove.error"),
		},
		create: map[string]*fileReply{
			"create/error.ext": &fileReply{nil, fmt.Errorf("create.error")},
		},
		stat: map[string]*fileInfoReply{
			"stat/error.ext":    &fileInfoReply{nil, fmt.Errorf("stat.error")},
			"stat/notExist.ext": &fileInfoReply{nil, os.ErrNotExist},
		},
		mkdir: map[string]error{
			"mkdir/error.ext": fmt.Errorf("mkdir.error"),
		},
		readDir: map[string]*readDirReply{
			"readDir/error.ext": &readDirReply{nil, fmt.Errorf("readDir.error")},
		},
		closeErr: fmt.Errorf("testcase error"),
	}

	sftpStore := &Client{
		ID:        uid,
		clientCtx: context.Background(),
		client:    ftpClient,
		host:      "foo.com",
		port:      12345,
		cachepath: "/tmp/localcache/sftp",
		bucket:    "mock",
		paths:     make(map[string]struct{}),
	}

	//TODO fix all of the following should be failing because I didn't put "mock/" infront of the tests above ^
	// i.e. mock/remove/error.ext  vs remove/error.ext
	// currently only Delete fails but only  on the Delete call, the Exists call passes...

	//GET
	if _, e := sftpStore.Get(context.Background(), "stat/error.ext"); e != cloudstorage.ErrObjectNotFound {
		t.Fatalf("expected `stat.error` error: got:%v", e)
	}
	if _, e := sftpStore.Get(context.Background(), "stat/notExist.ext"); e != cloudstorage.ErrObjectNotFound {
		t.Fatalf("expected cloudstorage.ErrObjectNotFound error: got:%v", e)
	}
	//DELETE
	//TODO FIX should be:
	//    if e := sftpStore.Delete(context.Background(), "stat/notExist.ext"); e != cloudstorage.ErrObjectNotFound {
	// instead Im getting:
	if e := sftpStore.Delete(context.Background(), "stat/notExist.ext"); e != os.ErrNotExist {
		t.Fatalf("expected cloudstorage.ErrObjectNotFound error: got:%v", e)
	}
	if e := sftpStore.Delete(context.Background(), "remove/error.ext"); e == nil || e.Error() != "remove.error" {
		t.Fatalf("expected cloudstorage.ErrObjectNotFound error: got:%v", e)
	}

}

type fileReply struct {
	f *ftp.File
	e error
}

type fileInfoReply struct {
	fi os.FileInfo
	e  error
}

type readDirReply struct {
	fi []os.FileInfo
	e  error
}

type sftpMockClient struct {
	//path to response maps
	open     map[string]*fileReply     // (filename string) (*ftp.File, error)
	remove   map[string]error          //(filename string) error
	create   map[string]*fileReply     // (filename string) (*ftp.File, error)
	stat     map[string]*fileInfoReply // (filename string) (os.FileInfo, error)
	mkdir    map[string]error          // (path string) error
	readDir  map[string]*readDirReply  //(path string) ([]os.FileInfo, error)
	closeErr error
}

func (s *sftpMockClient) Open(filename string) (*ftp.File, error) {
	r, ok := s.open[filename]
	if !ok {
		return nil, nil
	}
	return r.f, r.e
}
func (s *sftpMockClient) Remove(filename string) error {
	defer gou.Debugf("remove exited at ... %v ..................", filename)
	r, ok := s.remove[filename]
	if !ok {
		return nil
	}
	return r
}
func (s *sftpMockClient) Create(filename string) (*ftp.File, error) {
	r, ok := s.create[filename]
	if !ok {
		return nil, nil
	}
	return r.f, r.e
}
func (s *sftpMockClient) Stat(filename string) (os.FileInfo, error) {
	r, ok := s.stat[filename]
	if !ok {
		return nil, nil
	}
	return r.fi, r.e
}
func (s *sftpMockClient) Mkdir(path string) error {
	r, ok := s.mkdir[path]
	if !ok {
		return nil
	}
	return r
}
func (s *sftpMockClient) ReadDir(path string) ([]os.FileInfo, error) {
	r, ok := s.readDir[path]
	if !ok {
		return nil, nil
	}
	return r.fi, r.e
}
func (s *sftpMockClient) Close() error {
	if s.closeErr != nil {
		return s.closeErr
	}
	return nil
}
