package sftp

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/araddon/gou"
	u "github.com/araddon/gou"
	"github.com/pborman/uuid"
	ftp "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/net/context"

	"github.com/lytics/cloudstorage"
)

const (
	// StoreType = "sftp" this is used to define the storage type to create
	// from cloudstorage.NewStore(config)
	StoreType = "sftp"

	timeout = 5 * time.Minute
	// Required config variables
	userpassVars = "user password host port"
	userkeyVars  = "user privatekey host port"

	AuthUserKey  cloudstorage.AuthMethod = "userkey"
	AuthUserPass cloudstorage.AuthMethod = "userpass"

	// ConfKeyUser config key name of the username
	ConfKeyUser = "user"
	// ConfKeyPassword config key name of the password
	ConfKeyPassword = "password"
	// ConfKeyPrivateKey config key name of the privatekey
	ConfKeyPrivateKey = "privatekey"
	// ConfKeyHost config key name of the server host
	ConfKeyHost = "host"
	// ConfKeyPort config key name of the sftp port
	ConfKeyPort = "port"
	// ConfKeyFolder config key name of the sftp folder
	ConfKeyFolder = "folder"
)

type (
	/*
		Store interface {
			Open(prefix, filename string) (io.ReadCloser, error)
			NewFile(filename string) (Uploader, error)
			Remove(filename string) error
			Rename(old, new string) error
			Exists(filename string) bool
			Files(folder string) ([]os.FileInfo, error)
			ListFiles(folder string, hidden bool) ([]string, error)
			ListDirs(folder string, hidden bool) ([]string, error)
			Cd(dir string)
			FilesAfter(t time.Time) ([]os.FileInfo, error)
			Close()
		}
	*/
	Uploader interface {
		Upload(io.Reader) (int64, error)
	}
	// Client is the sftp client
	Client struct {
		ID        string
		clientCtx context.Context
		client    *ftp.Client
		cachepath string
		host      string
		port      int
		bucket    string
		files     []string
		paths     map[string]struct{}
	}

	// File represents sftp File
	object struct {
		client     *Client
		file       *ftp.File
		cachedcopy *os.File
		fi         os.FileInfo
		name       string
		readonly   bool
		opened     bool
		cachepath  string
		//updated    time.Time
		//metadata   map[string]string
		//infoOnce   sync.Once
		//infoErr    error
	}
)

func init() {
	// Register this Driver (s3) in cloudstorage driver registry.
	cloudstorage.Register(StoreType, func(conf *cloudstorage.Config) (cloudstorage.Store, error) {
		ctx := context.Background()
		if conf.LogPrefix != "" {
			ctx = gou.NewContext(ctx, conf.LogPrefix)
		}
		client, err := NewClientFromConfig(ctx, conf)
		if err != nil {
			return nil, err
		}
		return client, nil
	})
}

// NewClientFromConfig validates configuration then creates new client from token
func NewClientFromConfig(clientCtx context.Context, conf *cloudstorage.Config) (*Client, error) {

	var sshConfig *ssh.ClientConfig
	var err error

	switch conf.AuthMethod {
	case AuthUserKey: //"userkey"
		sshConfig, err = ConfigUserKey(conf.Settings.String(ConfKeyUser), conf.Settings.String(ConfKeyPrivateKey))
		if err != nil {
			gou.WarnCtx(clientCtx, "error configuring private key %v", err)
			return nil, err
		}
	case AuthUserPass: //"userpass"
		sshConfig = ConfigUserPass(conf.Settings.String(ConfKeyUser), conf.Settings.String(ConfKeyPassword))
	default:
		err := fmt.Errorf("invalid config.AuthMethod %q", conf.AuthMethod)
		gou.WarnCtx(clientCtx, "%v", err)
		return nil, err
	}

	// optional
	host := conf.Settings.String(ConfKeyHost)
	folder := conf.Settings.String(ConfKeyFolder)
	port := conf.Settings.Int(ConfKeyPort)

	return NewClient(clientCtx, conf, host, port, folder, sshConfig)
}

// NewClient returns a new SFTP Client
// Make sure to close SFTP connection when done
func NewClient(clientCtx context.Context, conf *cloudstorage.Config, host string, port int, folder string, config *ssh.ClientConfig) (*Client, error) {

	//u.Debugf("new sftp host=%q port=%d folder=%q", host, port, folder)
	target, err := sftpAddr(host, port)
	if err != nil {
		gou.WarnCtx(clientCtx, "failed creating address with %s, %d: %v", host, port, err)
		return nil, err
	}

	sshClient, err := ssh.Dial("tcp", target, config)
	if err != nil {
		gou.WarnCtx(clientCtx, "failed SFTP login for %s with error %s", config.User, err)
		return nil, err
	}

	ftpClient, err := ftp.NewClient(sshClient)
	if err != nil {
		gou.WarnCtx(clientCtx, "failed creating SFTP client for %s with error %s", config.User, err)
		sshClient.Close()
		return nil, err
	}

	uid := uuid.NewUUID().String()
	uid = strings.Replace(uid, "-", "", -1)

	client := &Client{
		ID:        uid,
		clientCtx: clientCtx,
		client:    ftpClient,
		host:      host,
		port:      port,
		cachepath: conf.TmpDir,
		bucket:    folder,
		paths:     make(map[string]struct{}),
	}

	//gou.Infof("%p created sftp client %#v", client, ftpClient)

	return client, nil
}

// ConfigUserPass creates ssh config with user/password
// HostKeyCallback was added here
// https://github.com/golang/crypto/commit/e4e2799dd7aab89f583e1d898300d96367750991
// currently we don't check hostkey, but in the future (todo) we could store the hostkey
// and check on future logins if there is a match.
func ConfigUserPass(user, password string) *ssh.ClientConfig {
	return &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		Config: ssh.Config{
			Ciphers: []string{
				"aes128-ctr", "aes192-ctr", "aes256-ctr", "aes128-gcm@openssh.com",
				"arcfour256", "arcfour128", "aes128-cbc",
			},
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         timeout,
	}
}

// ConfigUserKey creates ssh config with ssh/private rsa key
func ConfigUserKey(user, keyString string) (*ssh.ClientConfig, error) {
	// Decode the RSA private key
	key, err := ssh.ParsePrivateKey([]byte(keyString))
	if err != nil {
		return nil, fmt.Errorf("bad private key: %s", err)
	}

	return &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(key),
		},
		Config: ssh.Config{
			Ciphers: []string{
				"aes128-ctr", "aes192-ctr", "aes256-ctr", "aes128-gcm@openssh.com",
				"arcfour256", "arcfour128", "aes128-cbc",
			},
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         timeout,
	}, nil
}

// Type of store = "sftp"
func (m *Client) Type() string {
	return StoreType
}

// Client return underlying client
func (s *Client) Client() interface{} {
	return s
}

func (s *Client) String() string {
	return fmt.Sprintf("<sftp host=%q />", s.host)
}

// NewObject create a new object with given name.  Will not write to remote
// sftp until Close is called.
func (s *Client) NewObject(objectname string) (cloudstorage.Object, error) {
	obj, err := s.Get(context.Background(), objectname)
	if err != nil && err != cloudstorage.ErrObjectNotFound {
		return nil, err
	} else if obj != nil {
		return nil, cloudstorage.ErrObjectExists
	}

	cf := cloudstorage.CachePathObj(s.cachepath, objectname, s.ID)
	//gou.DebugCtx(s.clientCtx, "new object cf = %q", cf)

	return &object{
		client: s,
		name:   objectname,
		//metadata: map[string]string{cloudstorage.ContentTypeKey: cloudstorage.ContentType(objectname)},
		//bucket:     s.bucket,
		cachedcopy: nil,
		cachepath:  cf,
	}, nil
}

// Get opens a file for read or writing
func (s *Client) Get(ctx context.Context, name string) (cloudstorage.Object, error) {
	if !s.Exists(name) {
		return nil, cloudstorage.ErrObjectNotFound
	}
	get := Concat(s.bucket, name)
	//gou.DebugCtx(s.clientCtx, "getting file %s", get)
	f, err := s.client.Stat(get)
	if err != nil {
		return nil, err
	}
	return newObjectFromFile(s, get, f), nil
}

// Open opens a file for read or writing
func (s *Client) Open(prefix, filename string) (io.ReadCloser, error) {
	fn := Concat(prefix, filename)
	if !s.Exists(fn) {
		return nil, os.ErrNotExist
	}
	get := Concat(s.bucket, fn)
	gou.InfoCtx(s.clientCtx, "getting file %q", get)

	return s.client.Open(get)
}

// Objects returns an iterator over the objects in the google bucket that match the Query q.
// If q is nil, no filtering is done.
func (m *Client) Objects(ctx context.Context, q cloudstorage.Query) (cloudstorage.ObjectIterator, error) {
	return cloudstorage.NewObjectPageIterator(ctx, m, q), nil
}

// Delete deletes a file
func (s *Client) Delete(ctx context.Context, filename string) error {
	if !s.Exists(filename) {
		return os.ErrNotExist
	}
	r := Concat(s.bucket, filename)
	//gou.InfoCtx(s.clientCtx, "removing file %q", r)
	return s.client.Remove(r)
}

// Rename renames a file
func (s *Client) Rename(oldname, newname string) error {
	if !s.Exists(oldname) {
		return os.ErrNotExist
	}
	o := Concat(s.bucket, oldname)
	n := Concat(s.bucket, newname)

	gou.InfoCtx(s.clientCtx, "renaming file %q to %q", o, n)

	return s.client.Rename(o, n)
}

// Exists checks to see if files exists
func (s *Client) Exists(filename string) bool {
	folder := ""
	if i := strings.LastIndex(filename, "/"); i > 0 {
		folder = filename[:i]
		filename = filename[i+1:]
	}
	// TODO:  is there a more efficient way of getting single file existence?
	// i think we should move to .Stat()
	files, _ := s.ListFiles(folder, true)
	for _, f := range files {
		if f == filename {
			return true
		}
	}
	return false
}

func (s *Client) ensureDir(name string) {

	//u.Infof("bucket = %q", s.bucket)
	name = Concat(s.bucket, name)
	parts := strings.Split(strings.ToLower(name), "/")
	dir := ""
	for _, dirPart := range parts[0 : len(parts)-1] {
		if dir == "" {
			dir = dirPart
		} else {
			dir = strings.Join([]string{dir, dirPart}, "/")
		}
		if _, exists := s.paths[dir]; exists {
			continue
		}

		_, err := s.client.Stat(dir)
		if err != nil && strings.Contains(err.Error(), "not exist") {
			if err = s.client.Mkdir(dir); err != nil {
				u.Warn("Could not create directory for ftp", dir, err)
			}
		}
		s.paths[dir] = struct{}{}
	}
}

// List lists files in a directory
func (m *Client) List(ctx context.Context, q cloudstorage.Query) (*cloudstorage.ObjectsResponse, error) {

	objs := &cloudstorage.ObjectsResponse{
		Objects: make(cloudstorage.Objects, 0),
	}

	err := m.listFiles(ctx, q, objs, m.bucket)
	if err != nil {
		u.Warnf("fetch error %v", err)
		return nil, err
	}
	objs.Objects = q.ApplyFilters(objs.Objects)

	return objs, nil
}
func (m *Client) listFiles(ctx context.Context, q cloudstorage.Query, objs *cloudstorage.ObjectsResponse, path string) error {
	fil, err := m.fetchFiles(path)
	if err != nil {
		u.Warnf("fetch error %v %v", path, err)
		return err
	}
	name := ""
	for _, fi := range fil {
		if fi.IsDir() {
			// u.Debugf("is dir %v", dir)
			err = m.listFiles(ctx, q, objs, strings.Join([]string{path, fi.Name()}, "/"))
			if err != nil {
				u.Errorf("could not get files %v  %v", fi.Name(), err)
				return err
			}
		} else {
			if strings.HasPrefix(path, "/") {
				name = Concat(path[1:], fi.Name())
			} else {
				name = Concat(path, fi.Name())
			}
			if q.Prefix != "" && !strings.HasPrefix(name, q.Prefix) {
				continue
			}
			//u.Debugf("%v", name)
			objs.Objects = append(objs.Objects, newObjectFromFile(m, name, fi))
		}
	}
	return nil
}

/*
// Files lists files as os.FileInfo in a directory
func (s *Client) Files(folder string) ([]os.FileInfo, error) {
	fi, err := s.fetchFiles(folder)
	if err != nil {
		return nil, err
	}

	var files []os.FileInfo
	for _, f := range fi {
		if f.IsDir() {
			fid, err := s.Files(f.Name())
			if err != nil {
				u.Errorf("could not get files %v", err)
				return nil, err
			}
			files = append(files, fid...)
		} else {
			files = append(files, f)
		}
	}
	return files, nil
}
*/

// ListFiles lists files in a directory
func (s *Client) ListFiles(folder string, hidden bool) ([]string, error) {
	return s.filterFileNames(folder, false, true, hidden)
}

// Folders lists directories in a directory
func (s *Client) Folders(ctx context.Context, q cloudstorage.Query) ([]string, error) {
	return s.listDirs(q.Prefix, "", q.ShowHidden)
}

func (s *Client) listDirs(folder, prefix string, hidden bool) ([]string, error) {
	dirs, err := s.filterFileNames(folder, true, false, hidden)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, d := range dirs {
		f := Concat(folder, d)
		p := Concat(prefix, d)
		out = append(out, p)
		if ds, err := s.listDirs(f, p, hidden); err == nil {
			out = append(out, ds...)
		}
	}
	return out, nil
}

/*
// MkDir creates new folder in base dir
func (s *Client) MkDir(dir string) error {
	dirs, err := s.listDirs("", "", false)
	if err != nil {
		gou.WarnCtx(s.clientCtx, "error listing dirs: %v", err)
		return err
	}
	for _, d := range dirs {
		if d == dir {
			return nil
		}
	}
	return s.client.Mkdir(Concat(s.Folder, dir))
}

// Cd changes the base dir
func (s *Client) Cd(dir string) {
	s.Folder = Concat(s.Folder, dir)
}

func (s *Client) FilesAfter(t time.Time) ([]os.FileInfo, error) {
	fi, err := s.fetchFiles("")
	if err != nil {
		return nil, err
	}
	sort.Sort(ByModTime(fi))

	var files []os.FileInfo
	for _, f := range fi {
		if f.IsDir() {
			continue
		}
		if strings.Index(f.Name(), ".") != 0 && f.ModTime().After(t) {
			files = append(files, f)
		}
	}

	return files, nil
}
*/
// Close closes underlying client connection
func (s *Client) Close() {
	s.client.Close()
}

// NewReader create file reader.
func (s *Client) NewReader(o string) (io.ReadCloser, error) {
	return s.NewReaderWithContext(context.Background(), o)
}

// NewReaderWithContext create new File reader with context.
func (s *Client) NewReaderWithContext(ctx context.Context, name string) (io.ReadCloser, error) {
	if !s.Exists(name) {
		return nil, cloudstorage.ErrObjectNotFound
	}
	get := Concat(s.bucket, name)
	gou.InfoCtx(s.clientCtx, "getting file %s", get)
	f, err := s.client.Open(get)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// NewWriter create Object Writer.
func (s *Client) NewWriter(objectName string, metadata map[string]string) (io.WriteCloser, error) {
	return s.NewWriterWithContext(context.Background(), objectName, metadata)
}

// NewWriterWithContext create writer with provided context and metadata.
func (s *Client) NewWriterWithContext(ctx context.Context, name string, metadata map[string]string) (io.WriteCloser, error) {

	name = strings.Replace(name, " ", "+", -1)

	// pr, pw := io.Pipe()
	// bw := csbufio.NewWriter(pw)

	//o := &object{name: name}
	o, err := s.NewObject(name)
	if err != nil {
		return nil, err
	}

	if _, err = o.Open(cloudstorage.ReadWrite); err != nil {
		u.Errorf("could not open %v %v", name, err)
		return nil, err
	}
	return o, nil
}

/*
// NewFile creates file with filename in upload folder
func (s *Client) NewFile(filename string) (Uploader, error) {
	moveto := Concat(s.Folder, filename)
	gou.InfoCtx(s.clientCtx, "creating file %s", moveto)

	file, err := s.client.Create(moveto)
	if err != nil {
		return nil, err
	}

	return &object{file: file}, nil
}
*/
func (s *Client) fetchFiles(f string) ([]os.FileInfo, error) {
	folder := Concat(s.bucket, f)
	if folder == "" {
		folder = "."
	}
	fi, err := s.client.ReadDir(folder)
	if err != nil {
		if err == os.ErrNotExist {
			return nil, cloudstorage.ErrObjectNotFound
		}
		gou.WarnCtx(s.clientCtx, "failed to read directory %q with error: %v", s.bucket, err)
		return nil, err
	}
	return fi, nil
}

func (s *Client) filterFileNames(folder string, dirs, files, hidden bool) ([]string, error) {
	fi, err := s.fetchFiles(folder)
	if err != nil {
		return nil, err
	}

	return filterFiles(fi, dirs, files, hidden), nil
}

func newObjectFromFile(c *Client, name string, f os.FileInfo) *object {
	cf := cloudstorage.CachePathObj(c.cachepath, name, c.ID)
	//gou.Debugf("cachepath = %v", cf)
	return &object{
		client:    c,
		fi:        f,
		name:      name,
		cachepath: cf,
	}
}

// Upload copies reader body bytes into underlying sftp file
func (o *object) upload(body io.Reader) (int64, error) {

	o.client.ensureDir(o.name)

	name := Concat(o.client.bucket, o.name)

	//gou.Infof("upload %q", name)

	if o.file != nil {
		if err := o.file.Close(); err != nil {
			gou.Warnf("error closing %q %v", name, err)
		}
		// TODO:  Should we rename?  two-phase commit this?  if we do do we run
		// risk of having a list operation find it?  use folders?
		err := o.client.client.Remove(name)
		if err != nil {
			gou.Warnf("error removing %v", err)
			return 0, err
		}
		o.file = nil
		gou.Debugf("just removed %v to upload a new version", name)
	}

	//gou.Infof("client %p %#v", o.client, o.client)
	//gou.Infof("client %#v", o.client.client)
	f, err := o.client.client.Create(name)
	if err != nil {
		gou.Warnf("Could not create file %q err=%v", name, err)
		return 0, err
	}

	defer f.Close()

	wLength, err := f.ReadFrom(body)
	if err != nil {
		gou.Errorf("could not read file %v", err)
		return 0, err
	}

	// gou.Debugf("%p uploaded %q size=%d", body, name, wLength)
	return wLength, nil
}

func (o *object) Write(p []byte) (n int, err error) {
	return o.cachedcopy.Write(p)
}

func statinfo(msg, name string) {
	fi, err := os.Stat(name)
	if err != nil {
		//gou.Errorf("could not stat %q %v", name, err)
		return
	}
	//gou.LogD(4, gou.DEBUG, fmt.Sprintf("stat: %s   %+v  mode=%v", msg, fi, fi.Mode().String()))
	gou.LogD(4, gou.DEBUG, fmt.Sprintf("stat: %s %s size=%d mode=%v", msg, fi.Name(), fi.Size(), fi.Mode().String()))
}

// Open ensures the file is available for read/write (or accessevel)
func (o *object) Open(accesslevel cloudstorage.AccessLevel) (*os.File, error) {

	if o.opened {
		return nil, fmt.Errorf("the store object is already opened. %s", o.cachepath)
	}

	readonly := accesslevel == cloudstorage.ReadOnly

	err := cloudstorage.EnsureDir(o.cachepath)
	if err != nil {
		return nil, fmt.Errorf("could not create cachedcopy's dir. cachepath=%q err=%v", o.cachepath, err)
	}

	//statinfo("About to do first open() os.Create()", o.cachepath)
	//os.Remove(o.cachepath)
	cachedcopy, err := os.OpenFile(o.cachepath, os.O_RDWR|os.O_CREATE, 0665)
	if err != nil {
		return nil, fmt.Errorf("could not open cachedcopy file. cachepath=%q err=%v", o.cachepath, err)
	}
	//statinfo("About to do AFTER open() os.Create()", o.cachepath)

	// if readonly {
	// 	cachedcopy.Close()
	// 	cachedcopy, err = os.Open(o.cachepath)
	// 	gou.Debugf("just opened readonly?  %q?", o.cachepath)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("sftp: error occurred opening file. storepath=%s tfile=%v err=%v",o.cachepath, cachedcopy.Name(), err)
	// 	}
	// }

	if o.file != nil {
		//gou.Debugf("has file so copy to local cached copy")
		_, err = io.Copy(cachedcopy, o.file)
		if err != nil {
			return nil, err
		}
	} else if o.fi == nil {
		// this is a new file
		//gou.Debugf("new file ")
		//statinfo("new file statinfo", o.cachepath)
	} else if o.fi != nil {
		// existing file
		get := Concat(o.client.bucket, o.name)
		//gou.Debugf("existingfile, open %s", get)
		f, err := o.client.client.Open(get)
		if err != nil {
			gou.WarnCtx(o.client.clientCtx, "Could not get %q err=%v", get, err)
			return nil, err
		}
		o.file = f

		_, err = io.Copy(cachedcopy, f)
		if err != nil {
			gou.WarnCtx(o.client.clientCtx, "Could not copy %q err=%v", o.name, err)
			return nil, err
		}
		cachedcopy.Close()
		//statinfo("after close/iotutil readall", o.cachepath)

		cachedcopy, err = os.OpenFile(o.cachepath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0665)
		if err != nil {
			gou.Error(err)
			return nil, err
		}
	}

	o.cachedcopy = cachedcopy
	o.readonly = readonly
	o.opened = true

	// o.cachedcopy.Sync()
	// o.cachedcopy.Close()
	//gou.Debugf("opened %s", o.cachepath)
	//gou.Infof("Open() returning cache copy %p", o.cachedcopy)
	return o.cachedcopy, nil

	//return nil, fmt.Errorf("fetch error retry cnt reached: obj=%s tfile=%v", o.name, o.cachepath)
}

func (o *object) Read(p []byte) (n int, err error) {
	return o.cachedcopy.Read(p)
}

// Delete delete the underlying object from ftp server.
func (o *object) Delete() error {
	// this should be path/name ??
	// gou.Debugf("Delete name=%q  sftp.Name()=%q", o.name, o.fi.Name())
	return o.client.Delete(context.Background(), o.name)
}

func (o *object) Sync() error {

	if !o.opened {
		return fmt.Errorf("object isn't opened object:%s", o.name)
	}
	if o.readonly {
		return fmt.Errorf("trying to Sync a readonly object:%s", o.name)
	}
	if o.cachedcopy == nil {
		return fmt.Errorf("No cached copy")
	}

	//statinfo("about to sync cachecopy", o.cachepath)
	if err := o.cachedcopy.Sync(); err != nil {
		if !strings.Contains(err.Error(), "already closed") {
			gou.Warnf("%v", err)
			return err
		}
	}

	//gou.Warnf("%#v   %s", o.cachedcopy, o.cachedcopy.Name())
	//gou.Infof("about to close cache copy %p", o.cachedcopy)
	if err := o.cachedcopy.Close(); err != nil {
		if !strings.Contains(err.Error(), "already closed") {
			gou.Warnf("%v", err)
			return err
		}
	}

	//statinfo("about to upload cachecopy ", o.cachepath)
	cachedcopy, err := os.Open(o.cachepath)
	if err != nil {
		gou.Warnf("%v", err)
		return err
	}
	if cachedcopy == nil {
		gou.Warnf("damn, no object %q", o.cachepath)
	}
	_, err = o.upload(cachedcopy)
	if err != nil {
		gou.WarnCtx(o.client.clientCtx, "Could not upload %q err=%v", o.cachepath, err)
		return err
	}
	o.cachedcopy = cachedcopy
	//gou.DebugCtx(o.client.clientCtx, "Uploaded %q size=%d", o.name, size)
	return nil
}

func (o *object) Close() error {
	if !o.opened {
		return nil
	}
	defer func() {
		os.Remove(o.cachepath)
		o.cachedcopy = nil
		o.opened = false
	}()

	if o.opened && !o.readonly {
		err := o.Sync()
		if err != nil {
			u.Errorf("error on sync %v", err)
			return err
		}
	} else {
		gou.Warnf("not syncing on close? %v opened?%v  readonly?%v", o.name, o.opened, o.readonly)
	}

	err := o.cachedcopy.Close()
	if err != nil {
		return fmt.Errorf("error on sync and closing localfile. %q err=%v", o.cachepath, err)
	}

	return nil
}

func (o *object) Release() error {
	if o.cachedcopy != nil {
		o.cachedcopy.Close()
	}
	return os.Remove(o.cachepath)
}

func (o *object) MetaData() map[string]string {
	return nil
}
func (o *object) SetMetaData(meta map[string]string) {
	//o.metadata = meta
}
func (o *object) StorageSource() string {
	return StoreType
}
func (o *object) Name() string {
	return o.name
}
func (o *object) String() string {
	return o.name
}
func (o *object) Updated() time.Time {
	if o.fi != nil {
		return o.fi.ModTime()
	}
	return time.Time{}
}

type ByModTime []os.FileInfo

func (a ByModTime) Len() int      { return len(a) }
func (a ByModTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByModTime) Less(i, j int) bool {
	ti := a[i].ModTime()
	tj := a[j].ModTime()
	if ti.Equal(tj) {
		return strings.Compare(a[i].Name(), a[j].Name()) > 0
	}
	return ti.Before(tj)
}

func filterFiles(fi []os.FileInfo, dirs, files, hidden bool) []string {
	var out []string
	for _, f := range fi {
		if f.IsDir() && !dirs {
			continue
		}
		if !f.IsDir() && !files {
			continue
		}
		name := f.Name()
		if hidden || strings.Index(name, ".") != 0 {
			out = append(out, name)
		}
	}
	return out
}

// Concat concats strings with "/" but ignores empty strings
// so an input of "portland", "", would yield "portland"
// instead of "portland/"
func Concat(strs ...string) string {
	out := bytes.Buffer{}
	for _, s := range strs {
		if out.Len() == 0 {
			out.WriteString(s)
		} else if s != "" {
			out.WriteByte('/')
			out.WriteString(s)
		}
	}

	return string(out.Bytes())
}

// sftpAddr build sftp address
func sftpAddr(host string, port int) (string, error) {
	// remove things like ftp://
	if i := strings.Index(host, "://"); i >= 0 {
		host = host[(i + 3):]
	}

	// remove trailing :host
	if i := strings.Index(host, ":"); i >= 0 {
		host = host[:i]
	}

	if host == "" {
		return "", fmt.Errorf("host name not recognized %s", host)
	}

	if port <= 0 {
		return "", fmt.Errorf("port number must be greater than 0")
	}

	return fmt.Sprintf("%s:%v", host, port), nil
}
