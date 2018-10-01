package sftp

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/araddon/gou"
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
	cloudstorage.Register(StoreType, NewStore)
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

	gou.Infof("%p created sftp client %#v", client, ftpClient)

	return client, nil
}

func NewStore(conf *cloudstorage.Config) (cloudstorage.Store, error) {
	ctx := context.Background()
	if conf.LogPrefix != "" {
		ctx = gou.NewContext(ctx, conf.LogPrefix)
	}
	client, err := NewClientFromConfig(ctx, conf)
	if err != nil {
		return nil, err
	}
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
func (m *Client) Client() interface{} {
	return m.client
}

func (m *Client) String() string {
	return fmt.Sprintf("<sftp host=%q />", m.host)
}

// NewObject create a new object with given name.  Will not write to remote
// sftp until Close is called.
func (m *Client) NewObject(objectname string) (cloudstorage.Object, error) {
	obj, err := m.Get(context.Background(), objectname)
	if err != nil && err != cloudstorage.ErrObjectNotFound {
		return nil, err
	} else if obj != nil {
		return nil, cloudstorage.ErrObjectExists
	}

	cf := cloudstorage.CachePathObj(m.cachepath, objectname, m.ID)
	//gou.DebugCtx(m.clientCtx, "new object cf = %q", cf)

	return &object{
		client: m,
		name:   objectname,
		//metadata: map[string]string{cloudstorage.ContentTypeKey: cloudstorage.ContentType(objectname)},
		//bucket:     m.bucket,
		cachedcopy: nil,
		cachepath:  cf,
	}, nil
}

// Get opens a file for read or writing
func (m *Client) Get(ctx context.Context, name string) (cloudstorage.Object, error) {
	if !m.Exists(name) {
		return nil, cloudstorage.ErrObjectNotFound
	}
	get := Concat(m.bucket, name)
	//gou.DebugCtx(m.clientCtx, "getting file %s", get)
	f, err := m.client.Stat(get)
	if err != nil {
		return nil, err
	}
	return newObjectFromFile(m, get, f), nil
}

/*
// Open opens a file for read or writing
func (m *Client) Open(prefix, filename string) (io.ReadCloser, error) {
	fn := Concat(prefix, filename)
	if !m.Exists(fn) {
		return nil, os.ErrNotExist
	}
	get := Concat(m.bucket, fn)
	gou.InfoCtx(m.clientCtx, "getting file %q", get)

	return m.client.Open(get)
}
*/
// Objects returns an iterator over the objects in the google bucket that match the Query q.
// If q is nil, no filtering is done.
func (m *Client) Objects(ctx context.Context, q cloudstorage.Query) (cloudstorage.ObjectIterator, error) {
	return cloudstorage.NewObjectPageIterator(ctx, m, q), nil
}

// Delete deletes a file
func (m *Client) Delete(ctx context.Context, filename string) error {
	if !m.Exists(filename) {
		gou.Warnf("does not exist????? %q", filename)
		return os.ErrNotExist
	}
	r := Concat(m.bucket, filename)
	//gou.InfoCtx(m.clientCtx, "removing file %q", r)
	return m.client.Remove(r)
}

/*
// Rename renames a file
func (m *Client) Rename(oldname, newname string) error {
	if !m.Exists(oldname) {
		return os.ErrNotExist
	}
	o := Concat(m.bucket, oldname)
	n := Concat(m.bucket, newname)

	gou.InfoCtx(m.clientCtx, "renaming file %q to %q", o, n)

	return m.client.Rename(o, n)
}
*/
// Exists checks to see if files exists
func (m *Client) Exists(filename string) bool {
	_, err := m.client.Stat(filename)
	if err == nil {
		return true
	}
	if err == os.ErrNotExist {
		return false
	}
	gou.Warnf("could not stat? file=%s  err=%v", filename, err)
	return false
	/*
		// do we need this fallback?  i doubt it
		folder := ""
		if i := strings.LastIndex(filename, "/"); i > 0 {
			folder = filename[:i]
			filename = filename[i+1:]
		}

		files, _ := m.ListFiles(folder, true)
		for _, f := range files {
			if f == filename {
				return true
			}
		}
		return false
	*/
}

func (m *Client) ensureDir(name string) {

	name = Concat(m.bucket, name)
	parts := strings.Split(strings.ToLower(name), "/")
	dir := ""
	for _, dirPart := range parts[0 : len(parts)-1] {
		if dir == "" {
			dir = dirPart
		} else {
			dir = strings.Join([]string{dir, dirPart}, "/")
		}
		if _, exists := m.paths[dir]; exists {
			continue
		}

		_, err := m.client.Stat(dir)
		if err != nil && strings.Contains(err.Error(), "not exist") {
			if err = m.client.Mkdir(dir); err != nil {
				gou.Warn("Could not create directory for ftp", dir, err)
			}
		}
		m.paths[dir] = struct{}{}
	}
}

// List lists files in a directory
func (m *Client) List(ctx context.Context, q cloudstorage.Query) (*cloudstorage.ObjectsResponse, error) {

	objs := &cloudstorage.ObjectsResponse{
		Objects: make(cloudstorage.Objects, 0),
	}

	err := m.listFiles(ctx, q, objs, m.bucket)
	if err != nil {
		gou.Warnf("fetch listFiles error %v", err)
		return nil, err
	}
	objs.Objects = q.ApplyFilters(objs.Objects)
	return objs, nil
}

func (m *Client) listFiles(ctx context.Context, q cloudstorage.Query, objs *cloudstorage.ObjectsResponse, path string) error {
	fil, err := m.fetchFiles(path)
	if err != nil {
		gou.Warnf("fetch error %v %v", path, err)
		return err
	}
	name := ""
	for _, fi := range fil {
		if fi.IsDir() {
			err = m.listFiles(ctx, q, objs, strings.Join([]string{path, fi.Name()}, "/"))
			if err != nil {
				gou.Warnf("could not get files %v  %v", fi.Name(), err)
				return err
			}
		} else {

			if path == "" {
				name = fi.Name()
			} else if strings.HasPrefix(path, "/") {
				name = Concat(path[1:], fi.Name())
			} else {
				name = Concat(path, fi.Name())
			}
			if q.Prefix != "" && !strings.HasPrefix(name, q.Prefix) {
				continue
			}
			objs.Objects = append(objs.Objects, newObjectFromFile(m, name, fi))
		}
	}
	return nil
}

/*
// Files lists files as os.FileInfo in a directory
func (m *Client) Files(folder string) ([]os.FileInfo, error) {
	fi, err := m.fetchFiles(folder)
	if err != nil {
		return nil, err
	}

	var files []os.FileInfo
	for _, f := range fi {
		if f.IsDir() {
			fid, err := m.Files(f.Name())
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

/*
// ListFiles lists files in a directory
func (m *Client) ListFiles(folder string, hidden bool) ([]string, error) {
	return m.filterFileNames(folder, false, true, hidden)
}
*/
// Folders lists directories in a directory
func (m *Client) Folders(ctx context.Context, q cloudstorage.Query) ([]string, error) {
	return m.listDirs(ctx, q.Prefix, "", q.ShowHidden)
}

func (m *Client) listDirs(ctx context.Context, folder, prefix string, hidden bool) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	dirs, err := m.filterFileNames(folder, true, false, hidden)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, d := range dirs {
		out = append(out, fmt.Sprintf("%s/", path.Join(folder, d)))
	}
	return out, nil
}

/*
// MkDir creates new folder in base dir
func (m *Client) MkDir(dir string) error {
	dirs, err := m.listDirs("", "", false)
	if err != nil {
		gou.WarnCtx(m.clientCtx, "error listing dirs: %v", err)
		return err
	}
	for _, d := range dirs {
		if d == dir {
			return nil
		}
	}
	return m.client.Mkdir(Concat(m.Folder, dir))
}

// Cd changes the base dir
func (m *Client) Cd(dir string) {
	m.Folder = Concat(m.Folder, dir)
}

func (m *Client) FilesAfter(t time.Time) ([]os.FileInfo, error) {
	fi, err := m.fetchFiles("")
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
func (m *Client) Close() {
	m.client.Close()
}

// NewReader create file reader.
func (m *Client) NewReader(o string) (io.ReadCloser, error) {
	return m.NewReaderWithContext(context.Background(), o)
}

// NewReaderWithContext create new File reader with context.
func (m *Client) NewReaderWithContext(ctx context.Context, name string) (io.ReadCloser, error) {
	if !m.Exists(name) {
		return nil, cloudstorage.ErrObjectNotFound
	}
	get := Concat(m.bucket, name)
	gou.InfoCtx(m.clientCtx, "getting file %s", get)
	f, err := m.client.Open(get)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// NewWriter create Object Writer.
func (m *Client) NewWriter(objectName string, metadata map[string]string) (io.WriteCloser, error) {
	return m.NewWriterWithContext(context.Background(), objectName, metadata)
}

// NewWriterWithContext create writer with provided context and metadata.
func (m *Client) NewWriterWithContext(ctx context.Context, name string, metadata map[string]string) (io.WriteCloser, error) {

	name = strings.Replace(name, " ", "+", -1)

	//	NewWriter should override/truncate any existing file
	if m.Exists(name) {
		if err := m.Delete(ctx, name); err != nil {
			gou.Errorf("failed to delete existing file %v %v", name, err)
			return nil, err
		}
	}

	// pr, pw := io.Pipe()
	// bw := csbufio.NewWriter(pw)

	//o := &object{name: name}
	o, err := m.NewObject(name)
	if err != nil {
		return nil, err
	}

	if _, err = o.Open(cloudstorage.ReadWrite); err != nil {
		gou.Errorf("could not open %v %v", name, err)
		return nil, err
	}
	return o, nil
}

/*
// NewFile creates file with filename in upload folder
func (m *Client) NewFile(filename string) (Uploader, error) {
	moveto := Concat(m.Folder, filename)
	gou.InfoCtx(m.clientCtx, "creating file %s", moveto)

	file, err := m.client.Create(moveto)
	if err != nil {
		return nil, err
	}

	return &object{file: file}, nil
}
*/
func (m *Client) fetchFiles(f string) ([]os.FileInfo, error) {
	folder := Concat(m.bucket, f)
	if folder == "" {
		folder = "."
	}
	fi, err := m.client.ReadDir(folder)
	if err != nil {
		if err == os.ErrNotExist {
			return nil, cloudstorage.ErrObjectNotFound
		}
		gou.WarnCtx(m.clientCtx, "failed to read directory %q with error: %v", m.bucket, err)
		return nil, err
	}
	return fi, nil
}

func (m *Client) filterFileNames(folder string, dirs, files, hidden bool) ([]string, error) {
	fi, err := m.fetchFiles(folder)
	if err != nil {
		return nil, err
	}

	return filterFiles(fi, dirs, files, hidden), nil
}

func newObjectFromFile(c *Client, name string, f os.FileInfo) *object {
	name = strings.TrimLeft(name, "/")
	cf := cloudstorage.CachePathObj(c.cachepath, name, c.ID)
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
	//gou.Infof("sftp object.Open(%q) readonly?%v", o.name, readonly)

	err := cloudstorage.EnsureDir(o.cachepath)
	if err != nil {
		return nil, fmt.Errorf("could not create cachedcopy's dir. cachepath=%q err=%v", o.cachepath, err)
	}

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
		//gou.Debugf("new file %s", o.name)
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
	//gou.Debugf("opened %q  readonly?%v opened?%v", o.cachepath, o.readonly, o.opened)
	//gou.Infof("Open() returning cache copy %p", o.cachedcopy)
	return o.cachedcopy, nil

	//return nil, fmt.Errorf("fetch error retry cnt reached: obj=%s tfile=%v", o.name, o.cachepath)
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

	err = cachedcopy.Close()
	if err != nil {
		return fmt.Errorf("error on closing localfile. %q err=%v", o.cachepath, err)
	}

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
			gou.Errorf("error on sync file=%q err=%v", o.name, err)
			return err
		}
		return nil
	}

	if o.file != nil {
		if err := o.file.Close(); err != nil {
			gou.Errorf("error on sync file=%q err=%v", o.name, err)
			return err
		}
	}

	gou.Debugf("not syncing on close? %v opened?%v  readonly?%v", o.name, o.opened, o.readonly)
	err := o.cachedcopy.Close()
	if err != nil {
		return fmt.Errorf("error on sync and closing localfile. %q err=%v", o.cachepath, err)
	}

	return nil
}

func (o *object) Release() error {
	if o.cachedcopy != nil {
		gou.Debugf("release %q vs %q", o.cachedcopy.Name(), o.cachepath)
		o.cachedcopy.Close()
		o.cachedcopy = nil
		o.opened = false
		return os.Remove(o.cachepath)
	}
	if o.file != nil {
		if err := o.file.Close(); err != nil {
			gou.Errorf("error on sync file=%q err=%v", o.name, err)
			return err
		}
	}
	// most likely this doesn't exist so don't return error
	os.Remove(o.cachepath)
	return nil
}

func (o *object) File() *os.File {
	return o.cachedcopy
}
func (o *object) Read(p []byte) (n int, err error) {
	return o.cachedcopy.Read(p)
}
func (o *object) Write(p []byte) (n int, err error) {
	if o.cachedcopy == nil {
		_, err := o.Open(cloudstorage.ReadWrite)
		if err != nil {
			return 0, err
		}
	}
	return o.cachedcopy.Write(p)
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
	for i, s := range strs {
		if i == 0 {
			out.WriteString(s)
		} else if s != "" {
			out.WriteByte('/')
			out.WriteString(s)
		}
	}
	return string(out.Bytes())
}

// ConcatSlash concats strings and ensures ends with "/"
func ConcatSlash(strs ...string) string {
	out := bytes.Buffer{}
	for _, s := range strs {
		if strings.HasSuffix(s, "/") {
			out.WriteString(s)
		} else {
			out.WriteString(s)
			out.WriteByte('/')
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
