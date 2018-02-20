package sftp

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
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
		ID string
		clientCtx context.Context
		client    *ftp.Client
		cachepath string
		host      string
		port      int
		Folder    string
		files     []string
	}

	// File represents sftp File
	object struct {
		file       *ftp.File
		client     *Client
		cachedcopy *os.File
		fi         os.FileInfo
		/*
			name      string
			updated   time.Time
			metadata  map[string]string
			bucket    string
		*/
		readonly  bool
		opened    bool
		cachepath string
		infoOnce sync.Once
		infoErr  error
	}
)

func init() {
	// Register this Driver (s3) in cloudstorage driver registry.
	cloudstorage.Register(StoreType, func(conf *cloudstorage.Config) (cloudstorage.Store, error) {
		client, err := NewClientFromConfig(context.Background(), conf)
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
func NewClient(clientCtx context.Context, conf *cloudstorage.Config,  host string, port int, folder string, config *ssh.ClientConfig) (*Client, error) {
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

	sftpClient := &Client{
		ID: uid, 
		clientCtx: clientCtx, 
		client: ftpClient, 
		host: host, 
		port: port, 
		cachepath: conf.TmpDir,
		Folder: folder,
	}

	return sftpClient, nil
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

// Client return underlying client
func (s *Client) Client() interface{} {
	return s
}

func (s *Client) String() string {
	return fmt.Sprintf("<sftp host=%q />", s.host)
}

// NewObject of Type sftp.
func (s *Client) NewObject(objectname string) (cloudstorage.Object, error) {
	obj, err := s.Get(context.Background(), objectname)
	if err != nil && err != cloudstorage.ErrObjectNotFound {
		return nil, err
	} else if obj != nil {
		return nil, cloudstorage.ErrObjectExists
	}

	cf := cloudstorage.CachePathObj(s.cachepath, objectname, s.ID)

	return &object{
		client:         s,
		name:       objectname,
		metadata:   map[string]string{cloudstorage.ContentTypeKey: cloudstorage.ContentType(objectname)},
		//bucket:     s.bucket,
		cachedcopy: nil,
		cachepath:  cf,
	}, nil
}

// Open opens a file for read or writing
func (s *Client) Get(ctx context.Context, name string) (cloudstorage.Object, error) {
	if !s.Exists(name) {
		return nil, cloudstorage.ErrObjectNotFound
	}
	get := Concat(s.Folder, name)
	gou.InfoCtx(s.clientCtx, "getting file %s", get)
	f, err := s.client.Open(get)
	if err != nil {
		return nil, err
	}
	return &object{
		file:   f,
		client: s,
	}, nil
}

// Open opens a file for read or writing
func (s *Client) Open(prefix, filename string) (io.ReadCloser, error) {
	fn := Concat(prefix, filename)
	if !s.Exists(fn) {
		return nil, os.ErrNotExist
	}
	get := Concat(s.Folder, fn)
	gou.InfoCtx(s.clientCtx, "getting file %s", get)

	return s.client.Open(get)
}

// Delete deletes a file
func (s *Client) Delete(ctx context.Context, filename string) error {
	if !s.Exists(filename) {
		return os.ErrNotExist
	}
	r := Concat(s.Folder, filename)
	gou.InfoCtx(s.clientCtx, "removing file %s", r)

	return s.client.Remove(r)
}

// Rename renames a file
func (s *Client) Rename(oldname, newname string) error {
	if !s.Exists(oldname) {
		return os.ErrNotExist
	}
	o := Concat(s.Folder, oldname)
	n := Concat(s.Folder, newname)

	gou.InfoCtx(s.clientCtx, "renaming file %s to %s", o, n)

	return s.client.Rename(o, n)
}

// Exists checks to see if files exists
func (s *Client) Exists(filename string) bool {
	folder := ""
	if i := strings.LastIndex(filename, "/"); i > 0 {
		folder = filename[:i]
		filename = filename[i+1:]
	}

	files, _ := s.ListFiles(folder, true)
	for _, f := range files {
		if f == filename {
			return true
		}
	}
	return false
}

// List lists files in a directory
func (s *Client) List(ctx context.Context, q cloudstorage.Query) (*cloudstorage.ObjectsResponse, error) {
	fi, err := s.fetchFiles(folder)
	if err != nil {
		return nil, err
	}

	objResp := &cloudstorage.ObjectsResponse{
		Objects: make(cloudstorage.Objects, 0, len(fi)),
	}

	for _, o := range fi {
		if o.IsDir() {
			continue
		}
		objResp.Objects = append(objResp.Objects, newObjectFromFile(s, o))
	}
	return objResp, nil
}

// Files lists files as os.FileInfo in a directory
func (s *Client) Files(folder string) ([]os.FileInfo, error) {
	fi, err := s.fetchFiles(folder)
	if err != nil {
		return nil, err
	}

	var files []os.FileInfo
	for _, f := range fi {
		if f.IsDir() {
			continue
		}
		files = append(files, f)
	}
	return files, nil
}

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
func (s *Client) NewReaderWithContext(ctx context.Context, objectname string) (io.ReadCloser, error) {
	if !s.Exists(name) {
		return nil, cloudstorage.ErrObjectNotFound
	}
	get := Concat(s.Folder, name)
	gou.InfoCtx(s.clientCtx, "getting file %s", get)
	f, err := s.client.Open(get)
	if err != nil {
		return nil, err
	}

	return f, nil
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
	folder := Concat(s.Folder, f)
	if folder == "" {
		folder = "."
	}
	fi, err := s.client.ReadDir(folder)
	if err != nil {
		gou.WarnCtx(s.clientCtx, "failed to read directory %s with error: %v", s.Folder, err)
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

func newObjectFromFile(c *Client, f os.FileInfo) *object {
	return &object{
		client: c,
	}
}

// Upload copies reader body bytes into underlying sftp file
func (o *object) Upload(body io.Reader) (int64, error) {
	defer o.file.Close()

	wLength, err := o.file.ReadFrom(body)
	if err != nil {
		return 0, err
	}

	return wLength, nil
}

func (o *object) Delete() error {
	// this should be path/name ??
	return o.client.Delete(context.Background(), o.fi.Name())
}

func (o *object) Sync() error {

	if !o.opened {
		return fmt.Errorf("object isn't opened object:%s", o.name)
	}
	if o.readonly {
		return fmt.Errorf("trying to Sync a readonly object:%s", o.name)
	}

	cachedcopy, err := os.OpenFile(o.cachepath, os.O_RDWR, 0664)
	if err != nil {
		return fmt.Errorf("couldn't open localfile for sync'ing. local=%s err=%v", o.cachepath, err)
	}
	defer cachedcopy.Close()

	if _, err := cachedcopy.Seek(0, os.SEEK_SET); err != nil {
		return fmt.Errorf("error seeking to start of cachedcopy err=%v", err) //don't retry on local filesystem errors
	}

	return cloudstorage.ErrObjectNotImplemented
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

	serr := o.cachedcopy.Sync()
	cerr := o.cachedcopy.Close()
	if serr != nil || cerr != nil {
		return fmt.Errorf("error on sync and closing localfile. %s sync=%v, err=%v", o.cachepath, serr, cerr)
	}

	if o.opened && !o.readonly {
		err := o.Sync()
		if err != nil {
			u.Errorf("error on sync %v", err)
			return err
		}
	}
	return nil
}

func (o *object) Release() error {
	if o.cachedcopy != nil {
		o.cachedcopy.Close()
	}
	return os.Remove(o.cachepath)
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
