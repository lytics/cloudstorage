package cloudstorage

type TokenSource string

type AccessLevel int

const (
	LyticsJWTKeySource   TokenSource = "LyticsJWTkey"
	GoogleJWTKeySource   TokenSource = "GoogleJWTFile"
	GCEMetaKeySource     TokenSource = "gcemetadata"
	LocalFileSource      TokenSource = "localfiles"
	GCEDefaultOAuthToken TokenSource = "gcedefaulttoken"

	ReadOnly  AccessLevel = 0
	ReadWrite AccessLevel = 1
)

//Objects are just a collection of Object(s).  Used as the results for store.List commands.
type Objects []Object

func (o Objects) Len() int           { return len(o) }
func (o Objects) Less(i, j int) bool { return o[i].Name() < o[j].Name() }
func (o Objects) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

// the cloud store config parameters
//
//   # General settings
//   LogggingContext: logging context that's prefix to each logline.
//   TokenSource    : the methods of accessing the event archive.
//                    valid options are the xxxSource const above.
//   TmpDir         : where to cache local copies of the event files.
//   FilterStreams  : list of streams to filter out during archiving.
//   Topic          : the topic name to use when looking for archived events.
//                    all other topics will be assumed to be streaming only topics.
//
//   # GCS Archive settings
//   Project        :
//   Bucket         :
//   JwtConf        : required if TokenSource is JWTKey
//   PageSize       : what page size to use with Google
//
//   # Local Archive settings
//   LocalFS        : the location to use for archived events (i.e. the mocked cloud)

type CloudStoreContext struct {
	LogggingContext string

	TokenSource TokenSource

	//GCS Archive
	Project  string
	Bucket   string
	PageSize int // the page size to use with google api requests (default 1000)

	// used by LyticsJWTKeySource
	JwtConf JwtConfig
	// used by GoogleJWTKeySource
	JwtFile string
	Scope   string

	//LocalFS Archive
	LocalFS string // The location to use for archived events

	// The location to save locally cached seq files.
	TmpDir string
}

//For use with google/google_jwttransporter.go
// Which can be used by the google go sdk's
type JwtConfig interface {
	KeyLen() int
	Validate() error
	KeyBytes() ([]byte, error)
	GetClientEmail() string
	GetClientID() string
	GetKeyType() string
	GetScopes() []string
}
