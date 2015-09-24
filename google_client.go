package cloudstorage

import (
	"fmt"
	"net/http"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	googleOauth2 "golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
)

//An interface so we can return any of the 3 Google transporter wrapper as a single interface.
type GoogleOAuthClient interface {
	Client() *http.Client
}
type gOAuthClient struct {
	httpclient *http.Client
}

func (g *gOAuthClient) Client() *http.Client {
	return g.httpclient
}

func BuildJWTTransporter(jwtConf *JwtConf) (GoogleOAuthClient, error) {
	key, err := jwtConf.KeyBytes()
	if err != nil {
		return nil, err
	}

	conf := &jwt.Config{
		Email: jwtConf.Client_email,

		PrivateKey: key,
		Scopes:     jwtConf.Scopes,
		TokenURL:   googleOauth2.JWTTokenURL,
	}

	client := conf.Client(oauth2.NoContext)

	return &gOAuthClient{
		httpclient: client,
	}, nil
}

/*
   The account may be empty or the string "default" to use the instance's main account.
*/
func BuildGCEMetadatTransporter(serviceAccount string) (GoogleOAuthClient, error) {
	client := &http.Client{
		Transport: &oauth2.Transport{

			Source: googleOauth2.ComputeTokenSource(""),
		},
	}

	return &gOAuthClient{
		httpclient: client,
	}, nil
}

// BuildDefaultGoogleTransporter builds a transpoter that wraps the google DefaultClient:
//    Ref https://github.com/golang/oauth2/blob/master/google/default.go#L33
// DefaultClient returns an HTTP Client that uses the
// DefaultTokenSource to obtain authentication credentials
//    Ref : https://github.com/golang/oauth2/blob/master/google/default.go#L41
// DefaultTokenSource is a token source that uses
// "Application Default Credentials".
//
// It looks for credentials in the following places,
// preferring the first location found:
//
//   1. A JSON file whose path is specified by the
//      GOOGLE_APPLICATION_CREDENTIALS environment variable.
//   2. A JSON file in a location known to the gcloud command-line tool.
//      On other systems, $HOME/.config/gcloud/credentials.
//   3. On Google App Engine it uses the appengine.AccessToken function.
//   4. On Google Compute Engine, it fetches credentials from the metadata server.
//      (In this final case any provided scopes are ignored.)
//
// For more details, see:
// https://developers.google.com/accounts/docs/application-default-credentials
//
// Samples of possible scopes:
// Google Cloud Storage : https://github.com/GoogleCloudPlatform/gcloud-golang/blob/69098363d921fa3cf80f930468a41a33edd9ccb9/storage/storage.go#L51
// BigQuery             :  https://github.com/GoogleCloudPlatform/gcloud-golang/blob/522a8ceb4bb83c2def27baccf31d646bce11a4b2/bigquery/bigquery.go#L52
func BuildDefaultGoogleTransporter(scope ...string) (GoogleOAuthClient, error) {

	client, err := googleOauth2.DefaultClient(context.Background(), scope...)
	if err != nil {
		fmt.Errorf("Creating http client: %v", err)
	}

	return &gOAuthClient{
		httpclient: client,
	}, nil
}
