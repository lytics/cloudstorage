package cloudstorage

import "google.golang.org/api/storage/v1"

type ApiStore struct {
	service *storage.Service
	project string
}

func NewApiStore(csctx *CloudStoreContext) (*ApiStore, error) {
	googleClient, err := NewGoogleClient(csctx)
	if err != nil {
		return nil, err
	}
	service, err := storage.New(googleClient.Client())
	if err != nil {
		return nil, err
	}
	return &ApiStore{service: service, project: csctx.Project}, nil
}

// BucketExists checks for the bucket name
func (c *ApiStore) BucketExists(name string) bool {
	b, err := c.service.Buckets.Get(name).Do()
	if err != nil {
		return false
	}

	return b.Id != ""
}

// CreateBucket creates a new bucket in GCS
func (c *ApiStore) CreateBucket(name string) error {
	bucket := &storage.Bucket{Name: name}
	_, err := c.service.Buckets.Insert(c.project, bucket).Do()

	return err
}
