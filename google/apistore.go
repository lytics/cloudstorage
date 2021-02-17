package google

import (
	"github.com/lytics/cloudstorage"
	"google.golang.org/api/storage/v1"
)

// APIStore a google api store
type APIStore struct {
	service *storage.Service
	project string
}

// NewAPIStore create api store.
func NewAPIStore(conf *cloudstorage.Config) (*APIStore, error) {
	googleClient, err := NewGoogleClient(conf)
	if err != nil {
		return nil, err
	}
	service, err := storage.New(googleClient.Client())
	if err != nil {
		return nil, err
	}
	return &APIStore{service: service, project: conf.Project}, nil
}

// BucketExists checks for the bucket name
func (c *APIStore) BucketExists(name string) bool {
	b, err := c.service.Buckets.Get(name).Do()
	if err != nil {
		return false
	}

	return b.Id != ""
}

// CreateBucket creates a new bucket in GCS
func (c *APIStore) CreateBucket(name string) error {
	return c.CreateBucketWithLocation(name, "")
}

// CreateBucketWithLocation creates a new bucket in GCS with the specified location
func (c *APIStore) CreateBucketWithLocation(name, location string) error {
	bucket := &storage.Bucket{Name: name, Location: location}
	_, err := c.service.Buckets.Insert(c.project, bucket).Do()
	return err
}

// AddOwner adds entity as a owner of the object
func (c *APIStore) AddOwner(bucket, object, entity string) error {
	ac := &storage.ObjectAccessControl{Entity: entity, Role: "OWNER"}
	_, err := c.service.ObjectAccessControls.Insert(bucket, object, ac).Do()
	return err
}

// AddReader adds enitty as a reader of the object
func (c *APIStore) AddReader(bucket, object, entity string) error {
	ac := &storage.ObjectAccessControl{Entity: entity, Role: "READER"}
	_, err := c.service.ObjectAccessControls.Insert(bucket, object, ac).Do()
	return err
}

// AddBucketReader adds a reader of the bucket
func (c *APIStore) AddBucketReader(bucket, entity string) error {
	ac := &storage.BucketAccessControl{Entity: entity, Role: "READER"}
	_, err := c.service.BucketAccessControls.Insert(bucket, ac).Do()
	return err
}

// AddBucketWriter adds a writer of the bucket
func (c *APIStore) AddBucketWriter(bucket, entity string) error {
	ac := &storage.BucketAccessControl{Entity: entity, Role: "WRITER"}
	_, err := c.service.BucketAccessControls.Insert(bucket, ac).Do()
	return err
}

// SetBucketLifecycle updates a bucket lifecycle policy in days
func (c *APIStore) SetBucketLifecycle(name string, days int64) error {
	bucket := &storage.Bucket{Name: name}
	bucket.Lifecycle = new(storage.BucketLifecycle)
	action := &storage.BucketLifecycleRuleAction{Type: "Delete"}
	condition := &storage.BucketLifecycleRuleCondition{Age: days}
	bucket.Lifecycle.Rule = make([]*storage.BucketLifecycleRule, 1)
	bucket.Lifecycle.Rule[0] = &storage.BucketLifecycleRule{Action: action, Condition: condition}
	_, err := c.service.Buckets.Patch(name, bucket).Do()
	return err
}
