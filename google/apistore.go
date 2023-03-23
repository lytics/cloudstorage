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

// AddBucketReader updates the bucket ACL to add entity as a reader on the bucket
// The bucket must be in fine-grained access control mode, or this will produce an error
func (c *APIStore) AddBucketReader(bucket, entity string) error {
	ac := &storage.BucketAccessControl{Entity: entity, Role: "READER"}
	_, err := c.service.BucketAccessControls.Insert(bucket, ac).Do()
	return err
}

// AddBucketWriter updates the bucket ACL to add entity as a writer on the bucket
// The bucket must be in fine-grained access control mode, or this will produce an error
func (c *APIStore) AddBucketWriter(bucket, entity string) error {
	ac := &storage.BucketAccessControl{Entity: entity, Role: "WRITER"}
	_, err := c.service.BucketAccessControls.Insert(bucket, ac).Do()
	return err
}

// SetBucketAgeLifecycle updates a bucket-level lifecycle policy for object age in days
func (c *APIStore) SetBucketAgeLifecycle(name string, days int64) error {
	bucket := &storage.Bucket{Name: name}
	bucket.Lifecycle = new(storage.BucketLifecycle)
	action := &storage.BucketLifecycleRuleAction{Type: "Delete"}
	condition := &storage.BucketLifecycleRuleCondition{Age: &days}
	bucket.Lifecycle.Rule = make([]*storage.BucketLifecycleRule, 1)
	bucket.Lifecycle.Rule[0] = &storage.BucketLifecycleRule{Action: action, Condition: condition}
	_, err := c.service.Buckets.Patch(name, bucket).Do()
	return err
}

// GrantObjectViewer updates the IAM policy on the bucket to grant member the roles/storage.objectViewer role
// The existing policy attributes on the bucket are preserved
func (c *APIStore) GrantObjectViewer(bucket, member string) error {
	return c.grantRole(bucket, member, "roles/storage.objectViewer")
}

// GrantObjectCreator updates the IAM policy on the bucket to grant member the roles/storage.objectCreator role
// The existing policy attributes on the bucket are preserved
func (c *APIStore) GrantObjectCreator(bucket, member string) error {
	return c.grantRole(bucket, member, "roles/storage.objectCreator")
}

// GrantObjectAdmin updates the IAM policy on the bucket to grant member the roles/storage.objectAdmin role
// The existing policy attributes on the bucket are preserved
func (c *APIStore) GrantObjectAdmin(bucket, member string) error {
	return c.grantRole(bucket, member, "roles/storage.objectAdmin")
}

// grantRole updates the IAM policy for @bucket in order to rant @role to @member
// we have to retrieve the existing policy in order to modify it, per https://cloud.google.com/storage/docs/json_api/v1/buckets/setIamPolicy
func (c *APIStore) grantRole(bucket, member, role string) error {
	existingPolicy, err := c.service.Buckets.GetIamPolicy(bucket).Do()
	if err != nil {
		return err
	}

	var added bool
	for _, b := range existingPolicy.Bindings {
		if b.Role == role {
			for _, m := range b.Members {
				if m == member {
					// already granted
					return nil
				}
			}
			b.Members = append(b.Members, member)
			added = true
			break
		}
	}

	if !added {
		b := new(storage.PolicyBindings)
		b.Role = role
		b.Members = []string{member}
		existingPolicy.Bindings = append(existingPolicy.Bindings, b)
	}
	_, err = c.service.Buckets.SetIamPolicy(bucket, existingPolicy).Do()
	return err
}
