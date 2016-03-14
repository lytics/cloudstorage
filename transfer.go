package cloudstorage

import (
	"fmt"
	"net/http"
	"time"

	"google.golang.org/api/storagetransfer/v1"
)

const (
	StatusEnabled     = "ENABLED"
	StatusDisabled    = "DISABLED"
	StatusUnspecified = "STATUS_UNSPECIFIED"
	StatusDeleted     = "DELETED"
)

var (
	MaxPrefix    = 20
	errBadFilter = fmt.Errorf("too many inclusion/exclusion prefixes")
)

type Transferer struct {
	svc *storagetransfer.TransferJobsService
}

func NewTransferClient(client *http.Client) (*Transferer, error) {
	st, err := storagetransfer.New(client)
	if err != nil {
		return nil, err
	}

	return &Transferer{storagetransfer.NewTransferJobsService(st)}, nil
}

func (t *Transferer) List() ([]*storagetransfer.TransferJob, error) {
	var jobs []*storagetransfer.TransferJob
	var token string

	for {
		call := t.svc.List()
		if token != "" {
			call = call.PageToken(token)
		}

		resp, err := call.Do()
		if err != nil {
			return nil, err
		}

		for _, job := range resp.TransferJobs {
			jobs = append(jobs, job)
		}

		token = resp.NextPageToken
		if token == "" {
			break
		}
	}
	return jobs, nil
}

func (t *Transferer) GetJob(job string) (*storagetransfer.TransferJob, error) {
	resp, err := t.svc.Get(job).Do()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (t *Transferer) NewTransfer(destProject string, src Source, include, exclude []string) error {
	spec := src.TransferSpec(destProject)

	// Google returns an error if more than 20 inclusionary/exclusionary fields are included
	if len(include) > MaxPrefix || len(exclude) > MaxPrefix {
		return errBadFilter
	}

	if len(include) > 0 || len(exclude) > 0 {
		spec.ObjectConditions = &storagetransfer.ObjectConditions{
			ExcludePrefixes: exclude,
			IncludePrefixes: include,
		}
	}

	job := newTransferJob(destProject, spec)
	_, err := t.svc.Create(job).Do()
	return err
}

func newTransferJob(project string, spec *storagetransfer.TransferSpec) *storagetransfer.TransferJob {
	return &storagetransfer.TransferJob{
		ProjectId:    project,
		Status:       StatusEnabled,
		TransferSpec: spec,
		Schedule:     oneTimeJobSchedule(time.Now()),
	}
}

func oneTimeJobSchedule(ts time.Time) *storagetransfer.Schedule {
	date := toDate(ts)
	return &storagetransfer.Schedule{
		ScheduleEndDate:   date,
		ScheduleStartDate: date,
	}
}

func toDate(ts time.Time) *storagetransfer.Date {
	return &storagetransfer.Date{
		Day:   int64(ts.Day()),
		Month: int64(ts.Month()),
		Year:  int64(ts.Year()),
	}
}

// When transferring data to GCS, the sink is restricted to a GCS bucket. However, the source
// can be either be another Gcs Bucket, an AWS S3 source, or a Http Source.
// Each source produces a source-specific storagetransfer TransferSpec.
type Source interface {
	TransferSpec(bucket string) *storagetransfer.TransferSpec
}

// GcsSource is a Source defined by a Gcs bucket
type GcsSource struct {
	source string
}

func NewGcsSource(bucket string) Source {
	return &GcsSource{bucket}
}

func (g *GcsSource) TransferSpec(bucket string) *storagetransfer.TransferSpec {
	ts := newTransferSpec(bucket)
	ts.GcsDataSource = &storagetransfer.GcsData{BucketName: g.source}
	return ts
}

// HttpSource is a Source defined by a HTTP URL data source
type HttpSource struct {
	url string
}

func NewHttpSource(url string) Source {
	return &HttpSource{url}
}

func (h *HttpSource) TransferSpec(bucket string) *storagetransfer.TransferSpec {
	ts := newTransferSpec(bucket)
	ts.HttpDataSource = &storagetransfer.HttpData{ListUrl: h.url}
	return ts
}

// AwsSource: an AWS S3 data source
type AwsSource struct {
	bucket          string
	accessKeyId     string
	secretAccessKey string
}

func NewAwsSource(bucket, accesskey, secret string) Source {
	return &AwsSource{bucket, accesskey, secret}
}

func (a *AwsSource) TransferSpec(bucket string) *storagetransfer.TransferSpec {
	ts := newTransferSpec(bucket)
	ts.AwsS3DataSource = &storagetransfer.AwsS3Data{
		AwsAccessKey: &storagetransfer.AwsAccessKey{
			AccessKeyId:     a.accessKeyId,
			SecretAccessKey: a.secretAccessKey,
		},
		BucketName: a.bucket,
	}
	return ts
}

func newTransferSpec(sink string) *storagetransfer.TransferSpec {
	return &storagetransfer.TransferSpec{
		GcsDataSink: &storagetransfer.GcsData{BucketName: sink},
	}
}
