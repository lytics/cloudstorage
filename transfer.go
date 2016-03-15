package cloudstorage

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"google.golang.org/api/storagetransfer/v1"
)

type Status string

const (
	Enabled     Status = "ENABLED"
	Disabled    Status = "DISABLED"
	Unspecified Status = "STATUS_UNSPECIFIED"
	Deleted     Status = "DELETED"
)

var (
	// MaxPrefix is the maximum number of prefix filters allowed when transfering files in GCS buckets
	MaxPrefix = 20

	errBadFilter = errors.New("too many inclusion/exclusion prefixes")
	errBadConfig = errors.New("transferconfig not valid")
)

// Transferer manages the transfer of data sources to GCS
type Transferer struct {
	svc *storagetransfer.TransferJobsService
}

// NewTransferClient creates a new Transferer using an authed http client
func NewTransferClient(client *http.Client) (*Transferer, error) {
	st, err := storagetransfer.New(client)
	if err != nil {
		return nil, err
	}

	return &Transferer{storagetransfer.NewTransferJobsService(st)}, nil
}

// List returns all of the transferJobs under a specific project. If the variadic argument "statuses"
// is provided, only jobs with the listed statuses are returned
func (t *Transferer) List(project string, statuses ...Status) ([]*storagetransfer.TransferJob, error) {
	var jobs []*storagetransfer.TransferJob
	var token string

	body, err := json.Marshal(struct {
		ProjectID   string   `json:"project_id"`
		JobStatuses []Status `json:"job_statuses,omitempty"`
	}{
		ProjectID:   project,
		JobStatuses: statuses,
	})

	if err != nil {
		return nil, err
	}

	for {
		call := t.svc.List().Filter(string(body))
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

// GetJob returns the transferJob with the specified project and job ID
func (t *Transferer) GetJob(project, job string) (*storagetransfer.TransferJob, error) {
	resp, err := t.svc.Get(job).ProjectId(project).Do()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// NewTransfer creates a new transferJob with the specified project, destination GCS bucket and source.
// The include/exclude arguments define the file prefixes in the source bucket to include/exclude
func (t *Transferer) NewTransfer(conf *TransferConfig) (*storagetransfer.TransferJob, error) {
	job, err := conf.Job()
	if err != nil {
		return nil, err
	}

	return t.svc.Create(job).Do()
}

func newTransferJob(project, description string, spec *storagetransfer.TransferSpec, sched *storagetransfer.Schedule) *storagetransfer.TransferJob {
	return &storagetransfer.TransferJob{
		ProjectId:    project,
		Status:       string(Enabled),
		TransferSpec: spec,
		Schedule:     sched,
		Description:  description,
	}
}

// oneTimeJobSchedule returns a storagetransfer job schedule that will only be executed one
func oneTimeJobSchedule(ts time.Time) *storagetransfer.Schedule {
	date := toDate(ts)
	return &storagetransfer.Schedule{
		ScheduleEndDate:   date,
		ScheduleStartDate: date,
	}
}

// toDate converts a time into a storagetransfer friendly Date
func toDate(ts time.Time) *storagetransfer.Date {
	return &storagetransfer.Date{
		Day:   int64(ts.Day()),
		Month: int64(ts.Month()),
		Year:  int64(ts.Year()),
	}
}

// Source defines the data source when transferring data to a GCS bucket. While the sink is restricted to a GCS bucket
// the source can either be another GCS bucket, and AWS S3 source, or a HTTP source
// Each source produces a storagetransfer TransferSpec
type Source interface {
	TransferSpec(destBucket string) *storagetransfer.TransferSpec
	String() string
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

func (g *GcsSource) String() string {
	return g.source
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

func (h *HttpSource) String() string {
	return h.url
}

// AwsSource is an AWS S3 data source
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

func (a *AwsSource) String() string {
	return a.bucket
}

func newTransferSpec(sink string) *storagetransfer.TransferSpec {
	return &storagetransfer.TransferSpec{
		GcsDataSink: &storagetransfer.GcsData{BucketName: sink},
	}
}

// TransferConfig wraps all of the relevant variables for transfer jobs
// into a unified struct
type TransferConfig struct {
	ProjectID  string // projectID of destination bucket
	DestBucket string
	Src        Source
	Include    []string
	Exclude    []string
	Schedule   *storagetransfer.Schedule
}

// Job instantiates a Transfer job from the TransferConfig struct
func (t *TransferConfig) Job() (*storagetransfer.TransferJob, error) {
	if t.DestBucket == "" || t.Src == nil {
		return nil, errBadConfig
	}

	// Google returns an error if more than 20 inclusionary/exclusionary fields are included
	if len(t.Include) > MaxPrefix || len(t.Exclude) > MaxPrefix {
		return nil, errBadFilter
	}

	spec := t.Src.TransferSpec(t.DestBucket)

	// Set the file-filters if the conditions are met
	if len(t.Include) > 0 || len(t.Exclude) > 0 {
		spec.ObjectConditions = &storagetransfer.ObjectConditions{
			ExcludePrefixes: t.Exclude,
			IncludePrefixes: t.Include,
		}
	}

	// if a schedule is not provided, create a 1-time transfer schedule
	var schedule *storagetransfer.Schedule
	if t.Schedule == nil {
		schedule = oneTimeJobSchedule(time.Now())
	}

	description := fmt.Sprintf("%s_%s_transfer", t.DestBucket, t.Src)
	return newTransferJob(t.ProjectID, description, spec, schedule), nil
}
