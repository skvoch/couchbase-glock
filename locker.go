package cb_glock

import (
	"fmt"
	"time"

	"github.com/couchbase/gocb/v2"
)

type Option func(*Locker) error

func WithBucket(bucket *gocb.Bucket) Option {
	return func(locker * Locker) error {
		if bucket == nil {
			return fmt.Errorf("failed to set bucket, bucket is nil")
		}

		locker.bucket = bucket
		return nil
	}
}

func WithCluster(cluster *gocb.Cluster) Option {
	return func(locker * Locker) error {
		if cluster == nil {
			return fmt.Errorf("failed to set cluster, cluster is nil")
		}

		locker.cluster = cluster

		return nil
	}
}

func WithLockDuration(lockDuration time.Duration) Option {
	return func(locker *Locker) error {
		locker.lockDuration = lockDuration

		return nil
	}
}

type Locker struct {
	bucket *gocb.Bucket
	cluster *gocb.Cluster
	lockDuration time.Duration
}

type Result struct {
	rows *gocb.QueryResult
}

func (r *Result) Row(valuePtr interface{}) error {
	return r.rows.Row(valuePtr)
}

func (r *Result) Next() bool {
	return r.rows.Next()
}


func New(opts ...Option) (*Locker,error) {
	var locker Locker


	for _, opt := range opts {
		if err := opt(&locker); err != nil {
			return nil, fmt.Errorf("failed to set option: %w", err)
		}
	}

	return &locker, nil
}

func (l *Locker) GetAndLock(where string) (*Result, error) {
	if where != "" {
		where += " AND"
	}
	query := fmt.Sprintf(`UPDATE %s SET _cb_glock_locked = NOW_MILLIS() WHERE %s (NOW_MILLIS() - _cb_glock_locked > %v OR _cb_glock_locked IS NOT VALUED) RETURNING %s.*`, l.bucket.Name(), where, l.lockDuration.Milliseconds(), l.bucket.Name())

	rows, err := l.cluster.Query(query, nil)

	if err != nil {
		return nil, fmt.Errorf("failed to get and lock: failed to do query: %w", err)
	}

	return &Result{
		rows: rows,
	}, nil
}