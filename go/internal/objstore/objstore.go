// Package objstore wraps the Minio bucket holding raw tuple chunks (object key = chunk UUID).
package objstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Store is the Minio-backed tuple store.
type Store struct {
	client *minio.Client
	bucket string
}

// New connects to endpoint (e.g. http://minio:9000) and ensures the bucket exists.
func New(ctx context.Context, endpoint, bucket, accessKey, secretKey string) (*Store, error) {
	host, secure := endpoint, false
	if strings.Contains(endpoint, "://") {
		u, err := url.Parse(endpoint)
		if err != nil {
			return nil, err
		}
		host, secure = u.Host, u.Scheme == "https"
	}
	c, err := minio.New(host, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: secure})
	if err != nil {
		return nil, err
	}
	s := &Store{client: c, bucket: bucket}
	ok, err := c.BucketExists(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if !ok {
		if err := c.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Save writes the chunk bytes.
func (s *Store) Save(ctx context.Context, chunkID string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, chunkID, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	return err
}

// Read returns exactly length bytes starting at offset.
func (s *Store) Read(ctx context.Context, chunkID string, offset, length int64) ([]byte, error) {
	if offset < 0 {
		return nil, fmt.Errorf("Index of first byte to read must not be negative.")
	}
	if length < 1 {
		return nil, fmt.Errorf("Must read at least one byte from database.")
	}
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(offset, offset+length-1); err != nil {
		return nil, err
	}
	obj, err := s.client.GetObject(ctx, s.bucket, chunkID, opts)
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	buf := make([]byte, length)
	if _, err := io.ReadFull(obj, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// Delete removes the chunk object; errors are returned for the caller to swallow.
func (s *Store) Delete(ctx context.Context, chunkID string) error {
	return s.client.RemoveObject(ctx, s.bucket, chunkID, minio.RemoveObjectOptions{})
}

// Healthy performs a cheap reachability check.
func (s *Store) Healthy(ctx context.Context) error {
	_, err := s.client.BucketExists(ctx, s.bucket)
	return err
}
