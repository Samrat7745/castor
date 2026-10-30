package objstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/carbynestack/castor-go/internal/testenv"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Ports of MinioTupleStoreTest. The Java test mocks MinioClient; here the store runs against a real
// Minio and the stored objects are inspected with a second, independent client.

func raw(t *testing.T) *minio.Client {
	t.Helper()
	ep := testenv.MinioEndpoint(t)
	c, err := minio.New(ep[len("http://"):], &minio.Options{Creds: credentials.NewStaticV4(testenv.MinioAccessKey, testenv.MinioSecretKey, "")})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newStore(t *testing.T, bucket string) *Store {
	t.Helper()
	s, err := New(context.Background(), testenv.MinioEndpoint(t), bucket, testenv.MinioAccessKey, testenv.MinioSecretKey)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + 3)
	}
	return b
}

const chunk = "3fd7eaf7-cda3-4384-8d86-2c43450cbe63"

// givenBucketDoesNotExist_whenConstruct_thenMakeBucket (and the existing-bucket path)
func TestNewMakesBucketOnlyIfMissing(t *testing.T) {
	ctx := context.Background()
	// unique per run: the MinIO container is shared by the whole test process (and by -count=N iterations)
	bucket := fmt.Sprintf("test-bucket-%d", time.Now().UnixNano())
	c := raw(t)
	if ok, _ := c.BucketExists(ctx, bucket); ok {
		t.Fatal("bucket exists before the test")
	}
	newStore(t, bucket)
	if ok, err := c.BucketExists(ctx, bucket); err != nil || !ok {
		t.Fatal("bucket must have been created", err)
	}
	// constructing again with the bucket in place (and content) neither fails nor wipes it
	s := newStore(t, bucket)
	if err := s.Save(ctx, chunk, pattern(10)); err != nil {
		t.Fatal(err)
	}
	newStore(t, bucket)
	if _, err := c.StatObject(ctx, bucket, chunk, minio.StatObjectOptions{}); err != nil {
		t.Fatal(err)
	}
}

// givenSuccessfulRequest_whenSave_thenPutChunkDataInDatabase
func TestSavePutsChunkDataUnderChunkId(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, "test-bucket-save")
	data := pattern(32)
	if err := s.Save(ctx, chunk, data); err != nil {
		t.Fatal(err)
	}
	o, err := raw(t).GetObject(ctx, "test-bucket-save", chunk, minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(o)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("%v %x", err, got)
	}
}

// givenPutObjectThrowsException_whenSave_thenThrowCastorServiceException
func TestSaveFailureIsReportedWithJavaMessageAndCause(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, "test-bucket-gone")
	if err := raw(t).RemoveBucket(ctx, "test-bucket-gone"); err != nil {
		t.Fatal(err)
	}
	err := s.Save(ctx, chunk, pattern(32))
	var se *Error
	if !errors.As(err, &se) || se.Msg != "Persisting TupleChunk failed." || err.Error() != se.Msg || se.Cause == nil || !errors.Is(err, se.Cause) {
		t.Fatalf("%#v", err)
	}
}

// givenIndexOfFirstTupleToReadIsNegative / givenLengthToReadIsZero (IllegalArgumentException messages)
func TestReadRejectsInvalidArguments(t *testing.T) {
	s := newStore(t, "test-bucket-args")
	ctx := context.Background()
	for _, c := range []struct {
		off, n int64
		want   string
	}{
		{-1, 42, "Index of first byte to read must not be negative."},
		{0, 0, "Must read at least one byte from database."},
		{0, -3, "Must read at least one byte from database."},
	} {
		var se *Error
		if _, err := s.Read(ctx, chunk, c.off, c.n); err == nil || err.Error() != c.want || errors.As(err, &se) {
			t.Errorf("(%d,%d): %v", c.off, c.n, err)
		}
	}
}

// givenReadingFromDatabaseThrowsException_whenDownloadTuples_thenThrowCastorServiceException
func TestReadOfMissingObjectFailsWithJavaMessageAndCause(t *testing.T) {
	s := newStore(t, "test-bucket-read")
	_, err := s.Read(context.Background(), chunk, 0, 42)
	var se *Error
	if !errors.As(err, &se) || se.Msg != "Error while reading tuples from database." || se.Cause == nil {
		t.Fatalf("%#v", err)
	}
}

// givenSuccessfulRequest_whenDownloadTuples: exactly the requested range
func TestReadReturnsRequestedRange(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, "test-bucket-range")
	data := pattern(5 * 32)
	if err := s.Save(ctx, chunk, data); err != nil {
		t.Fatal(err)
	}
	for _, r := range [][2]int64{{0, 32}, {64, 64}, {0, 160}, {159, 1}} {
		got, err := s.Read(ctx, chunk, r[0], r[1])
		if err != nil || !bytes.Equal(got, data[r[0]:r[0]+r[1]]) {
			t.Fatalf("%v: %v", r, err)
		}
	}
}

// givenNoChunkWithIdInDatabase_whenDeleteTupleChunk_thenDoNothing
func TestDeleteOfMissingChunkIsHarmless(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, "test-bucket-delete")
	if err := s.Delete(ctx, chunk); err != nil {
		t.Fatal("deleting a missing object must not fail:", err)
	}
	if err := s.Save(ctx, chunk, pattern(8)); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, chunk); err != nil {
		t.Fatal(err)
	}
	if _, err := raw(t).StatObject(ctx, "test-bucket-delete", chunk, minio.StatObjectOptions{}); err == nil {
		t.Fatal("object must be gone")
	}
}
