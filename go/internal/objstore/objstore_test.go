package objstore

import (
	"bytes"
	"context"
	"os"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	ep := os.Getenv("CASTOR_TEST_MINIO")
	if ep == "" {
		t.Skip("CASTOR_TEST_MINIO not set")
	}
	ctx := context.Background()
	s, err := New(ctx, ep, "minio-castor-bucket", "dummy-key", "dummy-secret")
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 5000)
	for i := range data {
		data[i] = byte(i)
	}
	id := "00000000-0000-0000-0000-0000000000aa"
	if err := s.Save(ctx, id, data); err != nil {
		t.Fatal(err)
	}
	got, err := s.Read(ctx, id, 100, 200)
	if err != nil || !bytes.Equal(got, data[100:300]) {
		t.Fatal(err)
	}
	if _, err := s.Read(ctx, id, 4900, 200); err == nil {
		t.Fatal("reading past the end must fail")
	}
	if _, err := s.Read(ctx, id, -1, 1); err == nil {
		t.Fatal("negative offset")
	}
	if err := s.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(ctx, id, 0, 1); err == nil {
		t.Fatal("deleted")
	}
	if s.Healthy(ctx) != nil {
		t.Fatal("health")
	}
}
