package objstore

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/carbynestack/castor-go/internal/testenv"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testenv.Shutdown()
	os.Exit(code)
}

func TestRoundTrip(t *testing.T) {
	ep := testenv.MinioEndpoint(t)
	ctx := context.Background()
	s, err := New(ctx, ep, "minio-castor-bucket", testenv.MinioAccessKey, testenv.MinioSecretKey)
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
