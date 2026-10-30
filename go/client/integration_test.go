//go:build integration

package client

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Runs against a live Castor that already holds tuples: CASTOR_URL=http://127.0.0.1:30100 go test -tags integration ./client
func TestLiveService(t *testing.T) {
	url := os.Getenv("CASTOR_URL")
	if url == "" {
		t.Skip("CASTOR_URL not set")
	}
	c, err := NewIntraVcpClient(Options{ServiceAddresses: []string{url}})
	if err != nil {
		t.Fatal(err)
	}
	td, err := c.GetTelemetryData(context.Background())
	if err != nil || len(td.Metrics) == 0 {
		t.Fatalf("telemetry: %v %+v", err, td)
	}
	var tt TupleType
	found := false
	for _, m := range td.Metrics {
		if m.Available > 0 {
			tt, err = ParseTupleType(m.Type)
			if err != nil {
				t.Fatal(err)
			}
			found = true
			break
		}
	}
	if !found {
		t.Skip("no tuples available on the service")
	}
	tl, err := c.DownloadTupleShares(context.Background(), uuid.New(), tt, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(tl.Tuples) != 1 || len(tl.Tuples[0].Shares) != tt.Arity || len(tl.Tuples[0].Shares[0].Value) != tt.Field.ElementSize {
		t.Fatalf("unexpected tuple list for %s: %+v", tt.Name(), tl.Tuples)
	}
	t.Logf("downloaded 1 %s tuple, %d shares of %d bytes", tt.Name(), len(tl.Tuples[0].Shares), len(tl.Tuples[0].Shares[0].Value))
}

// Uploads a chunk over STOMP/WebSocket, activates it and checks that the available count grows.
func TestLiveUpload(t *testing.T) {
	url := os.Getenv("CASTOR_URL")
	if url == "" {
		t.Skip("CASTOR_URL not set")
	}
	ctx := context.Background()
	intra, err := NewIntraVcpClient(Options{ServiceAddresses: []string{url}})
	if err != nil {
		t.Fatal(err)
	}
	avail := func() int64 {
		td, err := intra.GetTelemetryData(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range td.Metrics {
			if m.Type == "BIT_GFP" {
				return m.Available
			}
		}
		return 0
	}
	before := avail()
	up, err := NewUploadClient(Options{ServiceAddresses: []string{url}})
	if err != nil {
		t.Fatal(err)
	}
	if err := up.Connect(ctx, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	defer up.Disconnect()
	tt, _ := ParseTupleType("BIT_GFP")
	data := make([]byte, 7*tt.TupleSize())
	for i := range data {
		data[i] = byte(i)
	}
	ch := TupleChunk{ChunkID: uuid.New(), TupleType: tt, Tuples: data}
	ok, err := up.UploadTupleChunk(ctx, ch, 15*time.Second)
	if err != nil || !ok {
		t.Fatalf("upload: ok=%v err=%v", ok, err)
	}
	if err := up.ActivateTupleChunk(ctx, ch.ChunkID); err != nil {
		t.Fatal(err)
	}
	if after := avail(); after != before+7 {
		t.Fatalf("available BIT_GFP tuples: before %d after %d, want +7", before, after)
	}
}
