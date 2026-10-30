//go:build integration

package client

import (
	"context"
	"os"
	"testing"

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
