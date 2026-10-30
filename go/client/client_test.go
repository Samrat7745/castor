package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/carbynestack/castor-go/internal/model"
	"github.com/google/uuid"
)

func TestDownloadTupleSharesAgainstServiceJSON(t *testing.T) {
	tt, _ := ParseTupleType("MULTIPLICATION_TRIPLE_GFP")
	data := make([]byte, tt.TupleSize()*2)
	for i := range data {
		data[i] = byte(i)
	}
	var gotQuery, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery, gotAuth = r.URL.RawQuery, r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_ = model.WriteTupleListJSON(w, tt, data)
	}))
	defer srv.Close()
	c, err := NewIntraVcpClient(Options{ServiceAddresses: []string{srv.URL + "/"}, BearerTokenProvider: func(string) string { return "tok" }})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	tl, err := c.DownloadTupleShares(context.Background(), id, tt, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gotQuery, "tupletype=MULTIPLICATION_TRIPLE_GFP") || !strings.Contains(gotQuery, "count=2") || !strings.Contains(gotQuery, "reservationId="+id.String()) {
		t.Fatalf("query %q", gotQuery)
	}
	if gotAuth != "Bearer tok" {
		t.Fatalf("auth %q", gotAuth)
	}
	if len(tl.Tuples) != 2 || len(tl.Tuples[0].Shares) != 3 || tl.Field.Name != "gfp" {
		t.Fatalf("tuple list %+v", tl)
	}
	if got := tl.Tuples[0].Shares[0]; len(got.Value) != 16 || got.Value[1] != 1 || got.Mac[0] != 16 {
		t.Fatalf("share %+v", got)
	}
}

func TestDownloadErrorsAreClientErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "boom", 500) }))
	defer srv.Close()
	c, _ := NewIntraVcpClient(Options{ServiceAddresses: []string{srv.URL}})
	tt, _ := ParseTupleType("BIT_GFP")
	_, err := c.DownloadTupleShares(context.Background(), uuid.New(), tt, 1)
	var ce *ClientError
	if err == nil || !asClientError(err, &ce) || !strings.Contains(ce.Msg, "Failed downloading tuples from service") {
		t.Fatalf("err %v", err)
	}
	if _, err := c.GetTelemetryDataFor(context.Background(), 30); err == nil {
		t.Fatal("telemetry must fail")
	}
}

func asClientError(err error, out **ClientError) bool {
	ce, ok := err.(*ClientError)
	*out = ce
	return ok
}

func TestTelemetry(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.RequestURI()
		_, _ = w.Write([]byte(`{"interval":30,"metrics":[{"type":"BIT_GFP","available":7,"consumptionRate":1}]}`))
	}))
	defer srv.Close()
	c, _ := NewIntraVcpClient(Options{ServiceAddresses: []string{srv.URL}})
	td, err := c.GetTelemetryDataFor(context.Background(), 30)
	if err != nil || path != "/intra-vcp/telemetry?interval=30" || td.Metrics[0].Available != 7 {
		t.Fatal(err, path, td)
	}
	if _, err = c.GetTelemetryData(context.Background()); err != nil || path != "/intra-vcp/telemetry" {
		t.Fatal(err, path)
	}
}

func TestInterVcp(t *testing.T) {
	var calls []string
	var bodies []string
	mk := func(code int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			calls = append(calls, r.Method+" "+r.URL.Path)
			bodies = append(bodies, string(b))
			w.WriteHeader(code)
		}))
	}
	a, b := mk(200), mk(409)
	defer a.Close()
	defer b.Close()
	c, err := NewInterVcpClient(Options{ServiceAddresses: []string{a.URL, b.URL}})
	if err != nil {
		t.Fatal(err)
	}
	r := &Reservation{ReservationID: "r1", TupleType: "BIT_GFP", Status: model.Locked, Reservations: []model.ReservationElement{{TupleChunkID: uuid.NewString(), ReservedTuples: 1}}}
	ok, err := c.ShareReservation(context.Background(), r)
	if err != nil || ok {
		t.Fatalf("ok=%v err=%v (one slave answered 409)", ok, err)
	}
	if err := c.UpdateReservationStatus(context.Background(), "r1", model.Unlocked); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 4 || calls[0] != "POST /inter-vcp/reservation" || calls[2] != "PUT /inter-vcp/reservation/r1" || bodies[2] != `"UNLOCKED"` {
		t.Fatalf("calls %v bodies %v", calls, bodies)
	}
	var back Reservation
	if err := json.Unmarshal([]byte(bodies[0]), &back); err != nil || back.ReservationID != "r1" {
		t.Fatal(err, bodies[0])
	}
}

func TestBadAddresses(t *testing.T) {
	if _, err := NewIntraVcpClient(Options{}); err == nil {
		t.Fatal("empty must fail")
	}
	if _, err := NewIntraVcpClient(Options{ServiceAddresses: []string{"nohost"}}); err == nil {
		t.Fatal("invalid must fail")
	}
}
