package client

import (
	"context"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Samrat7745/castor/go/internal/model"
	"github.com/google/uuid"
)

// Ports of CastorServiceUriTest, DefaultCastorIntraVcpClientTest and DefaultCastorInterVcpClientTest.
// The Java tests mock CsHttpClient; here the clients talk to httptest servers and the assertions
// are made on what arrives on the wire. Not applicable: the WebSocket/activation URIs (the Go
// client has no uploader).

type call struct{ method, uri, auth, ct, body string }

type recorder struct {
	mu    sync.Mutex
	calls []call
}

func (r *recorder) handler(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		b, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.calls = append(r.calls, call{req.Method, req.URL.RequestURI(), req.Header.Get("Authorization"), req.Header.Get("Content-Type"), string(b)})
		r.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

func (r *recorder) only(t *testing.T) call {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) != 1 {
		t.Fatalf("want exactly one call, got %v", r.calls)
	}
	return r.calls[0]
}

func TestServiceAddressValidation(t *testing.T) {
	for _, c := range []struct{ addr, want string }{
		{"", "serviceAddress must not be empty!"},
		{"localhost:8080", invalidAddressMsg}, // no scheme
		{"invalidUri", invalidAddressMsg},
	} {
		if _, err := NewIntraVcpClient(Options{ServiceAddresses: []string{c.addr}}); err == nil || err.Error() != c.want {
			t.Errorf("intra %q: %v", c.addr, err)
		}
		// the inter client validates every single address
		if _, err := NewInterVcpClient(Options{ServiceAddresses: []string{"https://castor.carbynestack.io:8080", c.addr}}); err == nil || err.Error() != c.want {
			t.Errorf("inter %q: %v", c.addr, err)
		}
	}
	if _, err := NewInterVcpClient(Options{}); err == nil {
		t.Error("inter client without any address must fail")
	}
}

func TestRestBase(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"https://castor.carbynestack.io:8080", "https://castor.carbynestack.io:8080"},
		{"http://castor.carbynestack.io:8080", "http://castor.carbynestack.io:8080"},
		{"ws://castor.carbynestack.io:8080", "http://castor.carbynestack.io:8080"},
		{"wss://castor.carbynestack.io:8080", "https://castor.carbynestack.io:8080"},
		{"https://castor.carbynestack.io:8081/", "https://castor.carbynestack.io:8081"},
		{"https://castor.carbynestack.io/Castor", "https://castor.carbynestack.io/Castor"},
		{"https://castor.carbynestack.io/Castor/", "https://castor.carbynestack.io/Castor"},
	} {
		got, err := restBase(c.in)
		if err != nil || got != c.want {
			t.Errorf("%s: got %q, %v", c.in, got, err)
		}
	}
}

// givenCastorServiceUri_whenGetRequestTupleUri / ..._whenGetRequestTelemetryUri / ...trailing slash /
// givenValidPathSegments_whenBuildingResourceUri: the composed endpoint URIs, below a base path.
func TestEndpointURIs(t *testing.T) {
	rec := &recorder{}
	// one body that decodes as tuple list as well as telemetry data (unknown properties are ignored)
	srv := httptest.NewServer(rec.handler(200, `{"tupleCls":"InputMask","field":{"@type":"Gfp","name":"gfp","elementSize":16},"tuples":[],"metrics":[],"interval":1}`))
	defer srv.Close()
	ctx := context.Background()
	for _, base := range []string{srv.URL, srv.URL + "/", srv.URL + "/Castor", srv.URL + "/Castor/"} {
		prefix := strings.TrimSuffix(strings.TrimPrefix(base, srv.URL), "/")
		intra, err := NewIntraVcpClient(Options{ServiceAddresses: []string{base}})
		if err != nil {
			t.Fatal(err)
		}
		rid := uuid.MustParse("80fbba1b-3da8-4b1e-8a2c-cebd65229fad")
		tt, _ := ParseTupleType("INPUT_MASK_GFP")
		rec.calls = nil
		if _, err := intra.DownloadTupleShares(ctx, rid, tt, 3); err != nil {
			t.Fatal(err)
		}
		u, _ := url.ParseRequestURI(rec.only(t).uri)
		q := u.Query()
		if u.Path != prefix+"/intra-vcp/tuples" || q.Get("count") != "3" || q.Get("reservationId") != rid.String() || q.Get("tupletype") != "INPUT_MASK_GFP" || len(q) != 3 {
			t.Fatalf("%s: %s", base, u)
		}
		rec.calls = nil
		if _, err := intra.GetTelemetryData(ctx); err != nil {
			t.Fatal(err)
		}
		if c := rec.only(t); c.uri != prefix+"/intra-vcp/telemetry" {
			t.Fatalf("%s: %s", base, c.uri)
		}
		rec.calls = nil
		if _, err := intra.GetTelemetryDataFor(ctx, 5000); err != nil {
			t.Fatal(err)
		}
		if c := rec.only(t); c.uri != prefix+"/intra-vcp/telemetry?interval=5000" {
			t.Fatalf("%s: %s", base, c.uri)
		}
		inter, err := NewInterVcpClient(Options{ServiceAddresses: []string{base}})
		if err != nil {
			t.Fatal(err)
		}
		rec.calls = nil
		if err := inter.UpdateReservationStatus(ctx, "1234", model.Unlocked); err != nil {
			t.Fatal(err)
		}
		if c := rec.only(t); c.method != "PUT" || c.uri != prefix+"/inter-vcp/reservation/1234" {
			t.Fatalf("%s: %+v", base, c)
		}
	}
}

func TestIntraClientDownloadAndErrors(t *testing.T) {
	ctx := context.Background()
	tt, _ := ParseTupleType("MULTIPLICATION_TRIPLE_GFP")
	rid := uuid.MustParse("3dc08ff2-5eed-49a9-979e-3a3ac0e4a2cf")

	// givenSuccessfulRequest_whenDownloadTripleShares_thenReturnExpectedContent
	share := []byte{1, 4, 72, 200, 234, 244, 72, 88, 109, 42, 229, 56, 109, 42, 229, 56}
	data := make([]byte, 0, 2*tt.TupleSize())
	for i := 0; i < 2*tt.Arity*2; i++ {
		data = append(data, share...)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = model.WriteTupleListJSON(w, tt, data) }))
	l, err := mustIntra(t, srv.URL).DownloadTupleShares(ctx, rid, tt, 2)
	if err != nil || len(l.Tuples) != 2 || len(l.Tuples[1].Shares) != 3 || string(l.Tuples[1].Shares[2].Mac) != string(share) {
		t.Fatalf("%v %+v", err, l)
	}
	srv.Close()

	// givenRequestEmitsError_whenDownloadTripleShares_thenThrowCastorClientException: unreachable service
	c := mustIntra(t, srv.URL)
	_, err = c.DownloadTupleShares(ctx, rid, tt, 1)
	var ce *ClientError
	if !errors.As(err, &ce) || !strings.HasPrefix(ce.Msg, "Failed downloading tuples from service "+srv.URL+": ") || ce.Cause == nil || ce.Unwrap() != ce.Cause {
		t.Fatalf("%#v", err)
	}
	if !strings.HasSuffix(ce.Msg, ce.Cause.Error()) {
		t.Fatalf("message must carry the cause: %q / %v", ce.Msg, ce.Cause)
	}
	// ... telemetry, with and without interval
	for _, call := range []func() error{
		func() error { _, err := c.GetTelemetryData(ctx); return err },
		func() error { _, err := c.GetTelemetryDataFor(ctx, 50); return err },
	} {
		if err := call(); !errors.As(err, &ce) || !strings.HasPrefix(ce.Msg, "Failed fetching telemetry data from service "+srv.URL+": ") || ce.Cause == nil {
			t.Fatalf("%#v", err)
		}
	}
	// a non-JSON answer is a client error as well
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("<html>")) }))
	defer bad.Close()
	if _, err := mustIntra(t, bad.URL).GetTelemetryData(ctx); !errors.As(err, &ce) {
		t.Fatalf("%v", err)
	}
}

func mustIntra(t *testing.T, addr string) IntraVcpClient {
	t.Helper()
	c, err := NewIntraVcpClient(Options{ServiceAddresses: []string{addr}})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// givenSuccessfulRequest_whenGetTelemetryData / ..WithInterval: the decoded TelemetryData.
func TestIntraClientTelemetryContent(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(rec.handler(200, `{"metrics":[{"available":1,"consumptionRate":2,"type":"BIT_GFP"}],"interval":100}`))
	defer srv.Close()
	c := mustIntra(t, srv.URL)
	want := TelemetryData{Metrics: []TupleMetric{{Available: 1, ConsumptionRate: 2, Type: "BIT_GFP"}}, Interval: 100}
	for _, get := range []func() (*TelemetryData, error){
		func() (*TelemetryData, error) { return c.GetTelemetryData(context.Background()) },
		func() (*TelemetryData, error) { return c.GetTelemetryDataFor(context.Background(), 50) },
	} {
		got, err := get()
		if err != nil || got.Interval != want.Interval || len(got.Metrics) != 1 || got.Metrics[0] != want.Metrics[0] {
			t.Fatalf("%v %+v", err, got)
		}
	}
}

// givenSslConfiguration_whenBuildClient_thenInitializeCsHttpClientAccordingly (intra and inter):
// certificate validation on/off, a trusted certificate file, and the bearer token provider.
func TestSSLConfigurationAndBearerToken(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewTLSServer(rec.handler(200, `{"metrics":[],"interval":100}`))
	defer srv.Close()
	ctx := context.Background()
	pemFile := filepath.Join(t.TempDir(), "cert.pem")
	if err := os.WriteFile(pemFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	var asked []string
	token := func(addr string) string { asked = append(asked, addr); return "testBearerToken" }

	// untrusted certificate: rejected
	c, _ := NewIntraVcpClient(Options{ServiceAddresses: []string{srv.URL}})
	if _, err := c.GetTelemetryData(ctx); err == nil {
		t.Fatal("self-signed certificate must be rejected by default")
	}
	for name, o := range map[string]Options{
		"without validation": {ServiceAddresses: []string{srv.URL}, NoSSLValidation: true, BearerTokenProvider: token},
		"trusted cert file":  {ServiceAddresses: []string{srv.URL}, TrustedCertificates: []string{pemFile}, BearerTokenProvider: token},
	} {
		rec.calls = nil
		c, err := NewIntraVcpClient(o)
		if err != nil {
			t.Fatal(name, err)
		}
		if _, err := c.GetTelemetryData(ctx); err != nil {
			t.Fatal(name, err)
		}
		if got := rec.only(t).auth; got != "Bearer testBearerToken" {
			t.Fatalf("%s: auth %q", name, got)
		}
		rec.calls = nil
		ic, err := NewInterVcpClient(o)
		if err != nil {
			t.Fatal(name, err)
		}
		if err := ic.UpdateReservationStatus(ctx, "r", model.Unlocked); err != nil {
			t.Fatal(name, err)
		}
		if got := rec.only(t).auth; got != "Bearer testBearerToken" {
			t.Fatalf("%s: inter auth %q", name, got)
		}
	}
	if len(asked) == 0 || asked[0] != srv.URL {
		t.Fatalf("the provider is asked for the service address: %v", asked)
	}
	// an unreadable / invalid certificate file is a client error at construction time
	for _, f := range []string{filepath.Join(t.TempDir(), "missing.pem"), func() string {
		p := filepath.Join(t.TempDir(), "bad.pem")
		_ = os.WriteFile(p, []byte("no pem"), 0o600)
		return p
	}()} {
		var ce *ClientError
		if _, err := NewIntraVcpClient(Options{ServiceAddresses: []string{srv.URL}, TrustedCertificates: []string{f}}); !errors.As(err, &ce) {
			t.Fatalf("%s: %v", f, err)
		}
	}
}

func TestClientTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer srv.Close()
	c, _ := NewIntraVcpClient(Options{ServiceAddresses: []string{srv.URL}, Timeout: 100 * time.Millisecond})
	start := time.Now()
	var ce *ClientError
	if _, err := c.GetTelemetryData(context.Background()); !errors.As(err, &ce) || time.Since(start) > 3*time.Second {
		t.Fatalf("%v after %s", err, time.Since(start))
	}
}

func res() *Reservation {
	return &Reservation{ReservationID: "r1", TupleType: "BIT_GFP", Status: model.Locked,
		Reservations: []model.ReservationElement{{TupleChunkID: uuid.NewString(), ReservedTuples: 1}}}
}

// givenSingleEndpointReturnsFailure_whenShareReservation_thenReturnFalse,
// givenSuccessfulRequest_whenShareReservation_thenReturnTrue, givenCommunicationFails_whenShareReservation_...
func TestInterShareReservation(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name         string
		status       [2]int
		wantOK       bool
		wantRequests int
	}{
		{"all succeed", [2]int{201, 200}, true, 2},
		{"second fails", [2]int{201, 404}, false, 2},
		{"first fails, second is still asked", [2]int{404, 201}, false, 2},
	} {
		recs := [2]*recorder{{}, {}}
		var srvs [2]*httptest.Server
		for i := range srvs {
			srvs[i] = httptest.NewServer(recs[i].handler(c.status[i], "x"))
			defer srvs[i].Close()
		}
		ic, err := NewInterVcpClient(Options{ServiceAddresses: []string{srvs[0].URL, srvs[1].URL}})
		if err != nil {
			t.Fatal(err)
		}
		ok, err := ic.ShareReservation(ctx, res())
		if err != nil || ok != c.wantOK || len(recs[0].calls)+len(recs[1].calls) != c.wantRequests {
			t.Fatalf("%s: ok=%v err=%v calls=%v %v", c.name, ok, err, recs[0].calls, recs[1].calls)
		}
		for _, r := range recs {
			if got := r.calls[0]; got.method != "POST" || got.uri != "/inter-vcp/reservation" || got.ct != "application/json" {
				t.Fatalf("%s: %+v", c.name, got)
			}
		}
	}
	// communication failure -> CastorClientException with cause
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	ic, _ := NewInterVcpClient(Options{ServiceAddresses: []string{dead.URL}})
	ok, err := ic.ShareReservation(ctx, res())
	var ce *ClientError
	if ok || !errors.As(err, &ce) || ce.Msg != FailedSharing || ce.Cause == nil {
		t.Fatalf("%v %#v", ok, err)
	}
}

// givenCommunicationFails_whenUpdateReservation_thenThrowCastorClientException and
// givenSuccessfulRequests_whenUpdateReservation_thenDoNothing: every slave receives the status.
func TestInterUpdateReservationStatus(t *testing.T) {
	ctx := context.Background()
	recs := [2]*recorder{{}, {}}
	var addrs []string
	for _, r := range recs {
		s := httptest.NewServer(r.handler(200, ""))
		defer s.Close()
		addrs = append(addrs, s.URL)
	}
	ic, _ := NewInterVcpClient(Options{ServiceAddresses: addrs, BearerTokenProvider: func(a string) string { return "t-" + a }})
	if err := ic.UpdateReservationStatus(ctx, "reservationId", model.Unlocked); err != nil {
		t.Fatal(err)
	}
	for i, r := range recs {
		if c := r.only(t); c.method != "PUT" || c.uri != "/inter-vcp/reservation/reservationId" || c.body != `"UNLOCKED"` || c.auth != "Bearer t-"+addrs[i] {
			t.Fatalf("%+v", c)
		}
	}
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	ic, _ = NewInterVcpClient(Options{ServiceAddresses: []string{dead.URL}})
	var ce *ClientError
	if err := ic.UpdateReservationStatus(ctx, "reservationId", model.Unlocked); !errors.As(err, &ce) || ce.Msg != FailedUpdating || ce.Cause == nil {
		t.Fatalf("%#v", err)
	}
}
