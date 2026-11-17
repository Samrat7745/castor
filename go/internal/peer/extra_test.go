package peer

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/carbynestack/castor-go/internal/model"
)

// Behaviour of the master -> slave client towards slaves that answer badly, slowly, partially or
// over TLS, and what exactly goes over the wire.

func newClient(t *testing.T, timeout time.Duration, urls ...string) *Client {
	t.Helper()
	c, err := New(urls, false, nil, timeout)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func deadURL(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return "http://" + l.Addr().String()
}

type wire struct {
	mu   sync.Mutex
	reqs []*http.Request
	body []string
}

func (w *wire) handler(status int) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.mu.Lock()
		w.reqs = append(w.reqs, r)
		w.body = append(w.body, string(b))
		w.mu.Unlock()
		rw.WriteHeader(status)
	})
}

func (w *wire) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.reqs)
}

func TestSlaveStatusCodes(t *testing.T) {
	for _, c := range []struct {
		status int
		want   bool
	}{{200, true}, {201, true}, {202, true}, {204, true}, {299, true}, {400, false}, {401, false}, {403, false}, {404, false},
		{409, false}, {415, false}, {429, false}, {500, false}, {502, false}, {503, false}, {504, false}} {
		var w wire
		ts := httptest.NewServer(w.handler(c.status))
		cl := newClient(t, time.Second, ts.URL)
		ok, err := cl.ShareReservation(context.Background(), resv())
		if err != nil || ok != c.want {
			t.Errorf("share, status %d: %v %v", c.status, ok, err)
		}
		// updates never inspect the status
		if err := cl.UpdateReservationStatus(context.Background(), "id", model.Unlocked); err != nil {
			t.Errorf("update, status %d: %v", c.status, err)
		}
		ts.Close()
	}
}

func TestSlaveWithHugeOrEndlessBodyAndBrokenResponses(t *testing.T) {
	big := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(201)
		_, _ = w.Write([]byte(strings.Repeat("x", 5<<20)))
	}))
	defer big.Close()
	ok, err := newClient(t, 5*time.Second, big.URL).ShareReservation(context.Background(), resv())
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	// connection closed without any response
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	cl := newClient(t, time.Second, "http://"+l.Addr().String())
	var ce *ClientError
	if ok, err := cl.ShareReservation(context.Background(), resv()); ok || !errors.As(err, &ce) || ce.Msg != FailedSharing {
		t.Fatal(ok, err)
	}
	if err := cl.UpdateReservationStatus(context.Background(), "x", model.Locked); !errors.As(err, &ce) || ce.Msg != FailedUpdating {
		t.Fatal(err)
	}
	// garbage instead of HTTP
	l2, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l2.Close()
	go func() {
		for {
			c, err := l2.Accept()
			if err != nil {
				return
			}
			_, _ = c.Write([]byte("this is not http\r\n\r\n"))
			c.Close()
		}
	}()
	if ok, err := newClient(t, time.Second, "http://"+l2.Addr().String()).ShareReservation(context.Background(), resv()); ok || !errors.As(err, &ce) {
		t.Fatal(ok, err)
	}
}

func TestSlowAndSilentSlaves(t *testing.T) {
	hang := make(chan struct{})
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		<-hang
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(func() { close(hang) }) // runs before ts.Close
	cl := newClient(t, 150*time.Millisecond, ts.URL)
	var ce *ClientError
	start := time.Now()
	ok, err := cl.ShareReservation(context.Background(), resv())
	if ok || !errors.As(err, &ce) || ce.Msg != FailedSharing || time.Since(start) > 3*time.Second {
		t.Fatalf("silent slave: %v %v after %s", ok, err, time.Since(start))
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("cause is not a timeout: %v", err)
	}
	if err := cl.UpdateReservationStatus(context.Background(), "x", model.Unlocked); !errors.As(err, &ce) || ce.Msg != FailedUpdating {
		t.Fatal(err)
	}
	// the caller's context ends the wait earlier than the client timeout
	cl = newClient(t, 30*time.Second, ts.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start = time.Now()
	if ok, err := cl.ShareReservation(ctx, resv()); ok || !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 3*time.Second {
		t.Fatalf("context deadline: %v %v after %s", ok, err, time.Since(start))
	}
	cctx, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if err := cl.UpdateReservationStatus(cctx, "x", model.Unlocked); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// a slave that is slow but within the timeout is fine
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(201)
	}))
	defer slow.Close()
	if ok, err := newClient(t, 2*time.Second, slow.URL).ShareReservation(context.Background(), resv()); err != nil || !ok {
		t.Fatal(ok, err)
	}
}

// The slaves are asked in order. A transport failure ends the round (the slaves behind it are not
// asked), a refusing slave does not.
func TestPartialFailureWithSeveralSlaves(t *testing.T) {
	var a, b, c, refuse wire
	sa, sb, sc, sr := httptest.NewServer(a.handler(201)), httptest.NewServer(b.handler(201)), httptest.NewServer(c.handler(201)), httptest.NewServer(refuse.handler(409))
	defer sa.Close()
	defer sb.Close()
	defer sc.Close()
	defer sr.Close()
	dead := deadURL(t)
	ctx := context.Background()
	var ce *ClientError

	// [ok, down, ok]: error; the first slave has the reservation, the third never hears of it
	cl := newClient(t, time.Second, sa.URL, dead, sc.URL)
	if ok, err := cl.ShareReservation(ctx, resv()); ok || !errors.As(err, &ce) || ce.Msg != FailedSharing {
		t.Fatal(ok, err)
	}
	if a.count() != 1 || c.count() != 0 {
		t.Fatalf("asked: first %d, third %d", a.count(), c.count())
	}
	if err := cl.UpdateReservationStatus(ctx, "x", model.Unlocked); !errors.As(err, &ce) || ce.Msg != FailedUpdating {
		t.Fatal(err)
	}
	if a.count() != 2 || c.count() != 0 {
		t.Fatalf("after update: first %d, third %d", a.count(), c.count())
	}
	// [down, ok]: nobody behind the dead one is asked
	if _, err := newClient(t, time.Second, dead, sb.URL).ShareReservation(ctx, resv()); err == nil || b.count() != 0 {
		t.Fatalf("%v %d", err, b.count())
	}
	// [refusing, ok, ok]: false, but everybody was asked
	cl = newClient(t, time.Second, sr.URL, sb.URL, sc.URL)
	if ok, err := cl.ShareReservation(ctx, resv()); ok || err != nil {
		t.Fatal(ok, err)
	}
	if refuse.count() != 1 || b.count() != 1 || c.count() != 1 {
		t.Fatalf("asked: %d %d %d", refuse.count(), b.count(), c.count())
	}
	// [ok, refusing]: false as well
	if ok, err := newClient(t, time.Second, sa.URL, sr.URL).ShareReservation(ctx, resv()); ok || err != nil {
		t.Fatal(ok, err)
	}
	// all fine
	if ok, err := newClient(t, time.Second, sa.URL, sb.URL, sc.URL).ShareReservation(ctx, resv()); !ok || err != nil {
		t.Fatal(ok, err)
	}
}

func TestRequestsOnTheWire(t *testing.T) {
	var w wire
	ts := httptest.NewServer(w.handler(201))
	defer ts.Close()
	cl := newClient(t, time.Second, ts.URL+"/base/")
	r := resv()
	r.Reservations = append(r.Reservations, model.ReservationElement{TupleChunkID: "223e4567-e89b-12d3-a456-426614174000", ReservedTuples: 7, StartIndex: 3})
	if ok, err := cl.ShareReservation(context.Background(), r); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if err := cl.UpdateReservationStatus(context.Background(), "a/b c_ü", model.Unlocked); err != nil {
		t.Fatal(err)
	}
	if err := cl.UpdateReservationStatus(context.Background(), "id", model.Locked); err != nil {
		t.Fatal(err)
	}
	if len(w.reqs) != 3 {
		t.Fatalf("%d requests", len(w.reqs))
	}
	post, put, put2 := w.reqs[0], w.reqs[1], w.reqs[2]
	if post.Method != "POST" || post.URL.Path != "/base/inter-vcp/reservation" || post.URL.RawQuery != "" {
		t.Fatalf("%s %s", post.Method, post.URL)
	}
	for _, q := range w.reqs {
		if q.Header.Get("Content-Type") != "application/json" || q.Header.Get("Authorization") != "" || q.ContentLength <= 0 {
			t.Fatalf("headers %v length %d", q.Header, q.ContentLength)
		}
	}
	var got model.Reservation
	if err := json.Unmarshal([]byte(w.body[0]), &got); err != nil || got.ReservationID != r.ReservationID || len(got.Reservations) != 2 || got.Reservations[1].StartIndex != 3 || got.Status != model.Locked {
		t.Fatalf("%s %v", w.body[0], err)
	}
	if put.Method != "PUT" || put.URL.EscapedPath() != "/base/inter-vcp/reservation/a%2Fb%20c_%C3%BC" || w.body[1] != `"UNLOCKED"` {
		t.Fatalf("%s %s %s", put.Method, put.URL.EscapedPath(), w.body[1])
	}
	if w.body[2] != `"LOCKED"` || put2.URL.Path != "/base/inter-vcp/reservation/id" {
		t.Fatalf("%s %s", put2.URL.Path, w.body[2])
	}
}

func TestRedirects(t *testing.T) {
	var target wire
	dst := httptest.NewServer(target.handler(201))
	defer dst.Close()
	for _, c := range []struct {
		code       int
		wantMethod string
		wantBody   bool
		follow     bool
	}{{307, "", false, false}, {308, "", false, false}, {301, "", false, false}, {302, "", false, false}, {303, "GET", false, true}} {
		target = wire{}
		src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, dst.URL+"/moved", c.code)
		}))
		cl := newClient(t, 2*time.Second, src.URL)
		ok, err := cl.ShareReservation(context.Background(), resv())
		if err != nil {
			t.Fatalf("%d: %v", c.code, err)
		}
		// The HTTP client library follows redirects like the Java client: a POST is only
		// redirected by 303 See Other; any other redirect is returned as a non-2xx answer.
		if !c.follow {
			if target.count() != 0 || ok {
				t.Errorf("%d: redirect followed %d times (ok=%v), want it returned as failure", c.code, target.count(), ok)
			}
			src.Close()
			continue
		}
		if target.count() != 1 {
			t.Fatalf("%d: redirect target hit %d times", c.code, target.count())
		}
		got := target.reqs[0]
		if got.Method != c.wantMethod || (target.body[0] != "") != c.wantBody || !ok {
			t.Errorf("%d: followed as %s with body %q (ok=%v)", c.code, got.Method, target.body[0], ok)
		}
		src.Close()
	}
	// a redirect loop ends with an error instead of spinning
	var loop *httptest.Server
	loop = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, loop.URL+"/again", 303) // 303 is the redirect a POST follows
	}))
	defer loop.Close()
	var ce *ClientError
	if ok, err := newClient(t, 2*time.Second, loop.URL).ShareReservation(context.Background(), resv()); ok || !errors.As(err, &ce) || !strings.Contains(ce.Cause.Error(), "redirects") {
		t.Fatal(ok, err)
	}
	// a redirect without Location is just a non-2xx answer
	nl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(302) }))
	defer nl.Close()
	if ok, err := newClient(t, 2*time.Second, nl.URL).ShareReservation(context.Background(), resv()); ok || err != nil {
		t.Fatal(ok, err)
	}
}

func writePEM(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ts.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// unrelatedCA writes a freshly generated self-signed certificate that signed nothing.
func unrelatedCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "unrelated"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "other.pem")
	if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestTLS(t *testing.T) {
	var w wire
	ts := httptest.NewTLSServer(w.handler(201))
	defer ts.Close()
	ctx := context.Background()
	var ce *ClientError

	// default: the self-signed certificate is not trusted
	c, err := New([]string{ts.URL}, false, nil, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := c.ShareReservation(ctx, resv()); ok || !errors.As(err, &ce) || !strings.Contains(ce.Cause.Error(), "certificate") {
		t.Fatalf("untrusted certificate accepted: %v %v", ok, err)
	}
	if w.count() != 0 {
		t.Fatal("request reached the server despite the failed handshake")
	}
	// validation switched off
	c, _ = New([]string{ts.URL}, true, nil, 2*time.Second)
	if ok, err := c.ShareReservation(ctx, resv()); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if err := c.UpdateReservationStatus(ctx, "x", model.Unlocked); err != nil {
		t.Fatal(err)
	}
	// custom CA (the server's own certificate)
	c, err = New([]string{ts.URL}, false, []string{writePEM(t, ts)}, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := c.ShareReservation(ctx, resv()); !ok || err != nil {
		t.Fatal(ok, err)
	}
	// wss:// addresses are https
	c, _ = New([]string{"wss" + strings.TrimPrefix(ts.URL, "https")}, true, nil, 2*time.Second)
	if ok, err := c.ShareReservation(ctx, resv()); !ok || err != nil {
		t.Fatal(ok, err)
	}
	// a CA of another server does not help
	c, _ = New([]string{ts.URL}, false, []string{unrelatedCA(t)}, 2*time.Second)
	if ok, err := c.ShareReservation(ctx, resv()); ok || err == nil {
		t.Fatalf("wrong CA accepted: %v %v", ok, err)
	}
	// plain http against a TLS port and TLS against a plain port fail cleanly
	c, _ = New([]string{"http" + strings.TrimPrefix(ts.URL, "https")}, false, nil, 2*time.Second)
	if ok, err := c.ShareReservation(ctx, resv()); ok && err == nil {
		t.Fatal("http to a TLS port reported success")
	}
	plain := httptest.NewServer(w.handler(201))
	defer plain.Close()
	c, _ = New([]string{"https" + strings.TrimPrefix(plain.URL, "http")}, true, nil, 2*time.Second)
	if ok, err := c.ShareReservation(ctx, resv()); ok || !errors.As(err, &ce) {
		t.Fatalf("TLS to a plain port: %v %v", ok, err)
	}
}

func TestNewValidation(t *testing.T) {
	dir := t.TempDir()
	junk := filepath.Join(dir, "junk.pem")
	_ = os.WriteFile(junk, []byte("not a certificate"), 0o600)
	for _, c := range []struct {
		name    string
		uris    []string
		certs   []string
		wantErr string
	}{
		{"nil", nil, nil, "Declared service URIs must not be empty"},
		{"empty", []string{}, nil, "Declared service URIs must not be empty"},
		{"no host", []string{"http://"}, nil, `Invalid service address: "http://"`},
		{"no scheme", []string{"just-a-name"}, nil, `Invalid service address: "just-a-name"`},
		{"unparsable", []string{"http://[::1"}, nil, `Invalid service address: "http://[::1"`},
		{"second bad", []string{"http://ok:1", ""}, nil, `Invalid service address: ""`},
		{"missing cert file", []string{"http://h"}, []string{filepath.Join(dir, "missing.pem")}, "Unable to configure certificate management open " + filepath.Join(dir, "missing.pem")},
		{"junk cert file", []string{"http://h"}, []string{junk}, "Unable to configure certificate management " + junk},
		{"ok", []string{"http://h:1", "https://h2", "ws://h3/x", "wss://h4/"}, nil, ""},
	} {
		cl, err := New(c.uris, false, c.certs, time.Second)
		switch {
		case c.wantErr == "" && (err != nil || cl == nil):
			t.Errorf("%s: %v", c.name, err)
		case c.wantErr != "" && (err == nil || !strings.HasPrefix(err.Error(), c.wantErr) || cl != nil):
			t.Errorf("%s: got %v, want %q", c.name, err, c.wantErr)
		}
	}
	cl, _ := New([]string{"http://h:1/", "https://h2", "ws://h3/x", "wss://h4/"}, false, nil, time.Second)
	want := []string{"http://h:1", "https://h2", "http://h3/x", "https://h4"}
	for i, b := range cl.slaves {
		if b != want[i] {
			t.Errorf("base %d: %q, want %q", i, b, want[i])
		}
	}
}

func TestClientErrorText(t *testing.T) {
	cause := errors.New("dial tcp: refused")
	e := &ClientError{FailedSharing, cause}
	if e.Error() != FailedSharing || !errors.Is(e, cause) || e.Unwrap() != cause {
		t.Fatal(e.Error())
	}
}

func TestConcurrentUseOfOneClient(t *testing.T) {
	var w wire
	ts := httptest.NewServer(w.handler(201))
	defer ts.Close()
	cl := newClient(t, 5*time.Second, ts.URL, ts.URL)
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, err := cl.ShareReservation(context.Background(), resv()); !ok || err != nil {
				t.Error(ok, err)
			}
			if err := cl.UpdateReservationStatus(context.Background(), "x", model.Unlocked); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if w.count() != 40*4 {
		t.Fatalf("%d requests", w.count())
	}
}
