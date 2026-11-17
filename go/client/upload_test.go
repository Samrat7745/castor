package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Samrat7745/castor/go/internal/javaser"
	"github.com/Samrat7745/castor/go/internal/stomp"
	"github.com/google/uuid"
)

type uploadEnv struct {
	url      string
	mu       sync.Mutex
	received []*javaser.TupleChunk
	auth     string
}

// startUploadServer serves the real Castor STOMP server; reply decides the response to each chunk (nil = no response).
func startUploadServer(t *testing.T, reply func(chunk *javaser.TupleChunk) []byte) *uploadEnv {
	t.Helper()
	e := &uploadEnv{}
	srv := stomp.NewServer(stomp.Config{MaxMessage: 8 << 20, ServerReceiveMs: 10000}, func(_ context.Context, payload []byte, send func([]byte)) error {
		c, err := javaser.DecodeTupleChunk(payload)
		if err != nil {
			return err
		}
		e.mu.Lock()
		e.received = append(e.received, c)
		e.mu.Unlock()
		if r := reply(c); r != nil {
			send(r)
		}
		return nil
	}, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("/base/intra-vcp/ws", func(w http.ResponseWriter, r *http.Request) {
		e.mu.Lock()
		e.auth = r.Header.Get("Authorization")
		e.mu.Unlock()
		srv.ServeHTTP(w, r)
	})
	mux.HandleFunc("PUT /base/intra-vcp/tuple-chunks/activate/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") == "00000000-0000-4000-8000-000000000bad" {
			http.Error(w, "unknown chunk", 404)
			return
		}
		e.mu.Lock()
		e.auth = r.Header.Get("Authorization")
		e.mu.Unlock()
		w.WriteHeader(200)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	e.url = ts.URL + "/base"
	return e
}

func okReply(c *javaser.TupleChunk) []byte { return javaser.EncodeUploadResponse(c.ChunkID, nil, true) }

func testChunk(t *testing.T, tuples int) TupleChunk {
	t.Helper()
	tt, _ := ParseTupleType("INPUT_MASK_GFP")
	return TupleChunk{ChunkID: uuid.New(), TupleType: tt, Tuples: make([]byte, tuples*tt.TupleSize())}
}

func connected(t *testing.T, e *uploadEnv, tok string) UploadClient {
	t.Helper()
	o := Options{ServiceAddresses: []string{e.url}}
	if tok != "" {
		o.BearerTokenProvider = func(string) string { return tok }
	}
	c, err := NewUploadClient(o)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Connect(context.Background(), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Disconnect() })
	return c
}

func TestUploadRoundTripAndBearerToken(t *testing.T) {
	e := startUploadServer(t, okReply)
	c := connected(t, e, "tok-1")
	ch := testChunk(t, 3)
	ok, err := c.UploadTupleChunk(context.Background(), ch, 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("upload: ok=%v err=%v", ok, err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.received) != 1 || e.received[0].ChunkID != ch.ChunkID.String() || len(e.received[0].Tuples) != len(ch.Tuples) {
		t.Fatalf("server saw %+v", e.received)
	}
	if e.auth != "Bearer tok-1" {
		t.Fatalf("handshake auth %q", e.auth)
	}
}

func TestUploadFailureResponseIsFalse(t *testing.T) {
	msg := "Chunk must contain at least one tuple."
	e := startUploadServer(t, func(c *javaser.TupleChunk) []byte { return javaser.EncodeUploadResponse(c.ChunkID, &msg, false) })
	c := connected(t, e, "")
	ok, err := c.UploadTupleChunk(context.Background(), testChunk(t, 1), 5*time.Second)
	if err != nil || ok {
		t.Fatalf("want (false, nil), got (%v, %v)", ok, err)
	}
}

func TestUploadTimeoutIsFalse(t *testing.T) {
	e := startUploadServer(t, func(*javaser.TupleChunk) []byte { return nil })
	c := connected(t, e, "")
	start := time.Now()
	ok, err := c.UploadTupleChunk(context.Background(), testChunk(t, 1), 300*time.Millisecond)
	if err != nil || ok || time.Since(start) < 250*time.Millisecond {
		t.Fatalf("want (false, nil) after the timeout, got (%v, %v) after %v", ok, err, time.Since(start))
	}
}

func TestUploadRequiresConnectionAndLimitsTuples(t *testing.T) {
	e := startUploadServer(t, okReply)
	c, _ := NewUploadClient(Options{ServiceAddresses: []string{e.url}})
	_, err := c.UploadTupleChunk(context.Background(), testChunk(t, 1), time.Second)
	if ce, ok := err.(*ClientError); !ok || !strings.HasPrefix(ce.Msg, "Connection not established for ") {
		t.Fatalf("want not-connected error, got %v", err)
	}
	big := testChunk(t, MaximumNumberOfTuples+1)
	_, err = c.UploadTupleChunk(context.Background(), big, time.Second)
	want := "The maximum number of tuples is exceeded: actual=100001, allowed=100000"
	if ce, ok := err.(*ClientError); !ok || ce.Msg != want {
		t.Fatalf("want %q, got %v", want, err)
	}
}

func TestConnectFailureMessage(t *testing.T) {
	c, _ := NewUploadClient(Options{ServiceAddresses: []string{"http://127.0.0.1:1"}})
	err := c.Connect(context.Background(), time.Second)
	if ce, ok := err.(*ClientError); !ok || ce.Msg != "WebSocket connection to CastorServices failed" {
		t.Fatalf("got %v", err)
	}
}

func TestActivateTupleChunk(t *testing.T) {
	e := startUploadServer(t, okReply)
	c, _ := NewUploadClient(Options{ServiceAddresses: []string{e.url}, BearerTokenProvider: func(string) string { return "tk" }})
	if err := c.ActivateTupleChunk(context.Background(), uuid.New()); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	got := e.auth
	e.mu.Unlock()
	if got != "Bearer tk" {
		t.Fatalf("auth %q", got)
	}
	err := c.ActivateTupleChunk(context.Background(), uuid.MustParse("00000000-0000-4000-8000-000000000bad"))
	if ce, ok := err.(*ClientError); !ok || ce.Msg != "Failed to activate TupleChunk" || !strings.Contains(ce.Cause.Error(), "404") {
		t.Fatalf("got %#v", err)
	}
}

func TestConcurrentUploadsGetTheirOwnResponses(t *testing.T) {
	e := startUploadServer(t, okReply)
	c := connected(t, e, "")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, err := c.UploadTupleChunk(context.Background(), testChunk(t, 2), 5*time.Second); err != nil || !ok {
				t.Errorf("upload: %v %v", ok, err)
			}
		}()
	}
	wg.Wait()
}

func TestConnectTwiceKeepsTheSession(t *testing.T) {
	e := startUploadServer(t, okReply)
	c := connected(t, e, "")
	if err := c.Connect(context.Background(), time.Second); err != nil {
		t.Fatalf("second connect must be a no-op: %v", err)
	}
	if ok, err := c.UploadTupleChunk(context.Background(), testChunk(t, 1), 5*time.Second); err != nil || !ok {
		t.Fatalf("upload after second connect: %v %v", ok, err)
	}
}

func TestResponseWithoutChunkIDIsDiscarded(t *testing.T) {
	msg := "some failure without a chunk id"
	e := startUploadServer(t, func(*javaser.TupleChunk) []byte { return javaser.EncodeUploadResponse("", &msg, false) })
	c := connected(t, e, "")
	ok, err := c.UploadTupleChunk(context.Background(), testChunk(t, 1), 400*time.Millisecond)
	if err != nil || ok {
		t.Fatalf("a response without chunk id must not complete the upload: (%v, %v)", ok, err)
	}
}

func TestDisconnectEndsTheSession(t *testing.T) {
	e := startUploadServer(t, okReply)
	c := connected(t, e, "")
	if err := c.Disconnect(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UploadTupleChunk(context.Background(), testChunk(t, 1), time.Second); err == nil {
		t.Fatal("upload after disconnect must fail")
	}
	if err := c.Disconnect(); err != nil {
		t.Fatalf("disconnect twice: %v", err)
	}
	if err := c.Connect(context.Background(), 5*time.Second); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if ok, err := c.UploadTupleChunk(context.Background(), testChunk(t, 1), 5*time.Second); err != nil || !ok {
		t.Fatalf("upload after reconnect: %v %v", ok, err)
	}
}

func TestUploadClientRejectsBadAddress(t *testing.T) {
	if _, err := NewUploadClient(Options{}); err == nil {
		t.Fatal("no address must fail")
	}
	if _, err := NewUploadClient(Options{ServiceAddresses: []string{"nohost"}}); err == nil {
		t.Fatal("invalid address must fail")
	}
}
