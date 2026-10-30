package stomp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Robustness of the STOMP server: every misbehaving peer is dropped (or ignored) and the server
// keeps serving everybody else. The tests speak raw WebSocket so they can misbehave at will.

func startCfg(t *testing.T, cfg Config, h UploadHandler) (*Server, string) {
	t.Helper()
	srv := NewServer(cfg, h, nil)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return srv, "ws" + strings.TrimPrefix(ts.URL, "http")
}

// subscribed is connect plus a round trip, so the SUBSCRIBE of connect has been processed on return.
func subscribed(t *testing.T, c *websocket.Conn) {
	t.Helper()
	connect(t, c)
	send(t, c, "SEND\ndestination:/queue/barrier\nreceipt:b\n\n\x00")
	if f := read(t, c); f.Command != "RECEIPT" {
		t.Fatalf("%+v", f)
	}
}

func noop(context.Context, []byte, func([]byte)) error { return nil }

func send(t *testing.T, c *websocket.Conn, s string) {
	t.Helper()
	if err := c.WriteMessage(websocket.TextMessage, []byte(s)); err != nil {
		t.Fatal(err)
	}
}

// expectClosed waits for the server to close the connection; ERROR frames on the way are returned.
func expectClosed(t *testing.T, c *websocket.Conn, within time.Duration) (errFrames []string) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(within))
	for {
		_, d, err := c.ReadMessage()
		if err != nil {
			var ne interface{ Timeout() bool }
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatalf("connection still open after %s", within)
			}
			return
		}
		if fr, _, perr := parseFrames(d, 1<<20); perr == nil && len(fr) == 1 && fr[0].Command == "ERROR" {
			m, _ := fr[0].Header("message")
			errFrames = append(errFrames, m)
		}
	}
}

// healthy proves the server still completes the whole protocol for a fresh client.
func healthy(t *testing.T, url string) {
	t.Helper()
	c := dial(t, url)
	send(t, c, "CONNECT\naccept-version:1.2\n\n\x00")
	if f := read(t, c); f.Command != "CONNECTED" {
		t.Fatalf("server unhealthy: %+v", f)
	}
	send(t, c, "SEND\ndestination:/queue/ping\nreceipt:ok\n\n\x00")
	if f := read(t, c); f.Command != "RECEIPT" {
		t.Fatalf("server unhealthy: %+v", f)
	}
}

func waitSessions(t *testing.T, srv *Server, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		srv.mu.RLock()
		got := len(srv.sessions)
		srv.mu.RUnlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d sessions registered, want %d", got, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestOversizedFrameIsRejected(t *testing.T) {
	var calls atomic.Int32
	srv, url := startCfg(t, Config{MaxMessage: 4096, ServerReceiveMs: 10000}, func(context.Context, []byte, func([]byte)) error {
		calls.Add(1)
		return nil
	})
	// declared content-length beyond the limit
	c := dial(t, url)
	connect(t, c)
	send(t, c, "SEND\ndestination:/app/tuples/upload\ncontent-length:100000\n\n")
	if errs := expectClosed(t, c, 3*time.Second); len(errs) != 1 || errs[0] != "Failed to parse frame" {
		t.Fatalf("errors %v", errs)
	}
	// a single WebSocket message beyond the read limit
	c = dial(t, url)
	connect(t, c)
	_ = c.WriteMessage(websocket.BinaryMessage, append([]byte("SEND\ndestination:/app/tuples/upload\n\n"), make([]byte, 10000)...))
	expectClosed(t, c, 3*time.Second)
	// a frame that never ends its header block, trickled in below the message limit
	c = dial(t, url)
	connect(t, c)
	for i := 0; i < 10; i++ {
		if c.WriteMessage(websocket.TextMessage, []byte("SEND\n"+strings.Repeat("h", 900))) != nil {
			break
		}
	}
	expectClosed(t, c, 3*time.Second)
	if calls.Load() != 0 {
		t.Fatal("handler saw an oversized frame")
	}
	waitSessions(t, srv, 0)
	healthy(t, url)
}

func TestFrameSplitAtEveryPosition(t *testing.T) {
	got := make(chan []byte, 1)
	_, url := startCfg(t, Config{MaxMessage: 1 << 20, ServerReceiveMs: 10000}, func(_ context.Context, p []byte, _ func([]byte)) error {
		got <- p
		return nil
	})
	body := []byte("pay\x00load\n\nwith\r\nnasty bytes")
	frame := append([]byte(fmt.Sprintf("SEND\ndestination:/app/tuples/upload\ncontent-length:%d\n\n", len(body))), body...)
	frame = append(frame, 0)
	c := dial(t, url)
	connect(t, c)
	for i := 1; i < len(frame); i++ {
		_ = c.WriteMessage(websocket.BinaryMessage, frame[:i])
		_ = c.WriteMessage(websocket.TextMessage, frame[i:])
		select {
		case p := <-got:
			if !bytes.Equal(p, body) {
				t.Fatalf("split %d: %q", i, p)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("split at %d: no payload", i)
		}
	}
	// one byte per message
	for i := range frame {
		_ = c.WriteMessage(websocket.BinaryMessage, frame[i:i+1])
	}
	select {
	case p := <-got:
		if !bytes.Equal(p, body) {
			t.Fatalf("bytewise: %q", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bytewise: no payload")
	}
	// two frames in one message
	_ = c.WriteMessage(websocket.BinaryMessage, append(append([]byte(nil), frame...), frame...))
	for i := 0; i < 2; i++ {
		select {
		case <-got:
		case <-time.After(3 * time.Second):
			t.Fatal("coalesced frames: missing payload")
		}
	}
}

func TestMissedHeartbeatClosesTheSession(t *testing.T) {
	srv, url := startCfg(t, Config{MaxMessage: 1 << 20, ServerReceiveMs: 40}, noop)
	// client promises a heart-beat every 40ms (server side timeout: 3 x 40ms) and then falls silent
	c := dial(t, url)
	send(t, c, "CONNECT\naccept-version:1.2\nheart-beat:40,0\n\n\x00")
	read(t, c)
	start := time.Now()
	expectClosed(t, c, 3*time.Second)
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Fatalf("closed after %s, before three missed beats", d)
	}
	waitSessions(t, srv, 0)

	// a client that does beat stays connected well beyond the timeout
	c = dial(t, url)
	send(t, c, "CONNECT\naccept-version:1.2\nheart-beat:40,0\n\n\x00")
	read(t, c)
	for i := 0; i < 15; i++ {
		send(t, c, "\n")
		time.Sleep(30 * time.Millisecond)
	}
	send(t, c, "SUBSCRIBE\nid:1\ndestination:/queue/response\nreceipt:alive\n\n\x00")
	if f := read(t, c); f.Command != "RECEIPT" {
		t.Fatalf("beating client was dropped: %+v", f)
	}
	// without a client heart-beat in CONNECT the server expects none
	c = dial(t, url)
	send(t, c, "CONNECT\naccept-version:1.2\n\n\x00")
	read(t, c)
	time.Sleep(400 * time.Millisecond)
	send(t, c, "SUBSCRIBE\nid:1\ndestination:/queue/response\nreceipt:alive\n\n\x00")
	if f := read(t, c); f.Command != "RECEIPT" {
		t.Fatalf("%+v", f)
	}
	healthy(t, url)
}

func TestClientDisconnectsMidUpload(t *testing.T) {
	var calls atomic.Int32
	srv, url := startCfg(t, Config{MaxMessage: 1 << 20, ServerReceiveMs: 10000}, func(context.Context, []byte, func([]byte)) error {
		calls.Add(1)
		return nil
	})
	for i := 0; i < 20; i++ {
		c := dial(t, url)
		connect(t, c)
		frame := "SEND\ndestination:/app/tuples/upload\ncontent-length:5000\n\n" + strings.Repeat("x", 1000)
		send(t, c, frame)
		switch i % 3 {
		case 0:
			c.Close() // vanishes without a close frame
		case 1:
			_ = c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, ""))
			c.Close()
		default:
			_ = c.UnderlyingConn().Close()
		}
	}
	waitSessions(t, srv, 0)
	if calls.Load() != 0 {
		t.Fatalf("incomplete frame reached the handler %d times", calls.Load())
	}
	healthy(t, url)
}

func TestMalformedFrameMidSessionOnlyAffectsThatSession(t *testing.T) {
	srv, url := startCfg(t, Config{MaxMessage: 1 << 20, ServerReceiveMs: 10000}, func(_ context.Context, p []byte, send func([]byte)) error {
		send(p)
		return nil
	})
	bystander := dial(t, url)
	subscribed(t, bystander)
	for _, bad := range []string{
		"SEND\nnocolonheader\n\n\x00",
		"SEND\ncontent-length:3\n\nabcd\x00", // body longer than declared
		"\x00\x00junk\n\n\x00",
		"BOGUS\n\n\x00",
	} {
		c := dial(t, url)
		connect(t, c)
		send(t, c, "SUBSCRIBE\nid:1\ndestination:/queue/response\n\n\x00")
		send(t, c, "SEND\ndestination:/queue/x\n\n\x00") // fine
		send(t, c, bad)
		if errs := expectClosed(t, c, 3*time.Second); len(errs) != 1 {
			t.Fatalf("%q: error frames %v", bad, errs)
		}
	}
	// bystander is untouched and still receives broadcasts
	good := dial(t, url)
	subscribed(t, good)
	send(t, good, "SEND\ndestination:/app/tuples/upload\n\nhello\x00")
	if m := read(t, bystander); m.Command != "MESSAGE" || string(m.Body) != "hello" {
		t.Fatalf("%+v", m)
	}
	waitSessions(t, srv, 2)
}

func TestSubscribeWithoutDestinationOrConnect(t *testing.T) {
	_, url := startCfg(t, Config{MaxMessage: 1 << 20, ServerReceiveMs: 10000}, noop)
	for _, fr := range []string{
		"SUBSCRIBE\nid:1\ndestination:/queue/response\n\n\x00", // before CONNECT
		"SEND\ndestination:/app/tuples/upload\n\nx\x00",
	} {
		c := dial(t, url)
		send(t, c, fr)
		if errs := expectClosed(t, c, 3*time.Second); len(errs) != 1 || errs[0] != "Not connected" {
			t.Fatalf("%q: %v", fr, errs)
		}
	}
	c := dial(t, url)
	connect(t, c)
	send(t, c, "SUBSCRIBE\nid:1\n\n\x00")
	if errs := expectClosed(t, c, 3*time.Second); len(errs) != 1 || errs[0] != "Missing destination" {
		t.Fatal(errs)
	}
	healthy(t, url)
}

func TestManyConcurrentSessions(t *testing.T) {
	const n = 60
	var calls atomic.Int32
	srv, url := startCfg(t, Config{MaxMessage: 1 << 20, ServerReceiveMs: 10000}, func(_ context.Context, p []byte, send func([]byte)) error {
		calls.Add(1)
		send(p)
		return nil
	})
	conns := make([]*websocket.Conn, n)
	for i := range conns {
		conns[i] = dial(t, url)
		subscribed(t, conns[i])
	}
	waitSessions(t, srv, n)
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i, c := range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// subscribed in connect(); the SUBSCRIBE of every session precedes the first SEND of its own
			if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("SEND\ndestination:/app/tuples/upload\n\nmsg-%d\x00", i))); err != nil {
				errs <- err
				return
			}
			seen := map[string]bool{}
			_ = c.SetReadDeadline(time.Now().Add(15 * time.Second))
			for len(seen) < n {
				_, d, err := c.ReadMessage()
				if err != nil {
					errs <- fmt.Errorf("session %d after %d messages: %w", i, len(seen), err)
					return
				}
				fr, _, err := parseFrames(d, 1<<20)
				if err != nil || len(fr) != 1 || fr[0].Command != "MESSAGE" {
					errs <- fmt.Errorf("session %d: bad frame %q", i, d)
					return
				}
				seen[string(fr[0].Body)] = true
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if calls.Load() != n {
		t.Fatalf("handler called %d times", calls.Load())
	}
	for _, c := range conns {
		c.Close()
	}
	waitSessions(t, srv, 0)
	healthy(t, url)
}

func TestSlowConsumerDoesNotStallOthers(t *testing.T) {
	payload := bytes.Repeat([]byte{'x'}, 1<<20)
	var send func([]byte)
	ready := make(chan struct{}, 1)
	srv, url := startCfg(t, Config{MaxMessage: 8 << 20, ServerReceiveMs: 10000, WriteTimeout: 200 * time.Millisecond}, func(_ context.Context, p []byte, s func([]byte)) error {
		send = s
		ready <- struct{}{}
		return nil
	})
	slow := dial(t, url)
	subscribed(t, slow) // subscribes and never reads again
	fast := dial(t, url)
	subscribed(t, fast)
	waitSessions(t, srv, 2)
	// shrink the slow peer's receive window so the server's writes block quickly
	if tc, ok := slow.UnderlyingConn().(interface{ SetReadBuffer(int) error }); ok {
		_ = tc.SetReadBuffer(4096)
	}
	send2 := func(msg string) {
		_ = fast.WriteMessage(websocket.TextMessage, []byte(msg))
		<-ready
	}
	send2("SEND\ndestination:/app/tuples/upload\n\nx\x00")

	const msgs = 40
	got := make(chan int, 1)
	go func() {
		n := 0
		_ = fast.SetReadDeadline(time.Now().Add(60 * time.Second))
		for n < msgs {
			_, d, err := fast.ReadMessage()
			if err != nil {
				break
			}
			if bytes.HasPrefix(d, []byte("MESSAGE")) {
				n++
			}
		}
		got <- n
	}()
	start := time.Now()
	for i := 0; i < msgs; i++ {
		send(payload)
	}
	select {
	case n := <-got:
		if n != msgs {
			t.Fatalf("healthy client received %d of %d", n, msgs)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("healthy client starved by the slow one")
	}
	t.Logf("%d MB delivered in %s with a stuck consumer", msgs, time.Since(start).Round(time.Millisecond))
	slow.Close()
	waitSessions(t, srv, 1)
	healthy(t, url)
}

func TestDuplicateSubscribeAndUnsubscribe(t *testing.T) {
	_, url := startCfg(t, Config{MaxMessage: 1 << 20, ServerReceiveMs: 10000}, func(_ context.Context, p []byte, s func([]byte)) error {
		s(p)
		return nil
	})
	c := dial(t, url)
	connect(t, c) // sub-0 on /queue/response
	send(t, c, "SUBSCRIBE\nid:sub-0\ndestination:/queue/response\nreceipt:r1\n\n\x00")
	read(t, c)
	send(t, c, "SEND\ndestination:/app/tuples/upload\n\nA\x00")
	if m := read(t, c); string(m.Body) != "A" {
		t.Fatal(m)
	}
	// the same subscription id twice must not deliver twice
	send(t, c, "SEND\ndestination:/app/tuples/upload\nreceipt:r2\n\nB\x00")
	got := []string{}
	_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	for {
		_, d, err := c.ReadMessage()
		if err != nil {
			break
		}
		fr, _, _ := parseFrames(d, 1<<20)
		got = append(got, fr[0].Command)
	}
	if fmt.Sprint(got) != "[RECEIPT MESSAGE]" && fmt.Sprint(got) != "[MESSAGE RECEIPT]" {
		t.Fatalf("frames after second SUBSCRIBE with the same id: %v", got)
	}
	// a second id on the same destination is a second subscription: both are served
	c = dial(t, url)
	connect(t, c)
	send(t, c, "SUBSCRIBE\nid:other\ndestination:/queue/response\nreceipt:r\n\n\x00")
	read(t, c)
	send(t, c, "SEND\ndestination:/app/tuples/upload\n\nC\x00")
	ids := map[string]bool{}
	for i := 0; i < 2; i++ {
		m := read(t, c)
		s, _ := m.Header("subscription")
		ids[s] = true
	}
	if !ids["sub-0"] || !ids["other"] {
		t.Fatal(ids)
	}
	// unsubscribe removes exactly that one; unknown ids are ignored
	send(t, c, "UNSUBSCRIBE\nid:sub-0\n\n\x00")
	send(t, c, "UNSUBSCRIBE\nid:nope\nreceipt:u\n\n\x00")
	read(t, c)
	send(t, c, "SEND\ndestination:/app/tuples/upload\n\nD\x00")
	m := read(t, c)
	if s, _ := m.Header("subscription"); s != "other" || string(m.Body) != "D" {
		t.Fatalf("%+v", m)
	}
}

func TestSendToUnknownDestinationAndBrokerDestination(t *testing.T) {
	var calls atomic.Int32
	_, url := startCfg(t, Config{MaxMessage: 1 << 20, ServerReceiveMs: 10000}, func(context.Context, []byte, func([]byte)) error {
		calls.Add(1)
		return nil
	})
	c := dial(t, url)
	connect(t, c)
	for _, dest := range []string{"/app/unknown", "", "/topic/x", "/app/tuples/upload/extra", "app/tuples/upload"} {
		send(t, c, "SEND\ndestination:"+dest+"\nreceipt:r\n\npayload\x00")
		if f := read(t, c); f.Command != "RECEIPT" {
			t.Fatalf("%q: %+v", dest, f)
		}
	}
	send(t, c, "SEND\nreceipt:r\n\npayload\x00") // no destination at all
	if f := read(t, c); f.Command != "RECEIPT" {
		t.Fatal(f)
	}
	if calls.Load() != 0 {
		t.Fatal("unknown destination reached the upload handler")
	}
	// messages to the broker prefix are relayed to its subscribers, not to the upload handler
	send(t, c, "SUBSCRIBE\nid:q\ndestination:/queue/custom\nreceipt:s\n\n\x00")
	read(t, c)
	send(t, c, "SEND\ndestination:/queue/custom\n\nrelay\x00")
	if m := read(t, c); m.Command != "MESSAGE" || string(m.Body) != "relay" {
		t.Fatal(m)
	}
	send(t, c, "FOO\n\n\x00")
	if errs := expectClosed(t, c, 3*time.Second); len(errs) != 1 || errs[0] != "Unknown command FOO" {
		t.Fatal(errs)
	}
	healthy(t, url)
}

// The server does not look at bearer tokens (the Spring service lets the API gateway do that):
// a missing, empty or garbage Authorization header on the handshake or in CONNECT is accepted.
func TestBearerTokenIsNotEnforced(t *testing.T) {
	_, url := startCfg(t, Config{MaxMessage: 1 << 20, ServerReceiveMs: 10000}, noop)
	for _, hdr := range []http.Header{nil, {"Authorization": {""}}, {"Authorization": {"Bearer"}}, {"Authorization": {"Bearer not-a-real-token"}}} {
		d := websocket.Dialer{Subprotocols: []string{"v12.stomp"}}
		c, _, err := d.Dial(url, hdr)
		if err != nil {
			t.Fatalf("%v: %v", hdr, err)
		}
		send(t, c, "CONNECT\naccept-version:1.2\nAuthorization:Bearer\n\n\x00")
		if f := read(t, c); f.Command != "CONNECTED" {
			t.Fatal(f)
		}
		c.Close()
	}
}

func TestNonWebSocketRequestsAndHandlerFailures(t *testing.T) {
	var n atomic.Int32
	srv, url := startCfg(t, Config{MaxMessage: 1 << 20, ServerReceiveMs: 10000}, func(_ context.Context, p []byte, s func([]byte)) error {
		switch string(p) {
		case "panic":
			panic("boom")
		case "error":
			return errors.New("handler failed")
		}
		n.Add(1)
		s(p)
		return nil
	})
	httpURL := "http" + strings.TrimPrefix(url, "ws")
	for _, m := range []string{"GET", "POST", "PUT"} {
		req, _ := http.NewRequest(m, httpURL, strings.NewReader("x"))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode < 400 || resp.StatusCode >= 500 {
			t.Fatalf("%s: status %d", m, resp.StatusCode)
		}
	}
	c := dial(t, url)
	connect(t, c)
	for _, p := range []string{"panic", "error", "ok"} {
		send(t, c, "SEND\ndestination:/app/tuples/upload\n\n"+p+"\x00")
	}
	if m := read(t, c); m.Command != "MESSAGE" || string(m.Body) != "ok" {
		t.Fatalf("session did not survive failing handlers: %+v", m)
	}
	waitSessions(t, srv, 1)
	healthy(t, url)
}
