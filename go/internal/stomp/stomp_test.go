package stomp

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestParseFrames(t *testing.T) {
	in := []byte("\nCONNECT\naccept-version:1.2\nheart-beat:10000,0\n\n\x00\nSEND\ndestination:/app/x\ncontent-length:3\n\na\x00b\x00SUBSCRIBE\nid:s\\c1\ndestination:/queue/response\n\n\x00SEN")
	frames, rest, err := parseFrames(in, 1<<20)
	if err != nil || len(frames) != 3 || string(rest) != "SEN" {
		t.Fatal(len(frames), string(rest), err)
	}
	if string(frames[1].Body) != "a\x00b" {
		t.Fatalf("%q", frames[1].Body)
	}
	if id, _ := frames[2].Header("id"); id != "s:1" {
		t.Fatal(id)
	}
	if h, _ := frames[0].Header("heart-beat"); h != "10000,0" {
		t.Fatal(h)
	}
	if _, _, err := parseFrames([]byte("SEND\nnocolon\n\n\x00"), 100); err == nil {
		t.Fatal("expected malformed")
	}
	if _, _, err := parseFrames([]byte("SEND\ncontent-length:5000\n\n"), 100); err == nil {
		t.Fatal("expected size error")
	}
}

type env struct {
	url string
	got chan []byte
}

func start(t *testing.T, h UploadHandler) string {
	srv := NewServer(Config{MaxMessage: 1 << 20, ServerReceiveMs: 10000}, h, nil)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return "ws" + strings.TrimPrefix(ts.URL, "http")
}

func dial(t *testing.T, url string) *websocket.Conn {
	d := websocket.Dialer{Subprotocols: []string{"v12.stomp"}}
	c, _, err := d.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Subprotocol() != "v12.stomp" {
		t.Fatal("subprotocol", c.Subprotocol())
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func read(t *testing.T, c *websocket.Conn) *Frame {
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, d, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	fr, _, err := parseFrames(d, 1<<20)
	if err != nil || len(fr) != 1 {
		t.Fatal(err, len(fr))
	}
	return fr[0]
}

func connect(t *testing.T, c *websocket.Conn) {
	_ = c.WriteMessage(websocket.TextMessage, []byte("CONNECT\naccept-version:1.2\nheart-beat:10000,0\nAuthorization:Bearer abc\n\n\x00"))
	f := read(t, c)
	if v, _ := f.Header("version"); f.Command != "CONNECTED" || v != "1.2" {
		t.Fatal(f)
	}
	if v, _ := f.Header("heart-beat"); v != "0,10000" {
		t.Fatal(v)
	}
	_ = c.WriteMessage(websocket.TextMessage, []byte("SUBSCRIBE\nid:sub-0\ndestination:/queue/response\n\n\x00"))
}

func TestUploadBroadcast(t *testing.T) {
	var payloads = make(chan []byte, 4)
	url := start(t, func(ctx context.Context, p []byte, send func([]byte)) error {
		payloads <- p
		send([]byte("reply:" + string(p)))
		return nil
	})
	a, b := dial(t, url), dial(t, url)
	connect(t, a)
	connect(t, b)
	time.Sleep(100 * time.Millisecond)
	body := []byte("bin\x00ary\n\n")
	frame := append([]byte("SEND\ndestination:/app/tuples/upload\ncontent-type:application/octet-stream\ncontent-length:9\n\n"), body...)
	frame = append(frame, 0)
	// split across two WS messages to exercise buffering
	_ = a.WriteMessage(websocket.BinaryMessage, frame[:30])
	_ = a.WriteMessage(websocket.BinaryMessage, frame[30:])
	select {
	case p := <-payloads:
		if string(p) != string(body) {
			t.Fatalf("%q", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no payload")
	}
	for _, c := range []*websocket.Conn{a, b} { // broadcast to ALL subscribers
		m := read(t, c)
		if m.Command != "MESSAGE" || string(m.Body) != "reply:"+string(body) {
			t.Fatalf("%+v", m)
		}
		if d, _ := m.Header("destination"); d != "/queue/response" {
			t.Fatal(d)
		}
		if s, _ := m.Header("subscription"); s != "sub-0" {
			t.Fatal(s)
		}
	}
}

func TestReceiptDisconnectAndErrors(t *testing.T) {
	url := start(t, func(context.Context, []byte, func([]byte)) error { return nil })
	c := dial(t, url)
	_ = c.WriteMessage(websocket.TextMessage, []byte("CONNECT\naccept-version:1.2\n\n\x00"))
	read(t, c)
	_ = c.WriteMessage(websocket.TextMessage, []byte("SUBSCRIBE\nid:1\ndestination:/queue/response\nreceipt:r1\n\n\x00"))
	if f := read(t, c); f.Command != "RECEIPT" {
		t.Fatal(f)
	}
	_ = c.WriteMessage(websocket.TextMessage, []byte("\n")) // heart-beat
	_ = c.WriteMessage(websocket.TextMessage, []byte("DISCONNECT\nreceipt:r2\n\n\x00"))
	if f := read(t, c); f.Command != "RECEIPT" {
		t.Fatal(f)
	}
	c2 := dial(t, url)
	_ = c2.WriteMessage(websocket.TextMessage, []byte("SEND\ndestination:/app/tuples/upload\n\nx\x00"))
	if f := read(t, c2); f.Command != "ERROR" {
		t.Fatal("expected ERROR when not connected")
	}
}
