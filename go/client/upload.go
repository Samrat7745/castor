package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Samrat7745/castor/go/internal/javaser"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Constants and messages mirror DefaultCastorUploadClient.
const (
	MaximumNumberOfTuples     = 100000
	DefaultConnectionTimeout  = 60 * time.Second
	DefaultClientHeartbeat    = 10 * time.Second
	TooManyTuplesMsg          = "The maximum number of tuples is exceeded: actual=%d, allowed=%d"
	FailedActivateChunkMsg    = "Failed to activate TupleChunk"
	WebSocketConnectionFailed = "WebSocket connection to CastorServices failed"
	NotConnectedMsg           = "Connection not established for \"%s\""
	uploadDestination         = "/app/tuples/upload"
	responseDestination       = "/queue/response"
)

// TupleChunk is a batch of tuples of one type that is uploaded to Castor.
type TupleChunk struct {
	ChunkID   uuid.UUID
	TupleType TupleType
	// Tuples is the raw tuple data: value and MAC words of all shares of all tuples, in order.
	Tuples []byte
}

// NumberOfTuples is the number of whole tuples in the chunk.
func (c TupleChunk) NumberOfTuples() int {
	ts := c.TupleType.TupleSize()
	if ts == 0 {
		return 0
	}
	return len(c.Tuples) / ts
}

// UploadClient uploads tuple chunks over the STOMP/WebSocket API and activates them (Java CastorUploadClient).
type UploadClient interface {
	// Connect opens the WebSocket session; it fails with a *ClientError if no session is established within timeout
	// (0 = DefaultConnectionTimeout).
	Connect(ctx context.Context, timeout time.Duration) error
	Disconnect() error
	// UploadTupleChunk sends the chunk and waits for Castor's response. It returns false when Castor reports a
	// failure or no response arrives within timeout (0 = DefaultConnectionTimeout); it errors if not connected.
	UploadTupleChunk(ctx context.Context, chunk TupleChunk, timeout time.Duration) (bool, error)
	// ActivateTupleChunk makes an uploaded chunk available for consumption (PUT /intra-vcp/tuple-chunks/activate/{id}).
	ActivateTupleChunk(ctx context.Context, chunkID uuid.UUID) error
}

type upload struct {
	opt   Options
	base  string
	wsURL string
	http  *http.Client
	tlsc  *tls.Config

	mu      sync.Mutex
	conn    *websocket.Conn
	writeMu sync.Mutex
	pending map[uuid.UUID]chan bool
}

// NewUploadClient builds an upload client for the first address in o.ServiceAddresses.
func NewUploadClient(o Options) (UploadClient, error) {
	bases, err := o.bases()
	if err != nil {
		return nil, err
	}
	hc, err := o.httpClient()
	if err != nil {
		return nil, err
	}
	u, _ := url.Parse(bases[0])
	scheme := "ws"
	if u.Scheme == "https" {
		scheme = "wss"
	}
	ws := scheme + "://" + u.Host + strings.TrimSuffix(u.Path, "/") + "/intra-vcp/ws"
	return &upload{opt: o, base: bases[0], wsURL: ws, http: hc,
		tlsc: hc.Transport.(*http.Transport).TLSClientConfig, pending: map[uuid.UUID]chan bool{}}, nil
}

func (c *upload) Connect(ctx context.Context, timeout time.Duration) error {
	c.mu.Lock()
	if c.conn != nil {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	if timeout == 0 {
		timeout = DefaultConnectionTimeout
	}
	hdr := http.Header{}
	if c.opt.BearerTokenProvider != nil {
		hdr.Set("Authorization", "Bearer "+c.opt.BearerTokenProvider(c.base))
	}
	d := websocket.Dialer{Subprotocols: []string{"v12.stomp"}, HandshakeTimeout: timeout, TLSClientConfig: c.tlsc,
		ReadBufferSize: 1 << 16, WriteBufferSize: 1 << 16}
	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ws, _, err := d.DialContext(dctx, c.wsURL, hdr)
	if err != nil {
		return &ClientError{WebSocketConnectionFailed, err}
	}
	ws.SetReadLimit(64 << 20)
	u, _ := url.Parse(c.base)
	conn := stompFrame{"CONNECT", [][2]string{{"accept-version", "1.2,1.1,1.0"}, {"heart-beat", "10000,0"}, {"host", u.Hostname()}}, nil}
	if c.opt.BearerTokenProvider != nil {
		conn.headers = append(conn.headers, [2]string{"Authorization", "Bearer " + c.opt.BearerTokenProvider(c.base)})
	}
	_ = ws.SetWriteDeadline(time.Now().Add(timeout))
	if err := ws.WriteMessage(websocket.TextMessage, conn.encode()); err != nil {
		ws.Close()
		return &ClientError{WebSocketConnectionFailed, err}
	}
	_ = ws.SetReadDeadline(time.Now().Add(timeout))
	_, msg, err := ws.ReadMessage()
	if err != nil {
		ws.Close()
		return &ClientError{WebSocketConnectionFailed, err}
	}
	if f, err := parseStomp(msg); err != nil || f.command != "CONNECTED" {
		ws.Close()
		return &ClientError{WebSocketConnectionFailed, fmt.Errorf("expected CONNECTED, got %q", msg)}
	}
	_ = ws.SetReadDeadline(time.Time{})
	_ = ws.SetWriteDeadline(time.Time{})
	sub := stompFrame{"SUBSCRIBE", [][2]string{{"id", "sub-0"}, {"destination", responseDestination}}, nil}
	if err := ws.WriteMessage(websocket.TextMessage, sub.encode()); err != nil {
		ws.Close()
		return &ClientError{WebSocketConnectionFailed, err}
	}
	c.mu.Lock()
	c.conn = ws
	c.mu.Unlock()
	go c.readLoop(ws)
	go c.heartbeat(ws)
	return nil
}

func (c *upload) heartbeat(ws *websocket.Conn) {
	t := time.NewTicker(DefaultClientHeartbeat)
	defer t.Stop()
	for range t.C {
		c.mu.Lock()
		cur := c.conn
		c.mu.Unlock()
		if cur != ws {
			return
		}
		c.writeMu.Lock()
		err := ws.WriteMessage(websocket.TextMessage, []byte("\n"))
		c.writeMu.Unlock()
		if err != nil {
			return
		}
	}
}

func (c *upload) readLoop(ws *websocket.Conn) {
	for {
		_, m, err := ws.ReadMessage()
		if err != nil {
			c.mu.Lock()
			if c.conn == ws {
				c.conn = nil
			}
			c.mu.Unlock()
			return
		}
		if len(bytes.Trim(m, "\n")) == 0 {
			continue
		}
		f, err := parseStomp(m)
		if err != nil || f.command != "MESSAGE" {
			continue
		}
		resp, err := javaser.DecodeUploadResponse(f.body)
		if err != nil || resp.ChunkID == "" {
			continue
		}
		id, err := uuid.Parse(resp.ChunkID)
		if err != nil {
			continue
		}
		c.mu.Lock()
		ch := c.pending[id]
		c.mu.Unlock()
		if ch != nil {
			select {
			case ch <- resp.Success:
			default:
			}
		}
	}
}

func (c *upload) Disconnect() error {
	c.mu.Lock()
	ws := c.conn
	c.conn = nil
	c.mu.Unlock()
	if ws == nil {
		return nil
	}
	c.writeMu.Lock()
	_ = ws.WriteMessage(websocket.TextMessage, stompFrame{command: "DISCONNECT"}.encode())
	c.writeMu.Unlock()
	return ws.Close()
}

func (c *upload) UploadTupleChunk(ctx context.Context, chunk TupleChunk, timeout time.Duration) (bool, error) {
	if n := chunk.NumberOfTuples(); n > MaximumNumberOfTuples {
		return false, &ClientError{Msg: fmt.Sprintf(TooManyTuplesMsg, n, MaximumNumberOfTuples)}
	}
	if timeout == 0 {
		timeout = DefaultConnectionTimeout
	}
	c.mu.Lock()
	ws := c.conn
	if ws == nil {
		c.mu.Unlock()
		return false, &ClientError{Msg: fmt.Sprintf(NotConnectedMsg, c.wsURL)}
	}
	ch := make(chan bool, 1)
	c.pending[chunk.ChunkID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, chunk.ChunkID)
		c.mu.Unlock()
	}()
	payload := javaser.EncodeTupleChunk(chunk.ChunkID.String(), chunk.TupleType.Name(), chunk.Tuples)
	f := stompFrame{"SEND", [][2]string{{"destination", uploadDestination}, {"content-type", "application/octet-stream"},
		{"content-length", strconv.Itoa(len(payload))}}, payload}
	c.writeMu.Lock()
	err := ws.WriteMessage(websocket.BinaryMessage, f.encode())
	c.writeMu.Unlock()
	if err != nil {
		return false, &ClientError{Msg: fmt.Sprintf(NotConnectedMsg, c.wsURL), Cause: err}
	}
	select {
	case ok := <-ch:
		return ok, nil
	case <-time.After(timeout):
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (c *upload) ActivateTupleChunk(ctx context.Context, chunkID uuid.UUID) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.base+"/intra-vcp/tuple-chunks/activate/"+chunkID.String(), nil)
	if err != nil {
		return &ClientError{FailedActivateChunkMsg, err}
	}
	c.opt.auth(c.base, req.Header)
	resp, err := c.http.Do(req)
	if err != nil {
		return &ClientError{FailedActivateChunkMsg, err}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &ClientError{FailedActivateChunkMsg, errors.New("HTTP " + strconv.Itoa(resp.StatusCode) + ": " + strings.TrimSpace(string(body)))}
	}
	return nil
}

// stompFrame is a STOMP 1.2 frame (what Spring's WebSocketStompClient exchanges, without SockJS).
type stompFrame struct {
	command string
	headers [][2]string
	body    []byte
}

func (f stompFrame) header(k string) string {
	for _, h := range f.headers {
		if h[0] == k {
			return h[1]
		}
	}
	return ""
}

func (f stompFrame) encode() []byte {
	var b bytes.Buffer
	b.WriteString(f.command + "\n")
	for _, h := range f.headers {
		b.WriteString(h[0] + ":" + h[1] + "\n")
	}
	b.WriteString("\n")
	b.Write(f.body)
	b.WriteByte(0)
	return b.Bytes()
}

func parseStomp(raw []byte) (stompFrame, error) {
	var f stompFrame
	raw = bytes.TrimLeft(raw, "\n")
	end := bytes.Index(raw, []byte("\n\n"))
	if end < 0 {
		return f, fmt.Errorf("bad frame %q", raw)
	}
	lines := strings.Split(string(raw[:end]), "\n")
	f.command = lines[0]
	for _, l := range lines[1:] {
		if k, v, ok := strings.Cut(l, ":"); ok {
			f.headers = append(f.headers, [2]string{k, v})
		}
	}
	body := raw[end+2:]
	if n, err := strconv.Atoi(f.header("content-length")); err == nil && n >= 0 && n <= len(body) {
		body = body[:n]
	} else {
		body = bytes.TrimSuffix(body, []byte{0})
	}
	f.body = body
	return f, nil
}
