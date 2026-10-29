package stomp

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Destinations (castor-common CastorWebSocketApiEndpoints).
const (
	UploadDestination   = "/app/tuples/upload"
	ResponseDestination = "/queue/response"
	brokerPrefix        = "/queue"
)

// UploadHandler processes one SEND to UploadDestination. send broadcasts a payload to every
// subscriber of ResponseDestination.
type UploadHandler func(ctx context.Context, payload []byte, send func([]byte)) error

// Config of the server.
type Config struct {
	MaxMessage      int           // WS message / STOMP buffer limit in bytes
	ServerSendMs    int           // heart-beat the server promises to send (Castor: 0)
	ServerReceiveMs int           // heart-beat the server wants to receive (Castor: 10000)
	WriteTimeout    time.Duration // per-write deadline
}

// Server accepts WebSocket connections speaking STOMP 1.2.
type Server struct {
	cfg      Config
	handler  UploadHandler
	log      *slog.Logger
	upgrader websocket.Upgrader
	mu       sync.RWMutex
	sessions map[*session]struct{}
	msgID    atomic.Uint64
}

// NewServer creates a server. Origins are not restricted (allowedOrigins "*").
func NewServer(cfg Config, h UploadHandler, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 30 * time.Second
	}
	s := &Server{cfg: cfg, handler: h, log: log, sessions: map[*session]struct{}{}}
	s.upgrader = websocket.Upgrader{
		CheckOrigin:     func(*http.Request) bool { return true },
		Subprotocols:    []string{"v12.stomp", "v11.stomp", "v10.stomp"},
		ReadBufferSize:  64 * 1024,
		WriteBufferSize: 64 * 1024,
	}
	return s
}

type subscription struct{ id, dest string }

type session struct {
	srv       *Server
	ws        *websocket.Conn
	wmu       sync.Mutex
	smu       sync.Mutex
	subs      []subscription
	connected bool
}

func (s *session) write(msgType int, data []byte) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_ = s.ws.SetWriteDeadline(time.Now().Add(s.srv.cfg.WriteTimeout))
	return s.ws.WriteMessage(msgType, data)
}

// ServeHTTP upgrades the request to a WebSocket and runs the STOMP session.
func (srv *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := srv.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already replied
	}
	ws.SetReadLimit(int64(srv.cfg.MaxMessage))
	s := &session{srv: srv, ws: ws}
	srv.mu.Lock()
	srv.sessions[s] = struct{}{}
	srv.mu.Unlock()
	defer func() {
		srv.mu.Lock()
		delete(srv.sessions, s)
		srv.mu.Unlock()
		ws.Close()
	}()
	s.run(r.Context())
}

func (s *session) run(ctx context.Context) {
	var buf []byte
	var readTimeout time.Duration
	for {
		if readTimeout > 0 {
			_ = s.ws.SetReadDeadline(time.Now().Add(readTimeout))
		}
		_, data, err := s.ws.ReadMessage()
		if err != nil {
			return
		}
		buf = append(buf, data...)
		frames, rest, err := parseFrames(buf, s.srv.cfg.MaxMessage)
		if err != nil {
			s.sendError("Failed to parse frame")
			return
		}
		buf = rest
		for _, f := range frames {
			if !s.handle(ctx, f, &readTimeout) {
				return
			}
		}
	}
}

func (s *session) sendError(msg string) {
	_ = s.write(websocket.TextMessage, encode("ERROR", [][2]string{{"message", msg}}, nil))
}

func (s *session) receipt(f *Frame) {
	if r, ok := f.Header("receipt"); ok {
		_ = s.write(websocket.TextMessage, encode("RECEIPT", [][2]string{{"receipt-id", r}}, nil))
	}
}

// handle returns false when the connection should be closed.
func (s *session) handle(ctx context.Context, f *Frame, readTimeout *time.Duration) bool {
	srv := s.srv
	switch f.Command {
	case "CONNECT", "STOMP":
		s.connected = true
		hb := [][2]string{{"version", "1.2"}, {"heart-beat", fmt.Sprintf("%d,%d", srv.cfg.ServerSendMs, srv.cfg.ServerReceiveMs)}}
		// Spring (SimpleBrokerMessageHandler): expect client heart-beats when both sides agree.
		if v, ok := f.Header("heart-beat"); ok {
			parts := strings.Split(v, ",")
			if len(parts) == 2 {
				cx, _ := strconv.Atoi(strings.TrimSpace(parts[0]))
				if cx > 0 && srv.cfg.ServerReceiveMs > 0 {
					*readTimeout = time.Duration(max(cx, srv.cfg.ServerReceiveMs)) * time.Millisecond * 3
				}
			}
		}
		return s.write(websocket.TextMessage, encode("CONNECTED", hb, nil)) == nil
	case "SUBSCRIBE":
		if !s.connected {
			s.sendError("Not connected")
			return false
		}
		id, _ := f.Header("id")
		dest, _ := f.Header("destination")
		if dest == "" {
			s.sendError("Missing destination")
			return false
		}
		s.smu.Lock()
		s.subs = append(s.subs, subscription{id, dest})
		s.smu.Unlock()
		s.receipt(f)
	case "UNSUBSCRIBE":
		id, _ := f.Header("id")
		s.smu.Lock()
		out := s.subs[:0]
		for _, sub := range s.subs {
			if sub.id != id {
				out = append(out, sub)
			}
		}
		s.subs = out
		s.smu.Unlock()
		s.receipt(f)
	case "SEND":
		if !s.connected {
			s.sendError("Not connected")
			return false
		}
		dest, _ := f.Header("destination")
		if dest == UploadDestination {
			body := f.Body
			go func() {
				if err := srv.handler(ctx, body, srv.broadcast); err != nil {
					srv.log.Warn("upload failed", "err", err)
				}
			}()
		} else if strings.HasPrefix(dest, brokerPrefix+"/") {
			srv.deliver(dest, f.Body)
		} else {
			srv.log.Debug("no handler for destination", "dest", dest)
		}
		s.receipt(f)
	case "DISCONNECT":
		s.receipt(f)
		return false
	case "ACK", "NACK", "BEGIN", "COMMIT", "ABORT":
		s.receipt(f)
	default:
		s.sendError("Unknown command " + f.Command)
		return false
	}
	return true
}

func (srv *Server) broadcast(payload []byte) { srv.deliver(ResponseDestination, payload) }

// deliver sends a MESSAGE to every subscriber of dest.
func (srv *Server) deliver(dest string, payload []byte) {
	srv.mu.RLock()
	ss := make([]*session, 0, len(srv.sessions))
	for s := range srv.sessions {
		ss = append(ss, s)
	}
	srv.mu.RUnlock()
	for _, s := range ss {
		s.smu.Lock()
		subs := append([]subscription(nil), s.subs...)
		s.smu.Unlock()
		for _, sub := range subs {
			if sub.dest != dest {
				continue
			}
			frame := encode("MESSAGE", [][2]string{
				{"destination", dest}, {"content-type", "application/octet-stream"},
				{"subscription", sub.id}, {"message-id", fmt.Sprintf("%d", srv.msgID.Add(1))}}, payload)
			if err := s.write(websocket.BinaryMessage, frame); err != nil {
				srv.log.Warn("delivery failed", "err", err)
			}
		}
	}
}
