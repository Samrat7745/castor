// Package httpapi exposes the REST endpoints and actuator, matching the Spring controllers.
package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/carbynestack/castor-go/internal/model"
	"github.com/carbynestack/castor-go/internal/service"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Backend is what the handlers need from the service layer.
type Backend interface {
	GetTuples(ctx context.Context, tt model.TupleType, count int64, requestID string) ([]byte, error)
	ActivateChunk(ctx context.Context, chunkID string) error
	Telemetry(ctx context.Context, interval time.Duration) (*model.TelemetryData, error)
	ApplyReservation(ctx context.Context, r *model.Reservation) error
	UpdateReservation(ctx context.Context, id string, status model.ActivationStatus) error
}

// HealthCheck reports whether a dependency is reachable.
type HealthCheck func(ctx context.Context) error

// Info is the static /actuator/info content.
type Info struct{ Name, Description, GroupID, ArtifactID, Version string }

// New builds the HTTP handler. extra routes (e.g. the WebSocket endpoint) can be added to the mux.
func New(b Backend, checks []HealthCheck, info Info, mux *http.ServeMux, log *slog.Logger) http.Handler {
	if mux == nil {
		mux = http.NewServeMux()
	}
	if log == nil {
		log = slog.Default()
	}
	a := &api{b: b, log: log}
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reqs := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "http_server_requests_total", Help: "HTTP requests."}, []string{"method", "status"})
	reg.MustRegister(reqs)

	mux.HandleFunc("GET /intra-vcp/tuples", a.getTuples)
	mux.HandleFunc("PUT /intra-vcp/tuple-chunks/activate/{chunkId}", a.activate)
	mux.HandleFunc("GET /intra-vcp/telemetry", a.telemetry)
	mux.HandleFunc("POST /inter-vcp/reservation", a.reserve)
	mux.HandleFunc("PUT /inter-vcp/reservation/{reservationId}", a.updateReservation)
	mux.HandleFunc("GET /actuator/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.spring-boot.actuator.v3+json")
		for _, c := range checks {
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			err := c(ctx)
			cancel()
			if err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"status":"DOWN"}`))
				return
			}
		}
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	})
	mux.HandleFunc("GET /actuator/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.spring-boot.actuator.v3+json")
		_ = json.NewEncoder(w).Encode(map[string]string{"name": info.Name, "description": info.Description,
			"groupId": info.GroupID, "artifactId": info.ArtifactID, "version": info.Version, "apiVersion": info.Version})
	})
	mux.Handle("GET /actuator/prometheus", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return &counting{h: springRoutes(mux), c: reqs}
}

type counting struct {
	h http.Handler
	c *prometheus.CounterVec
}
type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(c int) { s.code = c; s.ResponseWriter.WriteHeader(c) }

// Hijack lets the WebSocket upgrader take over the connection.
func (s *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(s.ResponseWriter).Hijack()
}
func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func (c *counting) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sw := &statusWriter{ResponseWriter: w, code: 200}
	c.h.ServeHTTP(sw, r)
	c.c.WithLabelValues(r.Method, strconv.Itoa(sw.code)).Inc()
}

type api struct {
	b   Backend
	log *slog.Logger
}

// writeError mimics RestControllerExceptionHandler: body is the JSON-encoded message.
func (a *api) writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	var br *service.BadRequestError
	if errors.As(err, &br) {
		status = http.StatusBadRequest
	}
	if status == 500 {
		a.log.Error("request failed", "err", err)
	}
	body, _ := json.Marshal(err.Error())
	w.Header().Set("Content-Type", "text/plain;charset=UTF-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// badBinding is Spring's default handling of binding failures: 400 with empty body.
func badBinding(w http.ResponseWriter) { w.WriteHeader(http.StatusBadRequest) }

func isUUID(s string) bool { return validUUID(s) }

func (a *api) getTuples(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if !q.Has("tupletype") || !q.Has("count") || !q.Has("reservationId") {
		badBinding(w)
		return
	}
	count, err := strconv.ParseInt(q.Get("count"), 10, 64)
	if err != nil || !isUUID(q.Get("reservationId")) {
		badBinding(w)
		return
	}
	if count <= 0 {
		a.writeError(w, &service.BadRequestError{Msg: "The number of requested Multiplication Triples has to be 1 or greater."})
		return
	}
	tt, err := model.ParseTupleType(q.Get("tupletype"))
	if err != nil {
		a.writeError(w, &service.BadRequestError{Msg: err.Error()})
		return
	}
	data, err := a.b.GetTuples(r.Context(), tt, count, q.Get("reservationId"))
	if err != nil {
		a.writeError(w, err)
		return
	}
	var buf bytes.Buffer
	if err := model.WriteTupleListJSON(&buf, tt, data); err != nil {
		a.writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(buf.Bytes())
}

func (a *api) activate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("chunkId")
	if !isUUID(id) {
		badBinding(w)
		return
	}
	if err := a.b.ActivateChunk(r.Context(), id); err != nil {
		a.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *api) telemetry(w http.ResponseWriter, r *http.Request) {
	interval := 60 * time.Second
	if r.URL.Query().Has("interval") {
		v, err := strconv.ParseInt(r.URL.Query().Get("interval"), 10, 64)
		if err != nil {
			badBinding(w)
			return
		}
		if v <= 0 {
			a.writeError(w, &service.BadRequestError{Msg: "interval must be larger than 0"})
			return
		}
		interval = time.Duration(v) * time.Second
	}
	td, err := a.b.Telemetry(r.Context(), interval)
	if err != nil {
		a.writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(td)
}

func (a *api) reserve(w http.ResponseWriter, r *http.Request) {
	if !jsonContentType(r) {
		w.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	var res model.Reservation
	if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
		badBinding(w)
		return
	}
	if err := a.b.ApplyReservation(r.Context(), &res); err != nil {
		a.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (a *api) updateReservation(w http.ResponseWriter, r *http.Request) {
	if !jsonContentType(r) {
		w.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	var s string
	if err := json.NewDecoder(r.Body).Decode(&s); err != nil || (s != "LOCKED" && s != "UNLOCKED") {
		badBinding(w)
		return
	}
	if err := a.b.UpdateReservation(r.Context(), r.PathValue("reservationId"), model.ActivationStatus(s)); err != nil {
		a.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func validUUID(s string) bool { return model.IsUUID(s) }

// jsonContentType mirrors Spring's @RequestBody check (missing/other media type -> 415).
func jsonContentType(r *http.Request) bool {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && (mt == "application/json" || strings.HasPrefix(mt, "application/") && strings.HasSuffix(mt, "+json"))
}

// probe captures the status the mux would answer with, without writing.
type probe struct {
	h    http.Header
	code int
}

func (p *probe) Header() http.Header         { return p.h }
func (p *probe) Write(b []byte) (int, error) { return len(b), nil }
func (p *probe) WriteHeader(c int)           { p.code = c }

// springRoutes replaces Go's plain-text 404/405 with Spring's: JSON error body for 404, empty body for 405.
func springRoutes(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, pattern := mux.Handler(r)
		if pattern != "" {
			mux.ServeHTTP(w, r)
			return
		}
		p := &probe{h: http.Header{}, code: 404}
		h.ServeHTTP(p, r)
		if p.code == http.StatusMethodNotAllowed {
			if a := p.h.Get("Allow"); a != "" {
				w.Header().Set("Allow", a)
			}
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if p.code != http.StatusNotFound {
			mux.ServeHTTP(w, r)
			return
		}
		body, _ := json.Marshal(map[string]any{"timestamp": time.Now().UTC().Format("2006-01-02T15:04:05.000+00:00"),
			"status": 404, "error": "Not Found", "path": r.URL.Path})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write(body)
	})
}
