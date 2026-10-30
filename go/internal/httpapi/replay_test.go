package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/carbynestack/castor-go/internal/model"
)

// Replay of the recorded Java parity fixtures (parity/castor/fixtures) that do not depend on
// service state: validation errors (400), routing errors (404/405/415) and the actuator
// endpoints. Fixtures are copied to testdata/parity; the backend must never be reached.
// Fixtures with state (downloads, telemetry, reservations) are replayed by the parity suite itself
// against the Go rig.

type fixture struct {
	Name    string `json:"name"`
	Request struct {
		Method  string            `json:"method"`
		Path    string            `json:"path"`
		Headers map[string]string `json:"headers"`
		Body    any               `json:"body"`
	} `json:"request"`
	Response struct {
		Status      int    `json:"status"`
		ContentType string `json:"contentType"`
		Body        any    `json:"body"`
		BodyIsJSON  bool   `json:"bodyIsJson"`
	} `json:"response"`
}

type untouchable struct{ t *testing.T }

func (u untouchable) fail() error { u.t.Error("backend reached"); return errors.New("backend reached") }
func (u untouchable) GetTuples(context.Context, model.TupleType, int64, string) ([]byte, error) {
	return nil, u.fail()
}
func (u untouchable) ActivateChunk(context.Context, string) error { return u.fail() }
func (u untouchable) Telemetry(context.Context, time.Duration) (*model.TelemetryData, error) {
	return nil, u.fail()
}
func (u untouchable) ApplyReservation(context.Context, *model.Reservation) error { return u.fail() }
func (u untouchable) UpdateReservation(context.Context, string, model.ActivationStatus) error {
	return u.fail()
}

func TestReplayRecordedJavaFixtures(t *testing.T) {
	files, _ := filepath.Glob("testdata/parity/*.json")
	if len(files) < 30 {
		t.Fatalf("only %d fixtures", len(files))
	}
	h := New(untouchable{t}, []HealthCheck{func(context.Context) error { return nil }}, Info{Name: "castor-service", Description: "@project.description@", GroupID: "io.carbynestack", ArtifactID: "castor-service", Version: "0.3.0"}, nil, nil)
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var fx fixture
		if err := json.Unmarshal(raw, &fx); err != nil {
			t.Fatal(f, err)
		}
		t.Run(fx.Name, func(t *testing.T) {
			var body io.Reader
			switch b := fx.Request.Body.(type) {
			case nil:
			case string:
				body = strings.NewReader(b)
			default:
				enc, _ := json.Marshal(b)
				body = strings.NewReader(string(enc))
			}
			r := httptest.NewRequest(fx.Request.Method, "http://castor"+fx.Request.Path, body)
			for k, v := range fx.Request.Headers {
				r.Header.Set(k, v)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			res := w.Result()
			got, _ := io.ReadAll(res.Body)
			if res.StatusCode != fx.Response.Status {
				t.Fatalf("status %d, recorded %d (body %q)", res.StatusCode, fx.Response.Status, got)
			}
			if ct := fx.Response.ContentType; ct != "" && res.Header.Get("Content-Type") != ct {
				t.Fatalf("content type %q, recorded %q", res.Header.Get("Content-Type"), ct)
			}
			if !fx.Response.BodyIsJSON {
				if string(got) != "" {
					t.Fatalf("body %q, recorded empty", got)
				}
				return
			}
			var gotJSON any
			if err := json.Unmarshal(got, &gotJSON); err != nil {
				t.Fatalf("body is not JSON: %q", got)
			}
			if m, ok := gotJSON.(map[string]any); ok {
				delete(m, "timestamp") // volatile, like the harness
			}
			if !reflect.DeepEqual(gotJSON, fx.Response.Body) {
				t.Fatalf("body %v, recorded %v", gotJSON, fx.Response.Body)
			}
		})
	}
}
