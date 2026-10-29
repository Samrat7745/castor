package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/carbynestack/castor-go/internal/model"
	"github.com/carbynestack/castor-go/internal/service"
)

type fake struct {
	err     error
	data    []byte
	applied *model.Reservation
	status  model.ActivationStatus
	id      string
	iv      time.Duration
}

func (f *fake) GetTuples(ctx context.Context, tt model.TupleType, c int64, r string) ([]byte, error) {
	return f.data, f.err
}
func (f *fake) ActivateChunk(ctx context.Context, id string) error { f.id = id; return f.err }
func (f *fake) Telemetry(ctx context.Context, iv time.Duration) (*model.TelemetryData, error) {
	f.iv = iv
	return &model.TelemetryData{Metrics: []model.TupleMetric{{Available: 1, ConsumptionRate: 2, Type: "BIT_GFP"}}, Interval: iv.Milliseconds()}, f.err
}
func (f *fake) ApplyReservation(ctx context.Context, r *model.Reservation) error {
	f.applied = r
	return f.err
}
func (f *fake) UpdateReservation(ctx context.Context, id string, s model.ActivationStatus) error {
	f.id, f.status = id, s
	return f.err
}

func do(h http.Handler, method, url, body string) (int, string, http.Header) {
	r := httptest.NewRequest(method, url, strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	b, _ := io.ReadAll(w.Result().Body)
	return w.Code, string(b), w.Header()
}

const uid = "123e4567-e89b-12d3-a456-426614174000"

func setup(f *fake, checks ...HealthCheck) http.Handler {
	return New(f, checks, Info{Name: "castor-service", Version: "0.3.0"}, nil, nil)
}

func TestTuples(t *testing.T) {
	raw, _ := os.ReadFile("../javaser/testdata/tuplelist_bit_gf2n.raw")
	want, _ := os.ReadFile("../javaser/testdata/tuplelist_bit_gf2n.json")
	f := &fake{data: raw}
	h := setup(f)
	c, b, hd := do(h, "GET", "/intra-vcp/tuples?tupletype=BIT_GF2N&count=2&reservationId="+uid, "")
	if c != 200 || b != string(want) || hd.Get("Content-Type") != "application/json" {
		t.Fatal(c, b)
	}
	cases := []struct {
		url  string
		code int
		body string
	}{
		{"/intra-vcp/tuples?tupletype=BIT_GF2N&count=2", 400, ""},
		{"/intra-vcp/tuples?tupletype=BIT_GF2N&count=x&reservationId=" + uid, 400, ""},
		{"/intra-vcp/tuples?tupletype=BIT_GF2N&count=1&reservationId=nope", 400, ""},
		{"/intra-vcp/tuples?tupletype=BIT_GF2N&count=0&reservationId=" + uid, 400, `"The number of requested Multiplication Triples has to be 1 or greater."`},
		{"/intra-vcp/tuples?tupletype=bit_gf2n&count=1&reservationId=" + uid, 400, `"No enum constant io.carbynestack.castor.common.entities.TupleType.bit_gf2n"`},
	}
	for _, c := range cases {
		code, body, _ := do(h, "GET", c.url, "")
		if code != c.code || body != c.body {
			t.Errorf("%s: %d %q", c.url, code, body)
		}
	}
	f.err = errors.New("boom")
	code, body, hd := do(h, "GET", "/intra-vcp/tuples?tupletype=BIT_GF2N&count=2&reservationId="+uid, "")
	if code != 500 || body != `"boom"` || hd.Get("Content-Type") != "text/plain;charset=UTF-8" {
		t.Fatal(code, body, hd)
	}
	f.err = &service.BadRequestError{Msg: "bad"}
	if code, _, _ = do(h, "GET", "/intra-vcp/tuples?tupletype=BIT_GF2N&count=2&reservationId="+uid, ""); code != 500 {
		// BadRequestError from the service layer maps to 400
		if code != 400 {
			t.Fatal(code)
		}
	}
}

func TestActivateTelemetry(t *testing.T) {
	f := &fake{}
	h := setup(f)
	if c, _, _ := do(h, "PUT", "/intra-vcp/tuple-chunks/activate/"+uid, ""); c != 200 || f.id != uid {
		t.Fatal(c)
	}
	if c, _, _ := do(h, "PUT", "/intra-vcp/tuple-chunks/activate/zzz", ""); c != 400 {
		t.Fatal(c)
	}
	f.err = errors.New(service.MsgNoFragmentForChunk)
	if c, b, _ := do(h, "PUT", "/intra-vcp/tuple-chunks/activate/"+uid, ""); c != 500 || b != `"Not a single fragment associated with the given identifier."` {
		t.Fatal(c, b)
	}
	f.err = nil
	if c, b, _ := do(h, "GET", "/intra-vcp/telemetry", ""); c != 200 || f.iv != 60*time.Second || !strings.HasSuffix(b, `"interval":60000}`+"\n") {
		t.Fatal(c, b)
	}
	if _, _, _ = do(h, "GET", "/intra-vcp/telemetry?interval=7", ""); f.iv != 7*time.Second {
		t.Fatal(f.iv)
	}
	if c, b, _ := do(h, "GET", "/intra-vcp/telemetry?interval=-1", ""); c != 400 || b != `"interval must be larger than 0"` {
		t.Fatal(c, b)
	}
	if c, b, _ := do(h, "GET", "/intra-vcp/telemetry?interval=abc", ""); c != 400 || b != "" {
		t.Fatal(c, b)
	}
}

func TestReservationEndpoints(t *testing.T) {
	f := &fake{}
	h := setup(f)
	body, _ := os.ReadFile("../javaser/testdata/reservation.json")
	if c, b, _ := do(h, "POST", "/inter-vcp/reservation", string(body)); c != 201 || b != "" || f.applied == nil || f.applied.ReservationID == "" {
		t.Fatal(c, b)
	}
	if c, _, _ := do(h, "POST", "/inter-vcp/reservation", `{"reservationId":"x"}`); c != 400 {
		t.Fatal(c)
	}
	if c, _, _ := do(h, "PUT", "/inter-vcp/reservation/"+uid+"_bit_gfp", `"UNLOCKED"`); c != 200 || f.status != model.Unlocked || f.id != uid+"_bit_gfp" {
		t.Fatal(c, f)
	}
	if c, _, _ := do(h, "PUT", "/inter-vcp/reservation/x", `"WHAT"`); c != 400 {
		t.Fatal(c)
	}
	f.err = errors.New("No reservation was found for requestId x.")
	if c, b, _ := do(h, "PUT", "/inter-vcp/reservation/x", `"LOCKED"`); c != 500 || !strings.Contains(b, "No reservation") {
		t.Fatal(c, b)
	}
	if c, _, _ := do(h, "DELETE", "/inter-vcp/reservation/x", ``); c != 405 {
		t.Fatal(c)
	}
}

func TestActuator(t *testing.T) {
	up := setup(&fake{}, func(context.Context) error { return nil })
	if c, b, _ := do(up, "GET", "/actuator/health", ""); c != 200 || b != `{"status":"UP"}` {
		t.Fatal(c, b)
	}
	down := setup(&fake{}, func(context.Context) error { return errors.New("db") })
	if c, b, _ := do(down, "GET", "/actuator/health", ""); c != 503 || b != `{"status":"DOWN"}` {
		t.Fatal(c, b)
	}
	if c, b, _ := do(up, "GET", "/actuator/info", ""); c != 200 || !strings.Contains(b, `"apiVersion":"0.3.0"`) {
		t.Fatal(c, b)
	}
	if c, b, _ := do(up, "GET", "/actuator/prometheus", ""); c != 200 || !strings.Contains(b, "go_goroutines") {
		t.Fatal(c)
	}
}
