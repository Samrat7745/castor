package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/carbynestack/castor-go/internal/model"
	"github.com/carbynestack/castor-go/internal/service"
)

// The remaining branches of the REST layer: routing errors for every route, malformed and huge
// request bodies, missing credentials, panicking backends, dependency failures and metrics.

type route struct{ method, path string }

var routes = []route{
	{"GET", "/intra-vcp/tuples"},
	{"PUT", "/intra-vcp/tuple-chunks/activate/" + uid},
	{"GET", "/intra-vcp/telemetry"},
	{"POST", "/inter-vcp/reservation"},
	{"PUT", "/inter-vcp/reservation/" + uid + "_bit_gfp"},
	{"GET", "/actuator/health"},
	{"GET", "/actuator/info"},
	{"GET", "/actuator/prometheus"},
}

func TestMethodNotAllowedOnEveryRoute(t *testing.T) {
	h := setup(&fake{}, func(context.Context) error { return nil })
	for _, r := range routes {
		for _, m := range []string{"GET", "POST", "PUT", "DELETE", "PATCH"} {
			if m == r.method {
				continue
			}
			code, body, hd := do(h, m, r.path, "")
			if code != 405 || body != "" || !strings.Contains(hd.Get("Allow"), r.method) {
				t.Errorf("%s %s: %d %q Allow=%q", m, r.path, code, body, hd.Get("Allow"))
			}
		}
	}
}

func TestUnknownPathsAre404WithSpringBody(t *testing.T) {
	h := setup(&fake{})
	for _, p := range []string{"/", "/intra-vcp", "/intra-vcp/", "/intra-vcp/tuples/", "/intra-vcp/tuple-chunks/activate/", "/intra-vcp/tuple-chunks/activate/a/b",
		"/inter-vcp/reservation/", "/inter-vcp/reservation/x/y", "/actuator", "/actuator/env", "/INTRA-VCP/tuples", "/intra-vcp/ws", "/%zz", "/a%2fb"} {
		for _, m := range []string{"GET", "POST", "PUT", "DELETE"} {
			r := httptest.NewRequest(m, "/", nil)
			r.URL.Path, r.URL.RawPath = p, ""
			if p == "/%zz" || p == "/a%2fb" {
				r.URL.Path = p
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 404 && w.Code != 405 {
				t.Errorf("%s %s: %d", m, p, w.Code)
			}
			if w.Code == 404 && (w.Header().Get("Content-Type") != "application/json" || !strings.Contains(w.Body.String(), `"error":"Not Found"`)) {
				t.Errorf("%s %s: body %q", m, p, w.Body.String())
			}
		}
	}
}

func TestHeadAndOptions(t *testing.T) {
	h := setup(&fake{data: []byte{}}, func(context.Context) error { return nil })
	if c, _, _ := do(h, "HEAD", "/actuator/health", ""); c != 200 {
		t.Fatal(c)
	}
	for _, r := range routes {
		if c, _, _ := do(h, "OPTIONS", r.path, ""); c != 405 && c != 200 && c != 204 {
			t.Errorf("OPTIONS %s: %d", r.path, c)
		}
	}
}

func TestUnsupportedMediaTypesAreRejectedWith415(t *testing.T) {
	f := &fake{}
	h := setup(f)
	good := `{"reservationId":"r","tupleType":"BIT_GFP","status":"LOCKED","reservations":[{"tupleChunkId":"` + uid + `","reservedTuples":1,"startIndex":0}]}`
	for _, ct := range []string{"", "text/plain", "application/xml", "text/json", "application/x-www-form-urlencoded", "multipart/form-data", "garbage", "application/", ";", "application/json;;;"} {
		for _, rq := range []struct{ method, path, body string }{
			{"POST", "/inter-vcp/reservation", good}, {"PUT", "/inter-vcp/reservation/x", `"LOCKED"`}} {
			r := httptest.NewRequest(rq.method, rq.path, strings.NewReader(rq.body))
			if ct != "" {
				r.Header.Set("Content-Type", ct)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 415 {
				t.Errorf("%s %q: %d", rq.method, ct, w.Code)
			}
		}
	}
	if f.applied != nil || f.id != "" {
		t.Fatal("backend called for an unsupported media type")
	}
	// accepted variants
	for _, ct := range []string{"application/json", "application/json;charset=UTF-8", "APPLICATION/JSON", "application/vnd.api+json", "application/problem+json; charset=utf-8"} {
		r := httptest.NewRequest("POST", "/inter-vcp/reservation", strings.NewReader(good))
		r.Header.Set("Content-Type", ct)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 201 {
			t.Errorf("%q: %d", ct, w.Code)
		}
	}
}

// The service does not negotiate: an Accept header it cannot honour is ignored (it always answers JSON).
func TestAcceptHeaderIsIgnored(t *testing.T) {
	h := setup(&fake{data: []byte{}})
	for _, accept := range []string{"application/xml", "text/html", "*/*", "application/json", ""} {
		r := httptest.NewRequest("GET", "/intra-vcp/telemetry", nil)
		if accept != "" {
			r.Header.Set("Accept", accept)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 || w.Header().Get("Content-Type") != "application/json" {
			t.Errorf("Accept %q: %d %s", accept, w.Code, w.Header().Get("Content-Type"))
		}
	}
}

func TestMalformedJSONBodies(t *testing.T) {
	f := &fake{}
	h := setup(f)
	for _, body := range []string{``, ` `, `{`, `{"reservationId":`, `[]`, `null`, `1`, `"str"`, `true`, `{"reservationId":1}`, `{"reservationId":"r","tupleType":"BIT_GFP","status":"LOCKED","reservations":"x"}`,
		`{"reservationId":"r","tupleType":"BIT_GFP","status":"LOCKED","reservations":[1]}`, `{"reservationId":"r","tupleType":"BIT_GFP","status":"LOCKED","reservations":[]}`,
		"\x00\x01\x02", `{"reservationId":"r"` + strings.Repeat(`,"x":{`, 5000)} {
		r := httptest.NewRequest("POST", "/inter-vcp/reservation", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Errorf("reservation %q: %d", body, w.Code)
		}
	}
	if f.applied != nil {
		t.Fatal("backend saw a malformed reservation")
	}
	for _, body := range []string{``, `UNLOCKED`, `{}`, `[]`, `null`, `1`, `"unlocked"`, `"LOCKED`, `"UNLOCKED "`, `""`, `true`} {
		r := httptest.NewRequest("PUT", "/inter-vcp/reservation/x", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Errorf("status %q: %d", body, w.Code)
		}
	}
	if f.id != "" {
		t.Fatal("backend saw a malformed status")
	}
}

func TestHugeRequests(t *testing.T) {
	f := &fake{data: []byte{}}
	srv := httptest.NewServer(setup(f))
	defer srv.Close()
	post := func(path, method, body string) int {
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	// 6 MB of reservation elements
	el := `{"tupleChunkId":"` + uid + `","reservedTuples":1,"startIndex":0},`
	huge := `{"reservationId":"r","tupleType":"BIT_GFP","status":"LOCKED","reservations":[` + strings.Repeat(el, 100000) + `{"tupleChunkId":"` + uid + `","reservedTuples":1,"startIndex":0}]}`
	if c := post("/inter-vcp/reservation", "POST", huge); c != 201 {
		t.Fatalf("large valid reservation: %d", c)
	}
	if len(f.applied.Reservations) != 100001 {
		t.Fatal(len(f.applied.Reservations))
	}
	// a 20 MB string as status, as reservation id and an endless number
	if c := post("/inter-vcp/reservation/x", "PUT", `"`+strings.Repeat("A", 20<<20)+`"`); c != 400 {
		t.Fatalf("huge status: %d", c)
	}
	if c := post("/inter-vcp/reservation", "POST", `{"reservationId":"`+strings.Repeat("R", 20<<20)+`","tupleType":"BIT_GFP","status":"LOCKED","reservations":[]}`); c != 400 {
		t.Fatalf("huge id: %d", c)
	}
	if c := post("/inter-vcp/reservation", "POST", `{"reservationId":"r","reservedTuples":`+strings.Repeat("9", 5<<20)+`}`); c != 400 {
		t.Fatalf("huge number: %d", c)
	}
	// very long query strings and path segments
	for _, p := range []string{"/intra-vcp/tuples?tupletype=BIT_GFP&count=1&reservationId=" + strings.Repeat("a", 6000),
		"/intra-vcp/tuple-chunks/activate/" + strings.Repeat("a", 6000), "/intra-vcp/telemetry?interval=" + strings.Repeat("9", 6000)} {
		method := "GET"
		if strings.Contains(p, "activate") {
			method = "PUT"
		}
		if c := post(p, method, ""); c != 400 {
			t.Errorf("%.60s...: %d", p, c)
		}
	}
	if c := post("/intra-vcp/telemetry?x="+strings.Repeat("a", 2<<20), "GET", ""); c != 431 {
		t.Errorf("2 MB query: %d (the server's header limit answers 431)", c)
	}
}

// No endpoint looks at Authorization (authentication is the gateway's job).
func TestAuthorizationIsNotRequired(t *testing.T) {
	h := setup(&fake{data: []byte{}}, func(context.Context) error { return nil })
	for _, auth := range []string{"", "Bearer", "Bearer ", "Bearer garbage", "Basic Zm9vOmJhcg==", "\tweird"} {
		for _, rt := range []struct{ method, url string }{
			{"GET", "/intra-vcp/tuples?tupletype=BIT_GFP&count=1&reservationId=" + uid}, {"GET", "/intra-vcp/telemetry"},
			{"PUT", "/intra-vcp/tuple-chunks/activate/" + uid}, {"GET", "/actuator/health"}} {
			r := httptest.NewRequest(rt.method, rt.url, nil)
			if auth != "" {
				r.Header.Set("Authorization", auth)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 200 {
				t.Errorf("%s %q: %d", rt.url, auth, w.Code)
			}
		}
	}
}

type panicking struct{ fake }

func (p *panicking) Telemetry(context.Context, time.Duration) (*model.TelemetryData, error) {
	panic("backend exploded")
}
func (p *panicking) ActivateChunk(context.Context, string) error { panic(errors.New("nil pointer")) }

func TestPanickingBackendAnswers500AndTheServerSurvives(t *testing.T) {
	var checks atomic.Int32
	srv := httptest.NewServer(New(&panicking{}, []HealthCheck{func(context.Context) error { checks.Add(1); return nil }}, Info{}, nil, nil))
	defer srv.Close()
	for i := 0; i < 3; i++ {
		for _, rq := range []struct{ method, path string }{{"GET", "/intra-vcp/telemetry"}, {"PUT", "/intra-vcp/tuple-chunks/activate/" + uid}} {
			req, _ := http.NewRequest(rq.method, srv.URL+rq.path, nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("%s: connection dropped instead of a 500 answer: %v", rq.path, err)
			}
			resp.Body.Close()
			if resp.StatusCode != 500 {
				t.Fatalf("%s: %d", rq.path, resp.StatusCode)
			}
		}
	}
	resp, err := http.Get(srv.URL + "/actuator/health")
	if err != nil || resp.StatusCode != 200 || checks.Load() != 1 {
		t.Fatalf("server unhealthy after panics: %v", err)
	}
	resp.Body.Close()
	// the panics are visible in the metrics
	resp, _ = http.Get(srv.URL + "/actuator/prometheus")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), `http_server_requests_seconds_count{method="GET",status="500"} 3`) {
		t.Fatalf("metrics miss the failed requests:\n%s", grep(string(b), "http_server"))
	}
}

func grep(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

func TestHealthWithEachDependencyDown(t *testing.T) {
	ok := func(context.Context) error { return nil }
	bad := func(context.Context) error { return errors.New("down") }
	for _, tc := range []struct {
		name   string
		checks []HealthCheck
		want   int
	}{
		{"none", nil, 200}, {"all up", []HealthCheck{ok, ok}, 200},
		{"first down", []HealthCheck{bad, ok}, 503}, {"second down", []HealthCheck{ok, bad}, 503}, {"both down", []HealthCheck{bad, bad}, 503},
	} {
		c, b, hd := do(setup(&fake{}, tc.checks...), "GET", "/actuator/health", "")
		wantBody := `{"status":"UP"}`
		if tc.want == 503 {
			wantBody = `{"status":"DOWN"}`
		}
		if c != tc.want || b != wantBody || hd.Get("Content-Type") != "application/vnd.spring-boot.actuator.v3+json" {
			t.Errorf("%s: %d %q %v", tc.name, c, b, hd)
		}
	}
	// a check that hangs is cut off after three seconds and reported as DOWN; later checks are not run
	var later atomic.Int32
	hang := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	start := time.Now()
	c, b, _ := do(setup(&fake{}, hang, func(context.Context) error { later.Add(1); return nil }), "GET", "/actuator/health", "")
	if c != 503 || b != `{"status":"DOWN"}` || time.Since(start) < 2900*time.Millisecond || time.Since(start) > 6*time.Second || later.Load() != 0 {
		t.Fatalf("hanging check: %d %q after %s", c, b, time.Since(start))
	}
	// a cancelled request context counts as down as well
	r := httptest.NewRequest("GET", "/actuator/health", nil)
	ctx, cancel := context.WithCancel(r.Context())
	cancel()
	w := httptest.NewRecorder()
	setup(&fake{}, func(ctx context.Context) error { return ctx.Err() }).ServeHTTP(w, r.WithContext(ctx))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
}

func TestPrometheusOutput(t *testing.T) {
	h := setup(&fake{data: []byte{}})
	do(h, "GET", "/intra-vcp/telemetry", "")
	do(h, "GET", "/intra-vcp/telemetry", "")
	do(h, "GET", "/nope", "")
	do(h, "DELETE", "/intra-vcp/telemetry", "")
	do(h, "GET", "/intra-vcp/tuples", "")
	c, b, hd := do(h, "GET", "/actuator/prometheus", "")
	if c != 200 || hd.Get("Content-Type") != "text/plain;version=0.0.4;charset=utf-8" {
		t.Fatal(c, hd)
	}
	for _, want := range []string{
		"# TYPE http_server_requests_seconds histogram",
		`http_server_requests_seconds_count{method="GET",status="200"} 2`,
		`http_server_requests_seconds_count{method="GET",status="404"} 1`,
		`http_server_requests_seconds_count{method="DELETE",status="405"} 1`,
		`http_server_requests_seconds_count{method="GET",status="400"} 1`,
		"go_goroutines", "process_open_fds",
	} {
		if !strings.Contains(b, want) {
			t.Errorf("missing %q in\n%s", want, grep(b, "http_server"))
		}
	}
	// scraping does not count itself before it has been answered, and a second scrape sees the first
	_, b2, _ := do(h, "GET", "/actuator/prometheus", "")
	if !strings.Contains(b2, `http_server_requests_seconds_count{method="GET",status="200"} 3`) {
		t.Errorf("second scrape:\n%s", grep(b2, "http_server"))
	}
}

func TestActuatorInfoBody(t *testing.T) {
	h := New(&fake{}, nil, Info{Name: "n", Description: "d", GroupID: "g", ArtifactID: "a", Version: "9.9"}, nil, nil)
	_, b, hd := do(h, "GET", "/actuator/info", "")
	if hd.Get("Content-Type") != "application/vnd.spring-boot.actuator.v3+json" {
		t.Fatal(hd)
	}
	for _, want := range []string{`"name":"n"`, `"description":"d"`, `"groupId":"g"`, `"artifactId":"a"`, `"version":"9.9"`, `"apiVersion":"9.9"`} {
		if !strings.Contains(b, want) {
			t.Errorf("%s missing in %s", want, b)
		}
	}
}

func TestErrorMappingToStatusAndBody(t *testing.T) {
	f := &fake{data: []byte{}}
	h := setup(f)
	for _, c := range []struct {
		err  error
		code int
		body string
	}{
		{&service.BadRequestError{Msg: "no \"good\"\n"}, 400, `"no \"good\"\n"`},
		{errors.New("plain"), 500, `"plain"`},
		{&service.Error{Msg: "wrapped", Cause: errors.New("cause")}, 500, `"wrapped"`},
		{errors.New("ünïcode <tag> & more"), 500, `"ünïcode <tag> & more"`},
		{errors.New(""), 500, `""`},
	} {
		f.err = c.err
		code, body, hd := do(h, "GET", "/intra-vcp/tuples?tupletype=BIT_GFP&count=1&reservationId="+uid, "")
		if code != c.code || body != c.body || hd.Get("Content-Type") != "text/plain;charset=UTF-8" {
			t.Errorf("%v: %d %q %v", c.err, code, body, hd)
		}
	}
	// tuple data the type cannot be cut into is a server error
	f.err, f.data = nil, make([]byte, 33)
	if c, _, _ := do(h, "GET", "/intra-vcp/tuples?tupletype=BIT_GFP&count=1&reservationId="+uid, ""); c != 500 {
		t.Fatal(c)
	}
}

func TestQueryParameterVariants(t *testing.T) {
	f := &fake{data: []byte{}}
	h := setup(f)
	base := "/intra-vcp/tuples?"
	for _, c := range []struct {
		q    string
		code int
	}{
		{"tupletype=BIT_GFP&count=1&reservationId=" + uid, 200},
		{"tupletype=BIT_GFP&count=1&reservationId=" + uid + "&extra=1", 200},
		{"count=1&reservationId=" + uid + "&tupletype=BIT_GFP", 200},
		{"tupletype=BIT_GFP&count=1&count=0&reservationId=" + uid, 200}, // first value wins
		{"tupletype=BIT_GFP&count=0&count=1&reservationId=" + uid, 400},
		{"tupletype=BIT_GFP&count=%2B1&reservationId=" + uid, 200},
		{"tupletype=BIT_GFP&count=01&reservationId=" + uid, 200},
		{"tupletype=BIT_GFP&count=&reservationId=" + uid, 400},
		{"tupletype=&count=1&reservationId=" + uid, 400},
		{"tupletype=BIT_GFP&count=1&reservationId=", 400},
		{"tupletype&count&reservationId", 400},
		{"tupletype=BIT_GFP&count=9223372036854775807&reservationId=" + uid, 200},
		{"tupletype=BIT_GFP&count=9223372036854775808&reservationId=" + uid, 400},
		{"tupletype=BIT_GFP&count=-9223372036854775808&reservationId=" + uid, 400},
		{"tupletype=BIT_GFP&count=1&reservationId=" + uid + "%20", 400},
		{"tupletype=BIT_GFP&count=1&reservationId=%00", 400},
		{"tupletype=BIT_GFP%00&count=1&reservationId=" + uid, 400},
		{"tupletype=BIT_GFP&count=1&reservationId=" + strings.ReplaceAll(uid, "-", ""), 400},
		{"tupletype=BIT_GFP&count=1&reservationId={" + uid + "}", 400},
		{"tupletype=BIT_GFP&count=1&reservationId=urn:uuid:" + uid, 400},
		{"tupletype=bit_gfp&count=1&reservationId=" + uid, 400},
		{"tupletype=%e2%82%ac&count=1&reservationId=" + uid, 400},
		{"tupletype=BIT_GFP;count=1&reservationId=" + uid, 400},
	} {
		if code, _, _ := do(h, "GET", base+c.q, ""); code != c.code {
			t.Errorf("%s: %d, want %d", c.q, code, c.code)
		}
	}
	for _, c := range []struct {
		q    string
		code int
	}{{"interval=1", 200}, {"interval=%2B5", 200}, {"interval=9223372036854775807", 200}, {"interval=9223372036854775808", 400}, {"interval=", 400},
		{"interval=0", 400}, {"interval=-1", 400}, {"interval=1.5", 400}, {"interval=0x10", 400}, {"interval=1&interval=x", 200}, {"unknown=1", 200}} {
		if code, _, _ := do(h, "GET", "/intra-vcp/telemetry?"+c.q, ""); code != c.code {
			t.Errorf("%s: %d, want %d", c.q, code, c.code)
		}
	}
	for _, id := range []string{"", "x", uid + "x", strings.ToUpper(uid), uid[:35], "%00", "123e4567-e89b-12d3-a456-42661417400%67"} {
		code, _, _ := do(h, "PUT", "/intra-vcp/tuple-chunks/activate/"+id, "")
		want := 400
		if id == strings.ToUpper(uid) {
			want = 200
		}
		if id == "" {
			want = 404
		}
		if code != want && !(id == "" && code == 405) {
			t.Errorf("activate %q: %d, want %d", id, code, want)
		}
	}
}
