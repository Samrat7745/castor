package httpapi

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func fuzzDo(h http.Handler, method, path, rawQuery, body string) int {
	r := httptest.NewRequest(method, "/", strings.NewReader(body))
	r.URL = &url.URL{Path: path, RawQuery: rawQuery}
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w.Code
}

// FuzzRequestParameters: arbitrary query / path parameters and bodies on every route answer
// with a 2xx or 4xx, never a 5xx (the backend itself is healthy), and never panic.
func FuzzRequestParameters(f *testing.F) {
	f.Add("BIT_GFP", "3", uid, "60", uid, "\"LOCKED\"")
	f.Add("", "", "", "", "", "")
	f.Add("BIT_GFP", "-1", strings.ToUpper(uid), "0", uid+"_bit_gfp", `{"x":1}`)
	f.Add("BIT_GFP", "9223372036854775808", "123e4567-e89b-12d3-a456-42661417400g", "x", "%zz", `"UNLOCKED"`)
	f.Add("MULTIPLICATION_TRIPLE_GF2N", "1", "00000000-0000-0000-0000-000000000000", "-5", "../x", `{"reservationId":"r","tupleType":"BIT_GFP","status":"LOCKED","reservations":[{"tupleChunkId":"`+uid+`","reservedTuples":1,"startIndex":0}]}`)
	f.Fuzz(func(t *testing.T, tt, count, resID, interval, pathID, body string) {
		h := setup(&fake{})
		q := url.Values{"tupletype": {tt}, "count": {count}, "reservationId": {resID}}
		check := func(what string, code int) {
			if code >= 500 {
				t.Fatalf("%s answered %d", what, code)
			}
		}
		check("tuples", fuzzDo(h, "GET", "/intra-vcp/tuples", q.Encode(), ""))
		check("telemetry", fuzzDo(h, "GET", "/intra-vcp/telemetry", url.Values{"interval": {interval}}.Encode(), ""))
		check("activate", fuzzDo(h, "PUT", "/intra-vcp/tuple-chunks/activate/"+pathID, "", ""))
		check("update", fuzzDo(h, "PUT", "/inter-vcp/reservation/"+pathID, "", body))
		check("reserve", fuzzDo(h, "POST", "/inter-vcp/reservation", "", body))
		// raw (unescaped) query strings must not break the handler either
		check("raw query", fuzzDo(h, "GET", "/intra-vcp/tuples", tt+"&"+count+"="+resID, ""))
	})
}
