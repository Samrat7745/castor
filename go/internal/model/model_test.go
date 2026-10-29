package model

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

const td = "../javaser/testdata/"

func TestTupleListGolden(t *testing.T) {
	for _, c := range []struct{ typ, name string }{
		{"MULTIPLICATION_TRIPLE_GFP", "tuplelist_mt_gfp"}, {"BIT_GF2N", "tuplelist_bit_gf2n"},
	} {
		raw, _ := os.ReadFile(td + c.name + ".raw")
		want, _ := os.ReadFile(td + c.name + ".json")
		tt, _ := ParseTupleType(c.typ)
		var b bytes.Buffer
		if err := WriteTupleListJSON(&b, tt, raw); err != nil {
			t.Fatal(err)
		}
		if b.String() != string(want) {
			t.Fatalf("%s mismatch:\n%s\n%s", c.name, b.String(), want)
		}
	}
}

func TestReservationJSON(t *testing.T) {
	want, _ := os.ReadFile(td + "reservation.json")
	var r Reservation
	if err := json.Unmarshal(want, &r); err != nil {
		t.Fatal(err)
	}
	got, _ := json.Marshal(r)
	if string(got) != string(want) {
		t.Fatalf("%s\n%s", got, want)
	}
	for _, bad := range []string{`{}`, `{"reservationId":"","tupleType":"BIT_GFP","status":"LOCKED","reservations":[{"tupleChunkId":"123e4567-e89b-12d3-a456-426614174000","reservedTuples":1,"startIndex":0}]}`,
		`{"reservationId":"x","tupleType":"BIT_GFP","status":"LOCKED","reservations":[]}`,
		`{"reservationId":"x","tupleType":"BIT_GFP","status":"LOCKED","reservations":[{"tupleChunkId":"123e4567-e89b-12d3-a456-426614174000","reservedTuples":0,"startIndex":0}]}`} {
		if json.Unmarshal([]byte(bad), &Reservation{}) == nil {
			t.Errorf("expected failure for %s", bad)
		}
	}
}

func TestSizes(t *testing.T) {
	m, _ := ParseTupleType("MULTIPLICATION_TRIPLE_GF2N")
	if m.TupleSize() != 48 || m.String() != "multiplicationtriple_gf2n" {
		t.Fatal("sizes")
	}
	if _, err := ParseTupleType("nope"); err == nil {
		t.Fatal("expected error")
	}
}

func TestTelemetryJSON(t *testing.T) {
	want, _ := os.ReadFile(td + "telemetry.json")
	var m []TupleMetric
	for _, tt := range TupleTypes {
		m = append(m, TupleMetric{1000, 3, tt.Name()})
	}
	got, _ := json.Marshal(TelemetryData{m, 60000})
	if string(got) != string(want) {
		t.Fatal(string(got))
	}
}
