package model

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// Ports of the castor-common ReservationElementTest and ReservationTest (JSON creator rules of the
// Java entities; the constructor-only null checks surface as missing properties on the wire).

const (
	chunk   = "80fbba1b-3da8-4b1e-8a2c-000cebd65229"
	resID   = "80fbba1b-3da8-4b1e-8a2c-cebd65229fad_MULTIPLICATION_TRIPLE_GFP"
	element = `{"tupleChunkId":"` + chunk + `","reservedTuples":1200,"startIndex":42}`
)

func reservationJSON(parts ...string) string { return "{" + strings.Join(parts, ",") + "}" }

func TestReservationElementJSON(t *testing.T) {
	wrap := func(el string) string {
		return reservationJSON(`"reservationId":"`+resID+`"`, `"tupleType":"MULTIPLICATION_TRIPLE_GFP"`, `"status":"LOCKED"`, `"reservations":[`+el+`]`)
	}
	for _, c := range []struct {
		name, el, wantErr string
	}{
		{"without tupleChunkId", `{"reservedTuples":1200,"startIndex":42}`, "Missing required creator property 'tupleChunkId'"},
		{"without reservedTuples", `{"tupleChunkId":"` + chunk + `","startIndex":42}`, "Missing required creator property 'reservedTuples'"},
		{"without startIndex", `{"tupleChunkId":"` + chunk + `","reservedTuples":1200}`, "Missing required creator property 'startIndex'"},
		{"null chunk id", `{"tupleChunkId":null,"reservedTuples":1200,"startIndex":42}`, "Missing required creator property 'tupleChunkId'"},
		{"zero tuples", `{"tupleChunkId":"` + chunk + `","reservedTuples":0,"startIndex":42}`, "At least 1 tuple share needs to be reserved."},
		{"negative start index", `{"tupleChunkId":"` + chunk + `","reservedTuples":1200,"startIndex":-1}`, "StartIndex must not be negative."},
		{"malformed uuid", `{"tupleChunkId":"80fbba1b-3da8-4b1e-8a2c-cebd65229","reservedTuples":1200,"startIndex":42}`, "invalid UUID"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := json.Unmarshal([]byte(wrap(c.el)), &Reservation{})
			if err == nil || !strings.HasPrefix(err.Error(), c.wantErr) {
				t.Fatalf("got %v, want prefix %q", err, c.wantErr)
			}
		})
	}
	var r Reservation
	if err := json.Unmarshal([]byte(wrap(element)), &r); err != nil {
		t.Fatal(err)
	}
	if e := r.Reservations[0]; e.TupleChunkID != chunk || e.ReservedTuples != 1200 || e.StartIndex != 42 {
		t.Fatalf("%+v", e)
	}
	// java.util.UUID#toString is lower case, so is the stored chunk id
	upper := strings.ReplaceAll(element, chunk, strings.ToUpper(chunk))
	if err := json.Unmarshal([]byte(wrap(upper)), &r); err != nil || r.Reservations[0].TupleChunkID != chunk {
		t.Fatal(err, r.Reservations)
	}
}

func TestReservationJSONRules(t *testing.T) {
	id, typ, st, els := `"reservationId":"`+resID+`"`, `"tupleType":"MULTIPLICATION_TRIPLE_GFP"`, `"status":"UNLOCKED"`, `"reservations":[`+element+`]`
	for _, c := range []struct {
		name, json, wantErr string
	}{
		{"empty id", reservationJSON(`"reservationId":""`, typ, st, els), "ReservationId must not be empty."},
		{"no reservation elements", reservationJSON(id, typ, st, `"reservations":[]`), "At least one ReservationElement required."},
		{"without reservationId", reservationJSON(typ, `"status":"LOCKED"`, `"reservations":[]`), "Missing required creator property 'reservationId'"},
		{"without reservations", reservationJSON(id, typ, `"status":"LOCKED"`), "Missing required creator property 'reservations'"},
		{"without tupleType", reservationJSON(id, `"status":"LOCKED"`, `"reservations":[]`), "Missing required creator property 'tupleType'"},
		{"without status", reservationJSON(id, typ, `"reservations":[]`), "Missing required creator property 'status'"},
		{"unknown tuple type", reservationJSON(id, `"tupleType":"nope"`, st, els), "No enum constant io.carbynestack.castor.common.entities.TupleType.nope"},
		{"unknown status", reservationJSON(id, typ, `"status":"OPEN"`, els), "invalid status"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := json.Unmarshal([]byte(c.json), &Reservation{})
			if err == nil || !strings.HasPrefix(err.Error(), c.wantErr) {
				t.Fatalf("got %v, want prefix %q", err, c.wantErr)
			}
		})
	}
	var r Reservation
	if err := json.Unmarshal([]byte(reservationJSON(id, typ, st, els)), &r); err != nil {
		t.Fatal(err)
	}
	if r.ReservationID != resID || r.TupleType != "MULTIPLICATION_TRIPLE_GFP" || r.Status != Unlocked || len(r.Reservations) != 1 || r.Total() != 1200 {
		t.Fatalf("%+v", r)
	}
	// the Reservation(id, type, elements) constructor defaults to LOCKED; the type string form is lower case
	if !strings.Contains(r.String(), "tupleType=multiplicationtriple_gfp") || !strings.HasSuffix(r.String(), "status=UNLOCKED)") {
		t.Fatal(r.String())
	}
}

// ArrayBackedTupleTest.givenStreamOfInvalidLength: a stream that ends inside a share is rejected.
func TestTupleDataOfInvalidLengthIsRejected(t *testing.T) {
	bit, _ := ParseTupleType("BIT_GFP")
	var b bytes.Buffer
	if err := WriteTupleListJSON(&b, bit, make([]byte, bit.TupleSize()-1)); err == nil {
		t.Fatal("expected error for data ending inside a share")
	}
	for _, tt := range TupleTypes {
		var b bytes.Buffer
		if err := WriteTupleListJSON(&b, tt, make([]byte, tt.TupleSize()*2+1)); err == nil {
			t.Fatalf("%s: expected error", tt.Name())
		}
	}
}

func TestTupleTypeProperties(t *testing.T) {
	// field element sizes and arities of the Java TupleType enum
	want := map[string][3]int{"BIT_GFP": {1, 16, 32}, "BIT_GF2N": {1, 8, 16}, "INPUT_MASK_GFP": {1, 16, 32}, "INVERSE_TUPLE_GF2N": {2, 8, 32},
		"SQUARE_TUPLE_GFP": {2, 16, 64}, "MULTIPLICATION_TRIPLE_GFP": {3, 16, 96}, "MULTIPLICATION_TRIPLE_GF2N": {3, 8, 48}}
	for n, w := range want {
		tt, err := ParseTupleType(n)
		if err != nil || tt.Arity != w[0] || tt.Field.ElementSize != w[1] || tt.TupleSize() != w[2] {
			t.Fatalf("%s: %+v %v", n, tt, err)
		}
	}
	if len(TupleTypes) != 10 {
		t.Fatal(len(TupleTypes))
	}
}
