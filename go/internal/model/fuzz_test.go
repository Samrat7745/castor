package model

import (
	"encoding/json"
	"testing"
)

// FuzzReservationJSON: decoding never panics; an accepted reservation satisfies the Java rules and
// re-encodes to something that decodes to the same value.
func FuzzReservationJSON(f *testing.F) {
	f.Add(`{"reservationId":"r","tupleType":"BIT_GFP","status":"LOCKED","reservations":[{"tupleChunkId":"123e4567-e89b-12d3-a456-426614174000","reservedTuples":3,"startIndex":0}]}`)
	f.Add(`{}`)
	f.Add(`null`)
	f.Add(`{"reservationId":"","tupleType":"BIT_GFP","status":"X","reservations":[]}`)
	f.Add(`{"reservationId":"r","tupleType":"BIT_GFP","status":"LOCKED","reservations":[{"tupleChunkId":"nope","reservedTuples":-1,"startIndex":-1}]}`)
	f.Add(`[1,2`)
	f.Fuzz(func(t *testing.T, in string) {
		var r Reservation
		if err := json.Unmarshal([]byte(in), &r); err != nil {
			return
		}
		if r.ReservationID == "" || len(r.Reservations) == 0 {
			t.Fatalf("accepted invalid reservation %+v", r)
		}
		for _, e := range r.Reservations {
			if e.ReservedTuples < 1 || e.StartIndex < 0 || !IsUUID(e.TupleChunkID) {
				t.Fatalf("accepted invalid element %+v", e)
			}
		}
		_ = r.String()
		_ = r.Total()
		b, err := json.Marshal(&r)
		if err != nil {
			t.Fatal(err)
		}
		var r2 Reservation
		if err := json.Unmarshal(b, &r2); err != nil || r2.ReservationID != r.ReservationID || len(r2.Reservations) != len(r.Reservations) {
			t.Fatalf("re-decode: %v %+v", err, r2)
		}
	})
}

// FuzzParseTupleType: only the ten enum names parse; everything else is an error, never a panic.
func FuzzParseTupleType(f *testing.F) {
	for _, tt := range TupleTypes {
		f.Add(tt.Name())
		f.Add(tt.String())
	}
	f.Add("")
	f.Add("bit_gfp\x00")
	f.Fuzz(func(t *testing.T, s string) {
		tt, err := ParseTupleType(s)
		if err != nil {
			if tt.Valid() {
				t.Fatal("valid type with error")
			}
			return
		}
		if tt.Name() != s || !tt.Valid() || tt.TupleSize() <= 0 {
			t.Fatalf("%q -> %+v", s, tt)
		}
	})
}

// FuzzIsUUID: the UUID check never panics and accepts only 36 character strings.
func FuzzIsUUID(f *testing.F) {
	f.Add("123e4567-e89b-12d3-a456-426614174000")
	f.Add("")
	f.Add("123E4567-E89B-12D3-A456-426614174000")
	f.Add("123e4567e89b12d3a456426614174000")
	f.Fuzz(func(t *testing.T, s string) {
		if IsUUID(s) && len(s) != 36 {
			t.Fatalf("%q accepted", s)
		}
	})
}
