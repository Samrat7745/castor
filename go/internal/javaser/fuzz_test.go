package javaser

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"unicode/utf8"
)

func seedFixtures(f *testing.F) {
	files, _ := filepath.Glob("testdata/*.bin")
	for _, p := range files {
		if b, err := os.ReadFile(p); err == nil {
			f.Add(b)
		}
	}
	f.Add([]byte{})
	f.Add([]byte{0xac, 0xed, 0x00, 0x05})
	f.Add([]byte{0xac, 0xed, 0x00, 0x05, 0x73, 0x72, 0xff, 0xff})
}

// FuzzDecode: no Decode* function panics on arbitrary bytes.
func FuzzDecode(f *testing.F) {
	seedFixtures(f)
	f.Add(EncodeLong(-1))
	f.Add(EncodeReservation(ReservationData{ID: "r", Type: "BIT_GFP", Status: "LOCKED", Elements: []ReservationElementData{{id1, 3, 4}}}))
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = DecodeTupleChunk(b)
		_, _ = DecodeUploadResponse(b)
		_, _ = DecodeReservation(b)
		_, _ = DecodeLong(b)
	})
}

// FuzzRoundTrip: whatever the encoders produce, the decoders return unchanged.
func FuzzRoundTrip(f *testing.F) {
	f.Add(int64(0), int64(0), "id", "BIT_GFP", []byte("abc"), int64(7), int64(9), true)
	f.Add(int64(-1), int64(1<<62), "", "MULTIPLICATION_TRIPLE_GF2N", []byte{}, int64(-5), int64(0), false)
	f.Add(int64(5), int64(6), "ünï☃code", "X", []byte{0, 0xff}, int64(1), int64(2), true)
	f.Fuzz(func(t *testing.T, msb, lsb int64, s, typ string, data []byte, a, b int64, ok bool) {
		if len(s) > 1000 || len(typ) > 1000 || !utf8.ValidString(s) || !utf8.ValidString(typ) {
			t.Skip()
		}
		id := FormatUUID(msb, lsb)
		tc, err := DecodeTupleChunk(EncodeTupleChunk(id, typ, data))
		if err != nil || tc.ChunkID != id || tc.Type != typ || !bytes.Equal(tc.Tuples, data) {
			t.Fatalf("tuple chunk: %+v %v", tc, err)
		}
		var msg *string
		if ok {
			msg = &s
		}
		ur, err := DecodeUploadResponse(EncodeUploadResponse(id, msg, ok))
		if err != nil || ur.ChunkID != id || ur.Success != ok || (msg == nil) != (ur.ErrorMsg == nil) || (msg != nil && *ur.ErrorMsg != s) {
			t.Fatalf("upload response: %+v %v", ur, err)
		}
		rd := ReservationData{ID: s, Type: typ, Status: "LOCKED", Elements: []ReservationElementData{{id, a, b}, {id1, b, a}}}
		got, err := DecodeReservation(EncodeReservation(rd))
		if err != nil || got.ID != rd.ID || got.Type != typ || got.Status != "LOCKED" || len(got.Elements) != 2 || got.Elements[0] != rd.Elements[0] || got.Elements[1] != rd.Elements[1] {
			t.Fatalf("reservation: %+v %v", got, err)
		}
		if n, err := DecodeLong(EncodeLong(a)); err != nil || n != a {
			t.Fatalf("long: %d %v", n, err)
		}
		if m, l, ok := ParseUUID(id); !ok || m != msb || l != lsb {
			t.Fatalf("uuid %s: %d %d %v", id, m, l, ok)
		}
	})
}
