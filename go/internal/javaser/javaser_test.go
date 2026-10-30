package javaser

import (
	"bytes"
	"os"
	"testing"

	"github.com/google/uuid"
)

func fx(t *testing.T, n string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + n)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

const id1 = "123e4567-e89b-12d3-a456-426614174000"

func eq(t *testing.T, name string, got, want []byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Fatalf("%s: byte mismatch\n got %x\nwant %x", name, got, want)
	}
}

func TestTupleChunk(t *testing.T) {
	for _, c := range []struct {
		f, typ string
		n      int
	}{
		{"tuplechunk_bit_gfp.bin", "BIT_GFP", 96}, {"tuplechunk_mt_gf2n.bin", "MULTIPLICATION_TRIPLE_GF2N", 96}, {"tuplechunk_empty.bin", "BIT_GFP", 0}} {
		raw := fx(t, c.f)
		tc, err := DecodeTupleChunk(raw)
		if err != nil {
			t.Fatal(c.f, err)
		}
		if tc.ChunkID != id1 || tc.Type != c.typ || len(tc.Tuples) != c.n {
			t.Fatalf("%s: %+v", c.f, tc)
		}
		eq(t, c.f, EncodeTupleChunk(tc.ChunkID, tc.Type, tc.Tuples), raw)
	}
}

func TestBitDataPreserved(t *testing.T) {
	tc, _ := DecodeTupleChunk(fx(t, "tuplechunk_bit_gfp.bin"))
	for i, b := range tc.Tuples {
		if b != byte(i) {
			t.Fatal("data")
		}
	}
}

func TestResponses(t *testing.T) {
	m1, m2 := "Failed writing tuple chunk to database.", "Unsupported payload type."
	eq(t, "success", EncodeUploadResponse(id1, nil, true), fx(t, "resp_success.bin"))
	eq(t, "failure_id", EncodeUploadResponse(id1, &m1, false), fx(t, "resp_failure_id.bin"))
	eq(t, "failure_null", EncodeUploadResponse("", &m2, false), fx(t, "resp_failure_null.bin"))
	r, err := DecodeUploadResponse(fx(t, "resp_failure_id.bin"))
	if err != nil || r.Success || r.ChunkID != id1 || *r.ErrorMsg != m1 {
		t.Fatal(r, err)
	}
}

func TestReservation(t *testing.T) {
	for _, f := range []string{"reservation_locked.bin", "reservation_unlocked.bin"} {
		raw := fx(t, f)
		r, err := DecodeReservation(raw)
		if err != nil {
			t.Fatal(f, err)
		}
		eq(t, f, EncodeReservation(*r), raw)
	}
	r, _ := DecodeReservation(fx(t, "reservation_locked.bin"))
	if r.Status != "LOCKED" || r.Type != "BIT_GFP" || len(r.Elements) != 2 || r.Elements[1].Start != 1000 || r.Elements[1].ChunkID != "00000000-0000-0000-0000-00000000ffff" {
		t.Fatalf("%+v", r)
	}
}

func TestLong(t *testing.T) {
	eq(t, "42", EncodeLong(42), fx(t, "long_42.bin"))
	eq(t, "big", EncodeLong(1<<40), fx(t, "long_big.bin"))
	v, err := DecodeLong(fx(t, "long_big.bin"))
	if err != nil || v != 1<<40 {
		t.Fatal(v, err)
	}
}

func TestRejects(t *testing.T) {
	if _, err := DecodeTupleChunk([]byte("garbage")); err == nil {
		t.Fatal("garbage")
	}
	if _, err := DecodeTupleChunk(fx(t, "resp_success.bin")); err == nil {
		t.Fatal("wrong class must fail (ClassCastException)")
	}
	raw := fx(t, "tuplechunk_bit_gfp.bin")
	for i := 0; i < len(raw); i += 7 {
		_, _ = DecodeTupleChunk(raw[:i]) // must not panic
	}
}

// Ports of UploadTupleChunkResponseTest: the serialization round trip of success and failure
// responses, and the rejection of a truncated payload.
func TestUploadResponseRoundTrip(t *testing.T) {
	id := uuid.NewString()
	r, err := DecodeUploadResponse(EncodeUploadResponse(id, nil, true))
	if err != nil || r.ChunkID != id || !r.Success || r.ErrorMsg != nil {
		t.Fatalf("success: %+v %v", r, err)
	}
	msg := "Expected message"
	r, err = DecodeUploadResponse(EncodeUploadResponse(id, &msg, false))
	if err != nil || r.ChunkID != id || r.Success || r.ErrorMsg == nil || *r.ErrorMsg != msg {
		t.Fatalf("failure: %+v %v", r, err)
	}
	// failure without chunk id (unsupported payload)
	r, err = DecodeUploadResponse(EncodeUploadResponse("", &msg, false))
	if err != nil || r.ChunkID != "" || r.Success {
		t.Fatalf("null id: %+v %v", r, err)
	}
}

func TestUploadResponseTruncatedPayloadIsRejected(t *testing.T) {
	payload := EncodeUploadResponse(uuid.NewString(), nil, true)
	if _, err := DecodeUploadResponse(payload[:len(payload)-1]); err == nil {
		t.Fatal("truncated payload must fail")
	}
	for i := 0; i < len(payload); i++ {
		if _, err := DecodeUploadResponse(payload[:i]); err == nil {
			t.Fatalf("prefix of %d bytes must fail", i)
		}
	}
	if _, err := DecodeUploadResponse([]byte{}); err == nil {
		t.Fatal("empty payload must fail")
	}
}
