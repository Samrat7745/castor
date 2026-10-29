package pgstore

import (
	"context"
	"os"
	"testing"
)

func open(t *testing.T) (*Store, context.Context) {
	dsn := os.Getenv("CASTOR_TEST_PG")
	if dsn == "" {
		t.Skip("CASTOR_TEST_PG not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if _, err := s.Pool.Exec(ctx, "DELETE FROM tuple_chunk_fragment"); err != nil {
		t.Fatal(err)
	}
	return s, ctx
}

const c1 = "123e4567-e89b-12d3-a456-426614174000"

func TestKeepConflictActivateAvailable(t *testing.T) {
	s, ctx := open(t)
	mk := func(a, b int64) *Fragment { f, _ := NewFragment(c1, "BIT_GFP", a, b, "LOCKED", nil); return f }
	if err := KeepFragments(ctx, s.Pool, []*Fragment{mk(0, 1000), mk(1000, 2500)}); err != nil {
		t.Fatal(err)
	}
	if err := KeepFragments(ctx, s.Pool, []*Fragment{mk(2499, 3000)}); err != ErrConflict {
		t.Fatalf("want conflict, got %v", err)
	}
	if err := KeepFragments(ctx, s.Pool, []*Fragment{mk(2500, 3000)}); err != nil {
		t.Fatal(err)
	}
	if n := AvailableTuples(ctx, s.Pool, "BIT_GFP"); n != 0 {
		t.Fatalf("locked fragments must not be available, got %d", n)
	}
	n, err := UnlockChunk(ctx, s.Pool, c1)
	if err != nil || n != 3 {
		t.Fatal(n, err)
	}
	if n := AvailableTuples(ctx, s.Pool, "BIT_GFP"); n != 3000 {
		t.Fatal(n)
	}
	if n := AvailableTuples(ctx, s.Pool, "BIT_GF2N"); n != 0 {
		t.Fatal(n)
	}
	if n, _ := UnlockChunk(ctx, s.Pool, "00000000-0000-0000-0000-000000000001"); n != 0 {
		t.Fatal("expected 0 rows")
	}
}

func TestSplitAndFind(t *testing.T) {
	s, ctx := open(t)
	f, _ := NewFragment(c1, "BIT_GFP", 0, 1000, "UNLOCKED", nil)
	if err := Insert(ctx, s.Pool, f); err != nil || f.ID == 0 {
		t.Fatal(err)
	}
	err := s.InTx(ctx, func(q Querier) error {
		fr, err := FindAvailableByType(ctx, q, "BIT_GFP")
		if err != nil || fr == nil {
			t.Fatal("find", err)
		}
		head, err := SplitAt(ctx, q, fr, 400)
		if err != nil || head.Start != 0 || head.End != 400 {
			t.Fatal("splitAt", head, err)
		}
		rid := "r1"
		head.ReservationID = &rid
		return Update(ctx, q, head)
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := AvailableTuples(ctx, s.Pool, "BIT_GFP"); n != 600 {
		t.Fatal(n)
	}
	fr, _ := FindAvailableForChunkAt(ctx, s.Pool, c1, 450)
	if fr == nil || fr.Start != 400 || fr.End != 1000 {
		t.Fatalf("%+v", fr)
	}
	tail, err := SplitBefore(ctx, s.Pool, fr, 500)
	if err != nil || tail.Start != 500 || tail.End != 1000 {
		t.Fatal(tail, err)
	}
	same, _ := SplitBefore(ctx, s.Pool, tail, 500)
	if same != tail {
		t.Fatal("splitBefore at start must be no-op")
	}
	if ex, _ := ChunkReferenced(ctx, s.Pool, c1); !ex {
		t.Fatal("referenced")
	}
	_ = DeleteByReservation(ctx, s.Pool, "r1")
	if fr, _ := FindAvailableForChunkAt(ctx, s.Pool, c1, 10); fr != nil {
		t.Fatal("deleted fragment must be gone")
	}
	if _, err := NewFragment(c1, "BIT_GFP", -1, 2, "LOCKED", nil); err == nil || err.Error() != "Illegal start index (-1). (must be >= 0)." {
		t.Fatal(err)
	}
}

func TestSchemaMatchesHibernate(t *testing.T) {
	s, ctx := open(t)
	rows, err := s.Pool.Query(ctx, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='tuple_chunk_fragment' ORDER BY column_name")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for rows.Next() {
		var a, b string
		_ = rows.Scan(&a, &b)
		got[a] = b
	}
	want := map[string]string{"fragment_id": "bigint", "activation_status": "character varying", "end_index": "bigint", "reservation_id": "character varying", "start_index": "bigint", "tuple_chunk_id": "uuid", "tuple_type": "character varying"}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: %s != %s", k, got[k], v)
		}
	}
}
