package pgstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
)

// Ports of the castor-service TupleChunkFragmentEntityTest, TupleChunkFragmentStorageServiceTest
// (Mockito, here against the real database) and TupleChunkFragmentStorageServiceIT, plus the
// observable behaviour of DedicatedTransactionService (Store.InTx).
//
// Not applicable: the null checks of TupleChunkFragmentEntity.of (tupleChunkId / tupleType are
// never null in Go, the chunk id is a validated string) and the Spring/Mockito wiring of the
// repository (same SQL, exercised directly).

const (
	chunkX = "3fd7eaf7-cda3-4384-8d86-2c43450cbe63"
	chunkY = "80fbba1b-3da8-4b1e-8a2c-cebd65229fad"
	chunkZ = "3dc08ff2-5eed-49a9-979e-3a3ac0e4a2cf"
	mt     = "MULTIPLICATION_TRIPLE_GFP"
	im     = "INPUT_MASK_GFP"
)

func frag(chunk, typ string, start, end int64, status string, res *string) *Fragment {
	f, err := NewFragment(chunk, typ, start, end, status, res)
	if err != nil {
		panic(err)
	}
	return f
}

func str(s string) *string { return &s }

// save inserts fragments in order (the repository's save) and returns them with their ids.
func save(t *testing.T, s *Store, fs ...*Fragment) []*Fragment {
	t.Helper()
	for _, f := range fs {
		if err := Insert(context.Background(), s.Pool, f); err != nil {
			t.Fatal(err)
		}
	}
	return fs
}

// all returns the stored fragments ordered by id (CrudRepository.findAll).
func all(t *testing.T, s *Store) []Fragment {
	t.Helper()
	rows, err := s.Pool.Query(context.Background(), "SELECT "+cols+" FROM tuple_chunk_fragment ORDER BY fragment_id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []Fragment
	for rows.Next() {
		var f Fragment
		if err := rows.Scan(&f.ID, &f.ChunkID, &f.TupleType, &f.Start, &f.End, &f.Status, &f.ReservationID); err != nil {
			t.Fatal(err)
		}
		out = append(out, f)
	}
	return out
}

// same compares all fields; the id only when the expectation carries one (b.ID != 0).
func same(a, b Fragment) bool {
	return (b.ID == 0 || a.ID == b.ID) && a.ChunkID == b.ChunkID && a.TupleType == b.TupleType && a.Start == b.Start && a.End == b.End && a.Status == b.Status &&
		(a.ReservationID == nil) == (b.ReservationID == nil) && (a.ReservationID == nil || *a.ReservationID == *b.ReservationID)
}

func expect(t *testing.T, got []Fragment, want ...*Fragment) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("want %d fragments, got %d: %+v", len(want), len(got), got)
	}
	for i := range want {
		if !same(got[i], *want[i]) {
			t.Fatalf("fragment %d: got %+v want %+v", i, got[i], *want[i])
		}
	}
}

func TestNewFragmentValidation(t *testing.T) {
	// givenStartIndexIsNegative / givenInvalidLength / setEndIndex(-1)
	for _, c := range []struct {
		start, end int64
		want       string
	}{
		{-2, 42, "Illegal start index (-2). (must be >= 0)."},
		{0, -1, "Illegal last index (-1)."},
		{10, 9, "Illegal last index (9)."},
	} {
		if f, err := NewFragment(chunkX, mt, c.start, c.end, "LOCKED", nil); err == nil || err.Error() != c.want || f != nil {
			t.Errorf("(%d,%d): %v", c.start, c.end, err)
		}
	}
	// givenValidConfiguration_whenCreateWithFactoryMethod_thenReturnExpectedFragment
	f, err := NewFragment(chunkX, mt, 0, 42, "UNLOCKED", str("123_multiplicationGfP"))
	if err != nil || f.ChunkID != chunkX || f.TupleType != mt || f.Start != 0 || f.End != 42 || f.Status != "UNLOCKED" || *f.ReservationID != "123_multiplicationGfP" {
		t.Fatalf("%+v %v", f, err)
	}
	if _, err := NewFragment(chunkX, mt, 5, 5, "LOCKED", nil); err != nil {
		t.Fatal("an empty range is allowed:", err)
	}
}

func TestSplitAtRejectsIndexBeforeStart(t *testing.T) {
	s, ctx := open(t)
	f := save(t, s, frag(chunkX, mt, 5, 10, "UNLOCKED", nil))[0]
	err := s.InTx(ctx, func(q Querier) error { _, err := SplitAt(ctx, q, f, 3); return err })
	if err == nil || err.Error() != "Illegal last index (3)." {
		t.Fatal(err)
	}
	// the failed split ran in a transaction: nothing was left behind
	expect(t, all(t, s), frag(chunkX, mt, 5, 10, "UNLOCKED", nil))
}

func TestKeepPersists(t *testing.T) {
	s, ctx := open(t)
	// givenNoConflictingFragments_whenKeep / whenKeepList
	if err := KeepFragments(ctx, s.Pool, []*Fragment{frag(chunkX, mt, 0, 12, "LOCKED", nil)}); err != nil {
		t.Fatal(err)
	}
	if err := KeepFragments(ctx, s.Pool, []*Fragment{frag(chunkY, mt, 0, 12, "LOCKED", nil), frag(chunkY, mt, 12, 24, "LOCKED", nil)}); err != nil {
		t.Fatal(err)
	}
	expect(t, all(t, s), frag(chunkX, mt, 0, 12, "LOCKED", nil), frag(chunkY, mt, 0, 12, "LOCKED", nil), frag(chunkY, mt, 12, 24, "LOCKED", nil))
	// the factory default is LOCKED without reservation, ids ascend
	got := all(t, s)
	if got[0].ID >= got[1].ID || got[1].ID >= got[2].ID {
		t.Fatalf("ids %d %d %d", got[0].ID, got[1].ID, got[2].ID)
	}
}

func TestKeepConflictingListPersistsNothing(t *testing.T) {
	s, ctx := open(t)
	save(t, s, frag(chunkX, mt, 15, 20, "UNLOCKED", nil)) // overlaps the second fragment of the list
	err := KeepFragments(ctx, s.Pool, []*Fragment{frag(chunkX, mt, 0, 12, "LOCKED", nil), frag(chunkX, mt, 12, 24, "LOCKED", nil)})
	if !errors.Is(err, ErrConflict) || err.Error() != "At least one tuple described by the given sequence is already referenced by another TupleChunkFragment." {
		t.Fatal(err)
	}
	expect(t, all(t, s), frag(chunkX, mt, 15, 20, "UNLOCKED", nil))
}

func TestHasConflict(t *testing.T) {
	s, ctx := open(t)
	// givenNoConflictingFragments: [0,42) and [51,MAX) leave [42,51) free
	save(t, s, frag(chunkX, mt, 0, 42, "UNLOCKED", nil), frag(chunkX, mt, 51, math.MaxInt64, "UNLOCKED", nil))
	if c, err := HasConflict(ctx, s.Pool, chunkX, 42, 51); err != nil || c {
		t.Fatal(c, err)
	}
	// another chunk is never in conflict
	if c, _ := HasConflict(ctx, s.Pool, chunkY, 0, 100); c {
		t.Fatal("different chunk")
	}
	// givenConflictingFragment: a locked, reserved fragment inside the range
	save(t, s, frag(chunkX, mt, 43, 50, "LOCKED", str("alreadyReserved")))
	if c, err := HasConflict(ctx, s.Pool, chunkX, 42, 51); err != nil || !c {
		t.Fatal(c, err)
	}
	// touching ranges do not overlap (end is exclusive), a single shared tuple does
	if c, _ := HasConflict(ctx, s.Pool, chunkX, 50, 51); c {
		t.Fatal("[50,51) is free")
	}
	if c, _ := HasConflict(ctx, s.Pool, chunkX, 41, 43); !c {
		t.Fatal("tuple 42 is referenced")
	}
}

func TestFindAvailableFragmentForChunkContainingIndex(t *testing.T) {
	s, ctx := open(t)
	find := func(chunk string, idx int64) *Fragment {
		var f *Fragment
		if err := s.InTx(ctx, func(q Querier) (err error) { f, err = FindAvailableForChunkAt(ctx, q, chunk, idx); return }); err != nil {
			t.Fatal(err)
		}
		return f
	}
	// no fragment at all
	if find(chunkX, 0) != nil {
		t.Fatal("empty db")
	}
	// not activated, reserved, range ends before the index
	save(t, s, frag(chunkX, mt, 0, 42, "LOCKED", nil))
	if find(chunkX, 0) != nil {
		t.Fatal("LOCKED fragments are not available")
	}
	save(t, s, frag(chunkY, mt, 0, 42, "UNLOCKED", str("already reserved")))
	if find(chunkY, 0) != nil {
		t.Fatal("reserved fragments are not available")
	}
	save(t, s, frag(chunkZ, mt, 0, 41, "UNLOCKED", nil))
	if find(chunkZ, 42) != nil || find(chunkZ, 41) != nil {
		t.Fatal("end index is exclusive")
	}
	// matching criteria
	want := save(t, s, frag("00000000-0000-0000-0000-000000000001", mt, 41, 43, "UNLOCKED", nil))[0]
	if got := find(want.ChunkID, 42); got == nil || !same(*got, *want) {
		t.Fatalf("%+v", got)
	}
	// multiple fragments of the same chunk: the one containing the index
	c := "00000000-0000-0000-0000-000000000002"
	save(t, s, frag(c, mt, 0, 41, "UNLOCKED", nil))
	exp := save(t, s, frag(c, mt, 42, 43, "UNLOCKED", nil))[0]
	save(t, s, frag(c, mt, 43, math.MaxInt64, "UNLOCKED", nil))
	if got := find(c, 42); got == nil || !same(*got, *exp) {
		t.Fatalf("%+v", got)
	}
}

func TestFindAvailableFragmentByType(t *testing.T) {
	s, ctx := open(t)
	find := func() *Fragment {
		var f *Fragment
		if err := s.InTx(ctx, func(q Querier) (err error) { f, err = FindAvailableByType(ctx, q, mt); return }); err != nil {
			t.Fatal(err)
		}
		return f
	}
	if find() != nil {
		t.Fatal("empty db")
	}
	save(t, s, frag(chunkX, mt, 0, 42, "LOCKED", nil))
	if find() != nil {
		t.Fatal("no unlocked fragment")
	}
	save(t, s, frag(chunkX, mt, 42, 84, "UNLOCKED", str("alreadyReserved")), frag(chunkX, im, 84, 100, "UNLOCKED", nil))
	if find() != nil {
		t.Fatal("no available fragment of the requested type")
	}
	// multiple available fragments: the one with the lowest id, even if lower ids were deleted
	fs := save(t, s, frag(chunkY, mt, 0, 42, "UNLOCKED", nil), frag(chunkX, mt, 100, 142, "UNLOCKED", nil), frag(chunkZ, mt, 0, 42, "UNLOCKED", nil))
	if got := find(); got == nil || !same(*got, *fs[0]) {
		t.Fatalf("%+v", got)
	}
	if _, err := s.Pool.Exec(ctx, "DELETE FROM tuple_chunk_fragment WHERE fragment_id=$1", fs[0].ID); err != nil {
		t.Fatal(err)
	}
	if got := find(); got == nil || !same(*got, *fs[1]) {
		t.Fatalf("%+v", got)
	}
}

func TestSplitAtAndBefore(t *testing.T) {
	s, ctx := open(t)
	// givenSuccessfulRequest_whenSplitAt: altered fragment [0,7) returned, new [7,12) stored with the same state
	f := save(t, s, frag(chunkX, mt, 0, 12, "UNLOCKED", str("r")))[0]
	var head *Fragment
	if err := s.InTx(ctx, func(q Querier) (err error) { head, err = SplitAt(ctx, q, f, 7); return }); err != nil {
		t.Fatal(err)
	}
	if head != f || head.Start != 0 || head.End != 7 {
		t.Fatalf("%+v", head)
	}
	expect(t, all(t, s), frag(chunkX, mt, 0, 7, "UNLOCKED", str("r")), frag(chunkX, mt, 7, 12, "UNLOCKED", str("r")))
	if got := all(t, s); got[0].ID != f.ID || got[1].ID <= f.ID {
		t.Fatalf("the original row keeps its id, the tail gets a new one: %+v", got)
	}
}

func TestSplitBefore(t *testing.T) {
	s, ctx := open(t)
	f := save(t, s, frag(chunkX, mt, 0, 12, "UNLOCKED", nil))[0]
	// givenSplitIndexOutOfRange_whenSplitBefore_thenDoNothing
	for _, idx := range []int64{12, 20, 0, -1} {
		got, err := SplitBefore(ctx, s.Pool, f, idx)
		if err != nil || got != f {
			t.Fatalf("idx %d: %v %v", idx, got, err)
		}
	}
	expect(t, all(t, s), frag(chunkX, mt, 0, 12, "UNLOCKED", nil))
	// givenSuccessfulRequest_whenSplitBefore: returns the new fragment [7,12), the original is cut to [0,7)
	tail, err := SplitBefore(ctx, s.Pool, f, 7)
	if err != nil || tail == f || tail.Start != 7 || tail.End != 12 || tail.ID == 0 || f.End != 7 {
		t.Fatalf("%+v %v", tail, err)
	}
	expect(t, all(t, s), frag(chunkX, mt, 0, 7, "UNLOCKED", nil), frag(chunkX, mt, 7, 12, "UNLOCKED", nil))
}

func TestUpdatePersists(t *testing.T) {
	s, ctx := open(t)
	f := save(t, s, frag(chunkX, mt, 0, 12, "UNLOCKED", nil))[0]
	f.ReservationID, f.End = str("r1"), 10
	if err := Update(ctx, s.Pool, f); err != nil {
		t.Fatal(err)
	}
	expect(t, all(t, s), frag(chunkX, mt, 0, 10, "UNLOCKED", str("r1")))
}

func TestAvailableTuples(t *testing.T) {
	s, ctx := open(t)
	// giveMultipleFragmentsInDb_whenCountAvailableTuples: only unlocked, unreserved fragments of the type count
	save(t, s,
		frag("00000000-0000-0000-0000-00000000000a", im, 0, math.MaxInt64, "UNLOCKED", nil),
		frag("00000000-0000-0000-0000-00000000000b", mt, 0, math.MaxInt64, "LOCKED", nil),
		frag("00000000-0000-0000-0000-00000000000c", mt, 0, math.MaxInt64, "UNLOCKED", str("alreadyReserved")),
		frag("00000000-0000-0000-0000-00000000000d", mt, 0, 12, "UNLOCKED", nil),
		frag("00000000-0000-0000-0000-00000000000e", mt, 111, 141, "UNLOCKED", nil))
	if n := AvailableTuples(ctx, s.Pool, mt); n != 12+30 {
		t.Fatal(n)
	}
	// givenRepositoryThrowsException_whenGetAvailableTuples_thenReturnZero
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if n := AvailableTuples(cancelled, s.Pool, mt); n != 0 {
		t.Fatal("a failing query yields 0, got", n)
	}
	// no fragment of the type: SUM() is NULL, which is also 0
	if n := AvailableTuples(ctx, s.Pool, "SQUARE_TUPLE_GF2N"); n != 0 {
		t.Fatal(n)
	}
}

func TestUnlockChunk(t *testing.T) {
	s, ctx := open(t)
	// giveMultipleFragmentsInDb_whenActivateForChunk_thenUpdateAccordingly
	other := save(t, s, frag(chunkY, im, 0, math.MaxInt64, "LOCKED", nil))[0]
	one := save(t, s, frag(chunkX, mt, 0, 12, "LOCKED", nil))[0]
	another := save(t, s, frag(chunkX, "SQUARE_TUPLE_GF2N", 111, 141, "LOCKED", nil))[0]
	n, err := UnlockChunk(ctx, s.Pool, chunkX)
	if err != nil || n != 2 {
		t.Fatal(n, err)
	}
	one.Status, another.Status = "UNLOCKED", "UNLOCKED"
	expect(t, all(t, s), other, one, another)
	// givenNotASingleFragmentActivated: no rows -> 0 (the service turns this into an error)
	if n, err := UnlockChunk(ctx, s.Pool, chunkZ); err != nil || n != 0 {
		t.Fatal(n, err)
	}
}

func TestDeleteByReservationAndChunkReferenced(t *testing.T) {
	s, ctx := open(t)
	keep := save(t, s, frag(chunkX, im, 0, math.MaxInt64, "LOCKED", nil))[0]
	save(t, s, frag(chunkX, mt, 0, 12, "LOCKED", str("testReservation")), frag(chunkX, "SQUARE_TUPLE_GF2N", 111, 141, "LOCKED", str("testReservation")))
	if err := DeleteByReservation(ctx, s.Pool, "testReservation"); err != nil {
		t.Fatal(err)
	}
	expect(t, all(t, s), keep)
	// givenNoFragmentAssociatedWithChunkId / givenAnyFragmentAssociatedWithChunkId
	if ref, err := ChunkReferenced(ctx, s.Pool, chunkZ); err != nil || ref {
		t.Fatal(ref, err)
	}
	if ref, err := ChunkReferenced(ctx, s.Pool, chunkX); err != nil || !ref {
		t.Fatal(ref, err)
	}
	// deleting a reservation that does not exist is a no-op
	if err := DeleteByReservation(ctx, s.Pool, "unknown"); err != nil {
		t.Fatal(err)
	}
	expect(t, all(t, s), keep)
}

// DedicatedTransactionServiceTest (runAsNewTransaction runs the supplier): InTx runs the function,
// commits its changes, rolls back when it fails and does so on a connection of its own (REQUIRES_NEW).
func TestInTx(t *testing.T) {
	s, ctx := open(t)
	calls := 0
	if err := s.InTx(ctx, func(q Querier) error {
		calls++
		return Insert(ctx, q, frag(chunkX, mt, 0, 1, "LOCKED", nil))
	}); err != nil || calls != 1 {
		t.Fatal(calls, err)
	}
	expect(t, all(t, s), frag(chunkX, mt, 0, 1, "LOCKED", nil))

	boom := errors.New("boom")
	if err := s.InTx(ctx, func(q Querier) error {
		if err := Insert(ctx, q, frag(chunkY, mt, 0, 1, "LOCKED", nil)); err != nil {
			return err
		}
		return boom
	}); err != boom {
		t.Fatalf("the function's error is returned unchanged, got %v", err)
	}
	expect(t, all(t, s), frag(chunkX, mt, 0, 1, "LOCKED", nil))

	// an inner transaction does not see (and is not rolled back with) the uncommitted work of an outer one
	outer, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := Insert(ctx, outer, frag(chunkZ, mt, 0, 1, "LOCKED", nil)); err != nil {
		t.Fatal(err)
	}
	if err := s.InTx(ctx, func(q Querier) error {
		if ref, _ := ChunkReferenced(ctx, q, chunkZ); ref {
			return fmt.Errorf("outer uncommitted insert is visible")
		}
		return Insert(ctx, q, frag(chunkY, mt, 0, 1, "LOCKED", nil))
	}); err != nil {
		t.Fatal(err)
	}
	if err := outer.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	expect(t, all(t, s), frag(chunkX, mt, 0, 1, "LOCKED", nil), frag(chunkY, mt, 0, 1, "LOCKED", nil))
}
