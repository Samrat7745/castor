package pgstore

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Error paths (closed pool, cancelled context, unreachable server), connection strings with odd
// characters, split invariants as properties, activation and deletion edge cases.

func cancelled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestOpenFailures(t *testing.T) {
	open(t) // makes sure a server exists (the test is skipped without one)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := Open(ctx, "not a dsn ::"); err == nil {
		t.Fatal("garbage DSN accepted")
	}
	if _, err := Open(ctx, DSN("127.0.0.1", 1, "u", "p", "castor")); err == nil {
		t.Fatal("unreachable server accepted")
	}
	if _, err := Open(cancelled(), DSN("127.0.0.1", 1, "u", "p", "castor")); err == nil {
		t.Fatal("cancelled context accepted")
	}
}

func TestPingAndClosedPool(t *testing.T) {
	s, ctx := open(t)
	if err := s.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Ping(cancelled()); err == nil {
		t.Fatal("ping with a cancelled context succeeded")
	}
	// a second pool that is closed: everything reports an error instead of panicking or hanging
	p2, err := Open(ctx, s.Pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	p2.Close()
	f := frag(chunkX, mt, 0, 5, "UNLOCKED", nil)
	rid := "r"
	checks := map[string]func() error{
		"Ping":                    func() error { return p2.Ping(ctx) },
		"EnsureSchema":            func() error { return p2.EnsureSchema(ctx) },
		"InTx":                    func() error { return p2.InTx(ctx, func(Querier) error { t.Fatal("fn ran"); return nil }) },
		"FindAvailableByType":     func() error { _, err := FindAvailableByType(ctx, p2.Pool, mt); return err },
		"FindAvailableForChunkAt": func() error { _, err := FindAvailableForChunkAt(ctx, p2.Pool, chunkX, 0); return err },
		"HasConflict":             func() error { _, err := HasConflict(ctx, p2.Pool, chunkX, 0, 1); return err },
		"Insert":                  func() error { return Insert(ctx, p2.Pool, f) },
		"Update":                  func() error { return Update(ctx, p2.Pool, f) },
		"UnlockChunk":             func() error { _, err := UnlockChunk(ctx, p2.Pool, chunkX); return err },
		"DeleteByReservation":     func() error { return DeleteByReservation(ctx, p2.Pool, rid) },
		"ChunkReferenced":         func() error { _, err := ChunkReferenced(ctx, p2.Pool, chunkX); return err },
		"KeepFragments":           func() error { return KeepFragments(ctx, p2.Pool, []*Fragment{f}) },
		"SplitAt":                 func() error { _, err := SplitAt(ctx, p2.Pool, f, 2); return err },
		"SplitBefore":             func() error { _, err := SplitBefore(ctx, p2.Pool, f, 2); return err },
	}
	for name, fn := range checks {
		if err := fn(); err == nil {
			t.Errorf("%s on a closed pool returned no error", name)
		}
	}
	// AvailableTuples hides errors by contract (Java parity): zero
	if n := AvailableTuples(ctx, p2.Pool, mt); n != 0 {
		t.Fatal(n)
	}
	// a cancelled context fails the same calls on a live pool
	cctx := cancelled()
	for name, fn := range map[string]func() error{
		"InTx":                    func() error { return s.InTx(cctx, func(Querier) error { return nil }) },
		"FindAvailableByType":     func() error { _, err := FindAvailableByType(cctx, s.Pool, mt); return err },
		"FindAvailableForChunkAt": func() error { _, err := FindAvailableForChunkAt(cctx, s.Pool, chunkX, 0); return err },
		"HasConflict":             func() error { _, err := HasConflict(cctx, s.Pool, chunkX, 0, 1); return err },
		"Insert":                  func() error { return Insert(cctx, s.Pool, frag(chunkX, mt, 0, 5, "LOCKED", nil)) },
		"Update":                  func() error { return Update(cctx, s.Pool, f) },
		"UnlockChunk":             func() error { _, err := UnlockChunk(cctx, s.Pool, chunkX); return err },
		"DeleteByReservation":     func() error { return DeleteByReservation(cctx, s.Pool, rid) },
		"ChunkReferenced":         func() error { _, err := ChunkReferenced(cctx, s.Pool, chunkX); return err },
		"KeepFragments":           func() error { return KeepFragments(cctx, s.Pool, []*Fragment{frag(chunkX, mt, 0, 5, "LOCKED", nil)}) },
	} {
		if err := fn(); !errors.Is(err, context.Canceled) {
			t.Errorf("%s with a cancelled context: %v", name, err)
		}
	}
	if n := AvailableTuples(cctx, s.Pool, mt); n != 0 {
		t.Fatal(n)
	}
	expect(t, all(t, s)) // nothing was written
}

func TestInTxCommitFailureAndPanicSafety(t *testing.T) {
	s, ctx := open(t)
	// context cancelled inside the function: the work is rolled back, the context error surfaces
	cctx, cancel := context.WithCancel(ctx)
	err := s.InTx(cctx, func(q Querier) error {
		if err := Insert(cctx, q, frag(chunkX, mt, 0, 1, "LOCKED", nil)); err != nil {
			return err
		}
		cancel()
		return nil
	})
	if err == nil {
		t.Fatal("commit with a cancelled context succeeded")
	}
	expect(t, all(t, s))
	// a constraint violation inside the function aborts the transaction
	err = s.InTx(ctx, func(q Querier) error {
		if err := Insert(ctx, q, frag(chunkX, mt, 0, 1, "LOCKED", nil)); err != nil {
			return err
		}
		_, err := q.Exec(ctx, "INSERT INTO tuple_chunk_fragment (fragment_id, end_index) VALUES (NULL, 1)")
		return err
	})
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "23502" {
		t.Fatalf("want not-null violation, got %v", err)
	}
	expect(t, all(t, s))
}

// A row locked by another transaction makes the "find" queries fail after the 3s lock timeout.
func TestLockTimeoutOnLockedRow(t *testing.T) {
	s, ctx := open(t)
	save(t, s, frag(chunkX, mt, 0, 10, "UNLOCKED", nil))
	holder, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(ctx)
	if f, err := FindAvailableByType(ctx, holder, mt); err != nil || f == nil {
		t.Fatal(f, err)
	}
	start := time.Now()
	err = s.InTx(ctx, func(q Querier) error { _, err := FindAvailableByType(ctx, q, mt); return err })
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "55P03" {
		t.Fatalf("want lock_not_available, got %v", err)
	}
	if d := time.Since(start); d < 2500*time.Millisecond || d > 10*time.Second {
		t.Fatalf("failed after %s, want about 3s", d)
	}
	err = s.InTx(ctx, func(q Querier) error { _, err := FindAvailableForChunkAt(ctx, q, chunkX, 5); return err })
	if !errors.As(err, &pe) || pe.Code != "55P03" {
		t.Fatalf("by chunk: %v", err)
	}
	// the lock timeout is local to the transaction: the pool connection is usable afterwards
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if f, err := FindAvailableByType(ctx, s.Pool, mt); err != nil || f == nil {
		t.Fatal(f, err)
	}
}

func TestDSNEscapesAwkwardCredentials(t *testing.T) {
	for _, c := range []struct{ user, pw, db string }{
		{"postgres", "pw", "castor"},
		{"us er", "p@ss:w/ord?#%&=+", "castor"},
		{"üser", "pässwörd☃", "castor"},
		{"u", "", "castor"},
		{"a:b", "c@d", "db-1_x.y"},
		{"u", "'; DROP TABLE x; --", "castor"},
		{"u", "100%", "castor"},
		{"u", "\n\t ", "castor"},
	} {
		dsn := DSN("db.example", 5433, c.user, c.pw, c.db)
		cfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			t.Errorf("%q: %v", dsn, err)
			continue
		}
		if cfg.User != c.user || cfg.Password != c.pw || cfg.Database != c.db || cfg.Host != "db.example" || cfg.Port != 5433 {
			t.Errorf("%q round trip: %q %q %q %s:%d", dsn, cfg.User, cfg.Password, cfg.Database, cfg.Host, cfg.Port)
		}
	}
	if got := urlEsc("aZ09-_.~"); got != "aZ09-_.~" {
		t.Fatal(got)
	}
	if got := urlEsc("ü "); got != "%C3%BC%20" {
		t.Fatal(got)
	}
}

// A role with an awkward password can really log in through DSN().
func TestConnectWithAwkwardPassword(t *testing.T) {
	s, ctx := open(t)
	const pw = "p@ss:w/ord?#%&=+ü☃"
	if _, err := s.Pool.Exec(ctx, "DROP ROLE IF EXISTS castor_odd"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, "CREATE ROLE castor_odd LOGIN PASSWORD '"+strings.ReplaceAll(pw, "'", "''")+"'"); err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Exec(ctx, "DROP ROLE castor_odd")
	cfg := s.Pool.Config().ConnConfig
	pool, err := pgxpool.New(ctx, DSN(cfg.Host, int(cfg.Port), "castor_odd", pw, cfg.Database)+"&sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

// --- splitting properties ---

func snapshotChunk(t *testing.T, s *Store, chunk string) []Fragment {
	t.Helper()
	var out []Fragment
	for _, f := range all(t, s) {
		if f.ChunkID == chunk {
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out
}

func assertCovers(t *testing.T, fs []Fragment, size int64) {
	t.Helper()
	at := int64(0)
	for _, f := range fs {
		if f.End <= f.Start {
			t.Fatalf("zero or negative length fragment %v in %v", f, fs)
		}
		if f.Start != at {
			t.Fatalf("gap or overlap at %d: %v", at, fs)
		}
		at = f.End
	}
	if at != size {
		t.Fatalf("fragments cover [0,%d), chunk has %d tuples: %v", at, size, fs)
	}
}

// Random sequences of SplitAt / SplitBefore on random fragments keep the fragments of a chunk
// contiguous, covering the chunk exactly and free of zero-length pieces.
func TestSplitPropertyFragmentsStayContiguous(t *testing.T) {
	s, ctx := open(t)
	rnd := rand.New(rand.NewSource(7))
	for round := 0; round < 25; round++ {
		if _, err := s.Pool.Exec(ctx, "DELETE FROM tuple_chunk_fragment"); err != nil {
			t.Fatal(err)
		}
		chunk := uuid.NewString()
		size := 1 + rnd.Int63n(400)
		save(t, s, frag(chunk, mt, 0, size, "UNLOCKED", nil))
		for step := 0; step < 40; step++ {
			fs := snapshotChunk(t, s, chunk)
			f := fs[rnd.Intn(len(fs))]
			if f.End-f.Start < 2 {
				continue
			}
			idx := f.Start + 1 + rnd.Int63n(f.End-f.Start-1) // strictly inside
			cp := f
			err := s.InTx(ctx, func(q Querier) error {
				var err error
				if rnd.Intn(2) == 0 {
					var head *Fragment
					if head, err = SplitAt(ctx, q, &cp, idx); err == nil && (head.Start != f.Start || head.End != idx) {
						err = fmt.Errorf("SplitAt returned [%d,%d)", head.Start, head.End)
					}
				} else {
					var tail *Fragment
					if tail, err = SplitBefore(ctx, q, &cp, idx); err == nil && (tail.Start != idx || tail.End != f.End) {
						err = fmt.Errorf("SplitBefore returned [%d,%d)", tail.Start, tail.End)
					}
				}
				return err
			})
			if err != nil {
				t.Fatalf("round %d step %d split of %v at %d: %v", round, step, f, idx, err)
			}
			assertCovers(t, snapshotChunk(t, s, chunk), size)
		}
		// sums of free tuples are unchanged by splitting
		if n := AvailableTuples(ctx, s.Pool, mt); n != size {
			t.Fatalf("available %d, want %d", n, size)
		}
	}
}

// SplitBefore outside the fragment is a no-op and creates nothing.
func TestSplitBeforeOutsideRangeDoesNothing(t *testing.T) {
	s, ctx := open(t)
	f := save(t, s, frag(chunkX, mt, 10, 20, "UNLOCKED", nil))[0]
	for _, idx := range []int64{-5, 0, 10, 20, 21, 1 << 40} {
		got, err := SplitBefore(ctx, s.Pool, f, idx)
		if err != nil || got != f {
			t.Fatalf("idx %d: %v %v", idx, got, err)
		}
	}
	expect(t, all(t, s), frag(chunkX, mt, 10, 20, "UNLOCKED", nil))
	// SplitAt past the end is rejected by the entity rules, not stored
	err := s.InTx(ctx, func(q Querier) error { _, err := SplitAt(ctx, q, f, 25); return err })
	if err == nil || err.Error() != "Illegal last index (20)." {
		t.Fatal(err)
	}
	expect(t, all(t, s), frag(chunkX, mt, 10, 20, "UNLOCKED", nil))
}

// Generated fragments (the upload path): random chunk and fragment sizes.
func TestKeepFragmentsPropertyRandomSizes(t *testing.T) {
	s, ctx := open(t)
	rnd := rand.New(rand.NewSource(11))
	for i := 0; i < 20; i++ {
		chunk := uuid.NewString()
		n, size := 1+rnd.Int63n(5000), 1+rnd.Int63n(700)
		var frs []*Fragment
		for start := int64(0); start < n; start += size {
			frs = append(frs, frag(chunk, mt, start, min(start+size, n), "LOCKED", nil))
		}
		if err := s.InTx(ctx, func(q Querier) error { return KeepFragments(ctx, q, frs) }); err != nil {
			t.Fatal(err)
		}
		assertCovers(t, snapshotChunk(t, s, chunk), n)
		for _, f := range snapshotChunk(t, s, chunk) {
			if f.End-f.Start > size {
				t.Fatalf("fragment %v larger than %d", f, size)
			}
		}
		// the very same chunk cannot be kept twice, nor any overlapping slice of it
		if err := KeepFragments(ctx, s.Pool, []*Fragment{frag(chunk, mt, rnd.Int63n(n), n, "LOCKED", nil)}); n > 0 && !errors.Is(err, ErrConflict) {
			// start may equal n-0 only when rnd gives n-1..: range is non empty since start < n
			t.Fatalf("overlap not detected: %v", err)
		}
	}
}

func TestKeepFragmentsEdgeCases(t *testing.T) {
	s, ctx := open(t)
	// empty list and adjacent (touching) fragments are fine
	if err := KeepFragments(ctx, s.Pool, nil); err != nil {
		t.Fatal(err)
	}
	if err := KeepFragments(ctx, s.Pool, []*Fragment{frag(chunkX, mt, 0, 10, "LOCKED", nil), frag(chunkX, mt, 10, 20, "LOCKED", nil)}); err != nil {
		t.Fatal(err)
	}
	// the same range of another chunk does not conflict
	if err := KeepFragments(ctx, s.Pool, []*Fragment{frag(chunkY, mt, 0, 20, "LOCKED", nil)}); err != nil {
		t.Fatal(err)
	}
	// a conflict leaves earlier members of the same call unwritten
	err := KeepFragments(ctx, s.Pool, []*Fragment{frag(chunkZ, mt, 0, 5, "LOCKED", nil), frag(chunkX, mt, 19, 25, "LOCKED", nil)})
	if !errors.Is(err, ErrConflict) || len(snapshotChunk(t, s, chunkZ)) != 0 {
		t.Fatalf("%v", err)
	}
	// a malformed chunk id is a database error, not a panic
	bad := &Fragment{ChunkID: "not-a-uuid", TupleType: mt, Start: 0, End: 1, Status: "LOCKED"}
	if err := KeepFragments(ctx, s.Pool, []*Fragment{bad}); err == nil || errors.Is(err, ErrConflict) {
		t.Fatalf("invalid uuid: %v", err)
	}
	if _, err := ChunkReferenced(ctx, s.Pool, "not-a-uuid"); err == nil {
		t.Fatal("invalid uuid accepted")
	}
}

func TestActivateAlreadyActiveAndMixedChunks(t *testing.T) {
	s, ctx := open(t)
	save(t, s, frag(chunkX, mt, 0, 5, "LOCKED", nil), frag(chunkX, mt, 5, 9, "UNLOCKED", nil), frag(chunkY, mt, 0, 3, "LOCKED", nil))
	// activating re-activates every fragment, also those already active, and reports all rows
	n, err := UnlockChunk(ctx, s.Pool, chunkX)
	if err != nil || n != 2 {
		t.Fatal(n, err)
	}
	n, err = UnlockChunk(ctx, s.Pool, chunkX)
	if err != nil || n != 2 {
		t.Fatalf("second activation: %d %v", n, err)
	}
	// reserved fragments become UNLOCKED too but stay unavailable
	rid := "res"
	save(t, s, frag(chunkZ, mt, 0, 4, "LOCKED", &rid))
	if n, err = UnlockChunk(ctx, s.Pool, chunkZ); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if got := AvailableTuples(ctx, s.Pool, mt); got != 9 {
		t.Fatalf("available %d, want 9 (chunk X only)", got)
	}
	// unknown chunk: zero rows, no error
	if n, err = UnlockChunk(ctx, s.Pool, uuid.NewString()); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	expect(t, snapshotChunk(t, s, chunkY), frag(chunkY, mt, 0, 3, "LOCKED", nil))
}

func TestDeleteConsumedChunks(t *testing.T) {
	s, ctx := open(t)
	r1, r2 := "r1", "r2"
	save(t, s, frag(chunkX, mt, 0, 5, "UNLOCKED", &r1), frag(chunkX, mt, 5, 8, "UNLOCKED", &r1),
		frag(chunkX, mt, 8, 10, "UNLOCKED", nil), frag(chunkY, mt, 0, 5, "UNLOCKED", &r2))
	if err := DeleteByReservation(ctx, s.Pool, r1); err != nil {
		t.Fatal(err)
	}
	// chunk X still has a free fragment, chunk Y is fully reserved but not yet deleted
	for _, c := range []struct {
		chunk string
		want  bool
	}{{chunkX, true}, {chunkY, true}} {
		if ref, err := ChunkReferenced(ctx, s.Pool, c.chunk); err != nil || ref != c.want {
			t.Fatalf("%s: %v %v", c.chunk, ref, err)
		}
	}
	if err := DeleteByReservation(ctx, s.Pool, r2); err != nil {
		t.Fatal(err)
	}
	if ref, _ := ChunkReferenced(ctx, s.Pool, chunkY); ref {
		t.Fatal("fully consumed chunk is still referenced")
	}
	// deleting again, or an unknown / empty reservation id, deletes nothing and is no error
	for _, id := range []string{r1, "unknown", ""} {
		if err := DeleteByReservation(ctx, s.Pool, id); err != nil {
			t.Fatal(err)
		}
	}
	expect(t, all(t, s), frag(chunkX, mt, 8, 10, "UNLOCKED", nil))
}

func TestOddReservationAndTypeIDs(t *testing.T) {
	s, ctx := open(t)
	odd := []string{"ünï☃code_ключ", "with'quote", `back\slash`, "semi;colon--", "percent%_like", strings.Repeat("x", 255), "\ttab", "emoji 🎲", " leading and trailing "}
	for i, id := range odd {
		id := id
		f := frag(chunkX, mt, int64(i)*10, int64(i)*10+10, "UNLOCKED", &id)
		save(t, s, f)
	}
	got := all(t, s)
	if len(got) != len(odd) {
		t.Fatalf("%d rows", len(got))
	}
	for i, id := range odd {
		if got[i].ReservationID == nil || *got[i].ReservationID != id {
			t.Fatalf("id %d: %q came back as %v", i, id, got[i].ReservationID)
		}
	}
	// ids are matched exactly: LIKE wildcards and prefixes do not select neighbours
	if err := DeleteByReservation(ctx, s.Pool, "percent%"); err != nil {
		t.Fatal(err)
	}
	if err := DeleteByReservation(ctx, s.Pool, "percent%_like"); err != nil {
		t.Fatal(err)
	}
	if n := len(all(t, s)); n != len(odd)-1 {
		t.Fatalf("%d rows left", n)
	}
	// too long for varchar(255) is a database error
	long := strings.Repeat("y", 256)
	if err := Insert(ctx, s.Pool, frag(chunkY, mt, 0, 1, "LOCKED", &long)); err == nil {
		t.Fatal("256 character reservation id accepted")
	}
	// chunk ids: upper case and without hyphens are valid UUID input for postgres, returned in canonical form
	f := frag(strings.ToUpper(chunkY), mt, 0, 1, "LOCKED", nil)
	if err := Insert(ctx, s.Pool, f); err != nil {
		t.Fatal(err)
	}
	if ref, err := ChunkReferenced(ctx, s.Pool, strings.ReplaceAll(chunkY, "-", "")); err != nil || !ref {
		t.Fatal(ref, err)
	}
	// unknown tuple type names are stored verbatim and counted separately
	save(t, s, frag(chunkZ, "NOT_A_TYPE", 0, 7, "UNLOCKED", nil))
	if AvailableTuples(ctx, s.Pool, "NOT_A_TYPE") != 7 || AvailableTuples(ctx, s.Pool, "") != 0 {
		t.Fatal("type matching")
	}
}

func TestNewFragmentBoundaries(t *testing.T) {
	for _, c := range []struct {
		start, end int64
		ok         bool
	}{{0, 0, true}, {0, 1, true}, {5, 5, true}, {-1, 5, false}, {5, 4, false}, {1<<62 - 1, 1 << 62, true}, {-1 << 63, 0, false}} {
		_, err := NewFragment(chunkX, mt, c.start, c.end, "LOCKED", nil)
		if (err == nil) != c.ok {
			t.Errorf("[%d,%d): %v", c.start, c.end, err)
		}
	}
}

func TestScanRowErrors(t *testing.T) {
	s, ctx := open(t)
	// scan reports database errors other than "no rows"
	_, err := scan(s.Pool.QueryRow(ctx, "SELECT 1/0"))
	if err == nil {
		t.Fatal("division by zero not reported")
	}
	// and wrong column shapes
	if _, err := scan(s.Pool.QueryRow(ctx, "SELECT 1")); err == nil {
		t.Fatal("short row accepted")
	}
	if f, err := scan(s.Pool.QueryRow(ctx, "SELECT "+cols+" FROM tuple_chunk_fragment WHERE false")); f != nil || err != nil {
		t.Fatal(f, err)
	}
}
