package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Samrat7745/castor/go/internal/model"
	"github.com/Samrat7745/castor/go/internal/objstore"
	"github.com/Samrat7745/castor/go/internal/pgstore"
	"github.com/Samrat7745/castor/go/internal/rediscache"
	"github.com/Samrat7745/castor/go/internal/testenv"
	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/redis/go-redis/v9"
)

// Fixtures shared by the ports of the Java service tests (ports_*_test.go).

// spyStore records calls to a TupleStore and can inject failures (the Mockito mocks of the Java tests).
type spyStore struct {
	TupleStore
	mu                          sync.Mutex
	saveErr, readErr, deleteErr error
	reads                       [][3]any // chunk, offset, length
	deletes                     []string
}

func (s *spyStore) Save(ctx context.Context, id string, d []byte) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	return s.TupleStore.Save(ctx, id, d)
}

func (s *spyStore) Read(ctx context.Context, id string, off, n int64) ([]byte, error) {
	s.mu.Lock()
	s.reads = append(s.reads, [3]any{id, off, n})
	s.mu.Unlock()
	if s.readErr != nil {
		return nil, s.readErr
	}
	return s.TupleStore.Read(ctx, id, off, n)
}

func (s *spyStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	s.deletes = append(s.deletes, id)
	s.mu.Unlock()
	if s.deleteErr != nil {
		return s.deleteErr
	}
	return s.TupleStore.Delete(ctx, id)
}

// cacheOnly is a service with a Redis cache (miniredis) but neither database nor object store:
// enough for the paths of ReservationCachingService that only touch the cache.
func cacheOnly(t *testing.T, master bool) (*Service, *miniredis.Miniredis, *spyStore, *fakePeers) {
	t.Helper()
	mr := miniredis.RunT(t)
	cache := rediscache.New(redis.NewClient(&redis.Options{Addr: mr.Addr()}), `"castor-reservation-store"`, `"castor-consumption-"`, 1000)
	spy := &spyStore{TupleStore: &memStore{objs: map[string][]byte{}}}
	peers := &fakePeers{shareOK: true}
	s := &Service{S: Settings{IsMaster: master, InitialFragmentSize: 7, ReservationTimeout: 200 * time.Millisecond, RetryDelay: 5 * time.Millisecond},
		Cache: cache, Tuples: spy, Now: func() time.Time { return time.UnixMilli(1_700_000_000_000) }}
	if master {
		s.Peers = peers
	}
	return s, mr, spy, peers
}

// newRealFix is newFix with a real Redis 7.0 and a real Minio behind the service (the Java *IT
// classes' Spring context). Store names are unquoted like in the Java test profile.
func newRealFix(t *testing.T, master bool) *fix {
	t.Helper()
	ctx := context.Background()
	pg, err := pgstore.Open(ctx, testenv.PostgresDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Close)
	if _, err := pg.Pool.Exec(ctx, "DELETE FROM tuple_chunk_fragment"); err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: testenv.RedisAddr(t)})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	ep := testenv.MinioEndpoint(t)
	const bucket = "minio-castor-test-bucket"
	clearBucket(t, ep, bucket)
	store, err := objstore.New(ctx, ep, bucket, testenv.MinioAccessKey, testenv.MinioSecretKey)
	if err != nil {
		t.Fatal(err)
	}
	spy := &spyStore{TupleStore: store}
	f := &fix{store: nil, spy: spy, rdb: rdb, bucket: bucket, minioEP: ep, peers: &fakePeers{shareOK: true}, pg: pg, now: time.UnixMilli(1_700_000_000_000)}
	f.svc = &Service{S: Settings{IsMaster: master, InitialFragmentSize: 1000, ReservationTimeout: 300 * time.Millisecond, RetryDelay: 10 * time.Millisecond},
		PG: pg, Cache: rediscache.New(rdb, "castor-reservation-store", "castor-consumption-", 1000), Tuples: spy, Now: func() time.Time { return f.now }}
	if master {
		f.svc.Peers = f.peers
	}
	return f
}

func rawMinio(t *testing.T, ep string) *minio.Client {
	t.Helper()
	c, err := minio.New(strings.TrimPrefix(ep, "http://"), &minio.Options{Creds: credentials.NewStaticV4(testenv.MinioAccessKey, testenv.MinioSecretKey, "")})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// clearBucket removes the bucket with all its objects (PersistenceTestEnvironment.clearAllData).
func clearBucket(t *testing.T, ep, bucket string) {
	t.Helper()
	ctx := context.Background()
	c := rawMinio(t, ep)
	if ok, _ := c.BucketExists(ctx, bucket); !ok {
		return
	}
	for o := range c.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
		_ = c.RemoveObject(ctx, bucket, o.Key, minio.RemoveObjectOptions{})
	}
	if err := c.RemoveBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
}

// bucketObjects lists the object keys currently stored in the fixture's bucket.
func (f *fix) bucketObjects(t *testing.T) []string {
	t.Helper()
	var out []string
	for o := range rawMinio(t, f.minioEP).ListObjects(context.Background(), f.bucket, minio.ListObjectsOptions{Recursive: true}) {
		out = append(out, o.Key)
	}
	return out
}

func frag(chunk, typ string, start, end int64, status string, res *string) *pgstore.Fragment {
	f, err := pgstore.NewFragment(chunk, typ, start, end, status, res)
	if err != nil {
		panic(err)
	}
	return f
}

func rid(s string) *string { return &s }

// seed stores fragments (CrudRepository.save).
func (f *fix) seed(t *testing.T, fs ...*pgstore.Fragment) {
	t.Helper()
	for _, fr := range fs {
		if err := pgstore.Insert(context.Background(), f.pg.Pool, fr); err != nil {
			t.Fatal(err)
		}
	}
}

// fragments returns all stored fragments ordered by id (CrudRepository.findAll).
func (f *fix) fragments(t *testing.T) []pgstore.Fragment {
	t.Helper()
	rows, err := f.pg.Pool.Query(context.Background(), "SELECT fragment_id, tuple_chunk_id::text, tuple_type, start_index, end_index, activation_status, reservation_id FROM tuple_chunk_fragment ORDER BY fragment_id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []pgstore.Fragment
	for rows.Next() {
		var fr pgstore.Fragment
		if err := rows.Scan(&fr.ID, &fr.ChunkID, &fr.TupleType, &fr.Start, &fr.End, &fr.Status, &fr.ReservationID); err != nil {
			t.Fatal(err)
		}
		out = append(out, fr)
	}
	return out
}

func fragEq(a, b pgstore.Fragment) bool {
	return a.ChunkID == b.ChunkID && a.TupleType == b.TupleType && a.Start == b.Start && a.End == b.End && a.Status == b.Status &&
		(a.ReservationID == nil) == (b.ReservationID == nil) && (a.ReservationID == nil || *a.ReservationID == *b.ReservationID)
}

func fragString(fr pgstore.Fragment) string {
	r := "<nil>"
	if fr.ReservationID != nil {
		r = *fr.ReservationID
	}
	return fmt.Sprintf("{%s %s [%d,%d) %s res=%s}", fr.ChunkID, fr.TupleType, fr.Start, fr.End, fr.Status, r)
}

// wantFragments asserts the exact ordered fragment list; wantContains (TupleChunkFragmentEntityListMatcher)
// only that every expected fragment is present.
func (f *fix) wantFragments(t *testing.T, want ...*pgstore.Fragment) {
	t.Helper()
	got := f.fragments(t)
	if len(got) != len(want) {
		t.Fatalf("want %d fragments, got %d: %s", len(want), len(got), describe(got))
	}
	for i := range want {
		if !fragEq(got[i], *want[i]) {
			t.Fatalf("fragment %d: got %s want %s\nall: %s", i, fragString(got[i]), fragString(*want[i]), describe(got))
		}
	}
}

func (f *fix) wantContains(t *testing.T, want ...*pgstore.Fragment) {
	t.Helper()
	got := f.fragments(t)
	for _, w := range want {
		found := false
		for _, g := range got {
			found = found || fragEq(g, *w)
		}
		if !found {
			t.Fatalf("%s not found in %s", fragString(*w), describe(got))
		}
	}
}

func describe(fs []pgstore.Fragment) string {
	var s []string
	for _, fr := range fs {
		s = append(s, fragString(fr))
	}
	return "[" + strings.Join(s, ", ") + "]"
}

// hideFragments makes the "find available fragment" query come back empty although the fragments
// are counted as available, to reach the defensive branch of composeElements (a concurrent
// consumer took the fragment in between).
type hideFragments struct{ pgstore.Querier }

type noRow struct{}

func (noRow) Scan(...any) error { return pgx.ErrNoRows }

func (h hideFragments) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if strings.Contains(sql, "FOR UPDATE") {
		return noRow{}
	}
	return h.Querier.QueryRow(ctx, sql, args...)
}

func mustType(name string) model.TupleType { t, _ := model.ParseTupleType(name); return t }
