package service

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Samrat7745/castor/go/internal/javaser"
	"github.com/Samrat7745/castor/go/internal/model"
	"github.com/Samrat7745/castor/go/internal/pgstore"
	"github.com/Samrat7745/castor/go/internal/rediscache"
	"github.com/Samrat7745/castor/go/internal/testenv"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

// Reservation invariants under load: a master and a slave service (own database, own Redis
// database, the master reaching the slave in process like the REST client does) serve many
// concurrent, randomly sized requests over several chunks. Postgres, Redis are real containers.

// slaveLink is the inter-VCP client of the master: it calls the slave service directly. failShare
// makes the master's sharing fail (before the slave sees it) for the requests it selects.
type slaveLink struct {
	slave     *Service
	failShare func(id string) bool
	shared    atomic.Int64
}

func (l *slaveLink) ShareReservation(ctx context.Context, r *model.Reservation) (bool, error) {
	if l.failShare != nil && l.failShare(r.ReservationID) {
		return false, nil
	}
	c := *r
	c.Reservations = append([]model.ReservationElement(nil), r.Reservations...)
	if err := l.slave.ApplyReservation(ctx, &c); err != nil {
		return false, nil
	}
	l.shared.Add(1)
	return true, nil
}

func (l *slaveLink) UpdateReservationStatus(ctx context.Context, id string, s model.ActivationStatus) error {
	return l.slave.UpdateReservation(ctx, id, s)
}

type pair struct {
	master, slave *Service
	mpg, spg      *pgstore.Store
	link          *slaveLink
	sizes         []int64 // tuples per chunk
	chunks        []string
}

// slaveDSN points the master's DSN at a second database of the same server.
func slaveDSN(t *testing.T, dsn string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	if _, err := c.Exec(ctx, "CREATE DATABASE castor_slave"); err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatal(err)
	}
	u.Path = "/castor_slave"
	return u.String()
}

func newPair(t *testing.T, sizes []int64, fragSize int64, slaveTimeout time.Duration) *pair {
	t.Helper()
	ctx := context.Background()
	dsn := testenv.PostgresDSN(t)
	open := func(dsn string) *pgstore.Store {
		pg, err := pgstore.Open(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pg.Close)
		if _, err := pg.Pool.Exec(ctx, "DELETE FROM tuple_chunk_fragment"); err != nil {
			t.Fatal(err)
		}
		return pg
	}
	p := &pair{mpg: open(dsn), spg: open(slaveDSN(t, dsn)), sizes: sizes}
	addr := testenv.RedisAddr(t)
	mk := func(db int) *rediscache.Cache {
		rdb := redis.NewClient(&redis.Options{Addr: addr, DB: db})
		t.Cleanup(func() { _ = rdb.Close() })
		if err := rdb.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		return rediscache.New(rdb, "castor-reservation-store", "castor-consumption-", 1000)
	}
	fixed := func() time.Time { return time.UnixMilli(1_700_000_000_000) }
	p.slave = &Service{S: Settings{IsMaster: false, InitialFragmentSize: fragSize, ReservationTimeout: slaveTimeout, RetryDelay: 5 * time.Millisecond},
		PG: p.spg, Cache: mk(1), Tuples: &memStore{objs: map[string][]byte{}}, Now: fixed}
	p.link = &slaveLink{slave: p.slave}
	p.master = &Service{S: Settings{IsMaster: true, InitialFragmentSize: fragSize, ReservationTimeout: slaveTimeout, RetryDelay: 5 * time.Millisecond},
		PG: p.mpg, Cache: mk(0), Tuples: &memStore{objs: map[string][]byte{}}, Peers: p.link, Now: fixed}

	bit := mustType("BIT_GFP")
	for i, n := range sizes {
		id := uuid.NewSHA1(uuid.NameSpaceOID, []byte{byte(i)}).String()
		p.chunks = append(p.chunks, id)
		data := make([]byte, int(n)*bit.TupleSize())
		for j := int64(0); j < n; j++ { // every tuple carries its own (chunk, index)
			binary.BigEndian.PutUint32(data[int(j)*bit.TupleSize():], uint32(i))
			binary.BigEndian.PutUint64(data[int(j)*bit.TupleSize()+4:], uint64(j))
		}
		for _, s := range []*Service{p.master, p.slave} {
			var out [][]byte
			if err := s.HandleUpload(ctx, javaser.EncodeTupleChunk(id, bit.Name(), data), func(b []byte) { out = append(out, b) }); err != nil {
				t.Fatal(err)
			}
			if err := s.ActivateChunk(ctx, id); err != nil {
				t.Fatal(err)
			}
		}
	}
	return p
}

type span struct{ start, end int64 }

// snapshot reads all fragments of one database (one statement, hence one consistent snapshot).
func snapshot(ctx context.Context, pg *pgstore.Store) (map[string][]pgstore.Fragment, error) {
	rows, err := pg.Pool.Query(ctx, "SELECT fragment_id, tuple_chunk_id::text, tuple_type, start_index, end_index, activation_status, reservation_id FROM tuple_chunk_fragment ORDER BY tuple_chunk_id, start_index")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]pgstore.Fragment{}
	for rows.Next() {
		var f pgstore.Fragment
		if err := rows.Scan(&f.ID, &f.ChunkID, &f.TupleType, &f.Start, &f.End, &f.Status, &f.ReservationID); err != nil {
			return nil, err
		}
		out[f.ChunkID] = append(out[f.ChunkID], f)
	}
	return out, rows.Err()
}

// checkFragments: no zero or negative length fragment, nothing outside the chunk, no overlap.
func (p *pair) checkFragments(snap map[string][]pgstore.Fragment) error {
	known := map[string]int64{}
	for i, id := range p.chunks {
		known[id] = p.sizes[i]
	}
	for id, fs := range snap {
		size, ok := known[id]
		if !ok {
			return fmt.Errorf("fragment of unknown chunk %s", id)
		}
		prevEnd := int64(0)
		for _, f := range fs { // ordered by start
			switch {
			case f.End <= f.Start:
				return fmt.Errorf("empty or inverted fragment %s", fragString(f))
			case f.Start < 0 || f.End > size:
				return fmt.Errorf("fragment outside chunk (size %d) %s", size, fragString(f))
			case f.Start < prevEnd:
				return fmt.Errorf("overlapping fragments in %s: %s starts before %d", id, fragString(f), prevEnd)
			}
			prevEnd = f.End
		}
	}
	return nil
}

// free merges the free (UNLOCKED, unreserved) fragments of each chunk into maximal ranges.
func free(snap map[string][]pgstore.Fragment) map[string][]span {
	out := map[string][]span{}
	for id, fs := range snap {
		for _, f := range fs {
			if f.Status != "UNLOCKED" || f.ReservationID != nil {
				continue
			}
			if n := len(out[id]); n > 0 && out[id][n-1].end == f.Start {
				out[id][n-1].end = f.End
			} else {
				out[id] = append(out[id], span{f.Start, f.End})
			}
		}
	}
	return out
}

func freeCount(m map[string][]span) (n int64) {
	for _, ss := range m {
		for _, s := range ss {
			n += s.end - s.start
		}
	}
	return
}

type outcome struct {
	count        int64
	mData, sData []byte
	mErr, sErr   error
}

// run fires the requests (each on master and slave concurrently, as the two VCPs' players do).
func (p *pair) run(t *testing.T, counts []int64) []outcome {
	t.Helper()
	bit := mustType("BIT_GFP")
	res := make([]outcome, len(counts))
	var wg sync.WaitGroup
	ctx := context.Background()
	for i, n := range counts {
		wg.Add(2)
		reqID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprint("req", i))).String()
		sctx, cancelSlave := context.WithCancel(ctx)
		res[i].count = n
		done := make(chan struct{})
		go func() {
			defer wg.Done()
			res[i].mData, res[i].mErr = p.master.GetTuples(ctx, bit, n, reqID)
			if res[i].mErr != nil {
				cancelSlave() // nobody will ever release this reservation
			}
			close(done)
		}()
		go func() {
			defer wg.Done()
			defer cancelSlave()
			res[i].sData, res[i].sErr = p.slave.GetTuples(sctx, bit, n, reqID)
			<-done
		}()
	}
	wg.Wait()
	return res
}

func TestStressReservationsMasterAndSlave(t *testing.T) {
	for _, tc := range []struct {
		name      string
		faultEach int // every n-th sharing fails (0: never)
	}{{"no faults", 0}, {"failing slave sharing", 4}} {
		t.Run(tc.name, func(t *testing.T) { stress(t, tc.faultEach) })
	}
}

func stress(t *testing.T, faultEach int) {
	ctx := context.Background()
	sizes := []int64{523, 100, 777, 61, 340} // 1801 tuples
	p := newPair(t, sizes, 37, 20*time.Second)
	bit := mustType("BIT_GFP")
	var total int64
	for _, s := range sizes {
		total += s
	}
	if faultEach > 0 {
		var n atomic.Int64
		p.link.failShare = func(string) bool { return n.Add(1)%int64(faultEach) == 0 }
	}
	before, err := snapshot(ctx, p.mpg)
	if err != nil || freeCount(free(before)) != total {
		t.Fatalf("seed: %v free=%d", err, freeCount(free(before)))
	}

	// the invariants of the fragment table hold at every instant, not only at the end
	stop := make(chan struct{})
	monitor := make(chan error, 1)
	go func() {
		defer close(monitor)
		for {
			select {
			case <-stop:
				return
			case <-time.After(15 * time.Millisecond):
			}
			for _, pg := range []*pgstore.Store{p.mpg, p.spg} {
				snap, err := snapshot(ctx, pg)
				if err == nil {
					err = p.checkFragments(snap)
				}
				if err != nil {
					monitor <- err
					return
				}
			}
		}
	}()

	rnd := rand.New(rand.NewSource(20240601))
	var counts []int64
	var demand int64
	for demand < total*5/4 { // more demand than supply
		n := 1 + rnd.Int63n(60)
		counts = append(counts, n)
		demand += n
	}
	res := p.run(t, counts)
	close(stop)
	if err := <-monitor; err != nil {
		t.Fatalf("invariant violated while running: %v", err)
	}

	seen := map[[2]uint64]int{}
	var granted, grantedReqs int64
	var errKinds = map[string]int{}
	for i, r := range res {
		if r.mErr != nil {
			if r.sErr == nil {
				t.Fatalf("request %d failed on master (%v) but succeeded on slave", i, r.mErr)
			}
			msg := r.mErr.Error()
			switch {
			case strings.HasPrefix(msg, "Insufficient Tuples"), msg == MsgSharingFailed, msg == MsgFailedReserve, msg == MsgCreateFailed:
				errKinds[msg[:12]]++
			default:
				t.Fatalf("request %d: unexpected error %q", i, msg)
			}
			continue
		}
		if r.sErr != nil {
			t.Fatalf("request %d granted on master but slave failed: %v", i, r.sErr)
		}
		if int64(len(r.mData)) != r.count*int64(bit.TupleSize()) {
			t.Fatalf("request %d: %d bytes for %d tuples", i, len(r.mData), r.count)
		}
		if string(r.mData) != string(r.sData) {
			t.Fatalf("request %d: master and slave hand out different tuples", i)
		}
		for j := 0; j < len(r.mData); j += bit.TupleSize() {
			k := [2]uint64{uint64(binary.BigEndian.Uint32(r.mData[j:])), binary.BigEndian.Uint64(r.mData[j+4:])}
			if k[1] >= uint64(sizes[k[0]]) {
				t.Fatalf("request %d: tuple %v does not exist", i, k)
			}
			if prev, dup := seen[k]; dup {
				t.Fatalf("tuple %v handed out twice (requests %d and %d)", k, prev, i)
			}
			seen[k] = i
		}
		granted += r.count
		grantedReqs++
	}
	t.Logf("%d requests, %d granted (%d tuples of %d), failures %v", len(res), grantedReqs, granted, total, errKinds)
	if grantedReqs == 0 || granted > total {
		t.Fatalf("granted %d tuples of %d", granted, total)
	}
	if faultEach > 0 && errKinds[MsgSharingFailed[:12]] == 0 {
		t.Fatal("fault injection never fired")
	}

	// conservation: whatever was not handed out is still free, on the master and on the slave
	mAfter, err := snapshot(ctx, p.mpg)
	if err != nil {
		t.Fatal(err)
	}
	sAfter, err := snapshot(ctx, p.spg)
	if err != nil {
		t.Fatal(err)
	}
	for name, s := range map[string]map[string][]pgstore.Fragment{"master": mAfter, "slave": sAfter} {
		if err := p.checkFragments(s); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, fs := range s {
			for _, f := range fs {
				if f.ReservationID != nil || f.Status != "UNLOCKED" {
					t.Fatalf("%s: leftover reserved fragment %s", name, fragString(f))
				}
			}
		}
		if got := freeCount(free(s)); got != total-granted {
			t.Fatalf("%s: %d tuples free, want %d (total %d - granted %d)", name, got, total-granted, total, granted)
		}
	}
	mFree, sFree := free(mAfter), free(sAfter)
	for _, id := range p.chunks {
		a, b := fmt.Sprint(mFree[id]), fmt.Sprint(sFree[id])
		if a != b {
			t.Fatalf("chunk %s: master free %s, slave free %s", id, a, b)
		}
		// exactly the not granted indexes are free
		var want []int
		for j := int64(0); j < p.sizes[indexOf(p.chunks, id)]; j++ {
			if _, taken := seen[[2]uint64{uint64(indexOf(p.chunks, id)), uint64(j)}]; !taken {
				want = append(want, int(j))
			}
		}
		var got []int
		for _, s := range mFree[id] {
			for j := s.start; j < s.end; j++ {
				got = append(got, int(j))
			}
		}
		sort.Ints(got)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("chunk %s: free indexes differ from the not granted ones", id)
		}
	}

	// consumption counters equal what was granted, and master and slave agree
	for name, s := range map[string]*Service{"master": p.master, "slave": p.slave} {
		n, err := s.Cache.ConsumptionSince(ctx, 0, bit.String())
		if err != nil || n != granted {
			t.Fatalf("%s consumption counter %d (%v), want %d", name, n, err, granted)
		}
		// no reservation left in the cache
		for i := range res {
			rid := uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprint("req", i))).String() + "_" + bit.String()
			if r, err := s.Cache.GetReservation(ctx, rid); err != nil || (r != nil && res[i].mErr == nil) {
				t.Fatalf("%s: reservation %s left in the cache (%v)", name, rid, err)
			}
		}
	}
	// a chunk without fragments was removed from the object store, any other chunk is still stored
	for name, s := range map[string]*Service{"master": p.master, "slave": p.slave} {
		snap, err := snapshot(ctx, s.PG)
		if err != nil {
			t.Fatal(err)
		}
		ms := s.Tuples.(*memStore)
		ms.mu.Lock()
		for _, id := range p.chunks {
			if _, stored := ms.objs[id]; stored != (len(snap[id]) > 0) {
				ms.mu.Unlock()
				t.Fatalf("%s: chunk %s stored=%v but has %d fragments", name, id, stored, len(snap[id]))
			}
		}
		ms.mu.Unlock()
	}
}

func indexOf(ids []string, id string) int {
	for i, v := range ids {
		if v == id {
			return i
		}
	}
	return -1
}

// A reservation that is never released: the slave gives up after its timeout, consumes nothing,
// and leaves the free tuples as they were; the master's rollback frees exactly what it had marked.
func TestStressTimeoutAndRollbackReleaseExactlyTheReservation(t *testing.T) {
	ctx := context.Background()
	p := newPair(t, []int64{200, 50}, 30, 150*time.Millisecond)
	bit := mustType("BIT_GFP")
	before, _ := snapshot(ctx, p.spg)

	start := time.Now()
	_, err := p.slave.GetTuples(ctx, bit, 5, uuid.NewString())
	if err == nil || !strings.HasPrefix(err.Error(), "No released tuple reservation") || time.Since(start) < 100*time.Millisecond {
		t.Fatalf("slave timeout: %v after %s", err, time.Since(start))
	}
	after, _ := snapshot(ctx, p.spg)
	if fmt.Sprint(free(before)) != fmt.Sprint(free(after)) {
		t.Fatal("a timed out slave request changed the free tuples")
	}

	// master: the slaves refuse -> the transaction rolls back, nothing stays marked or cached
	p.link.failShare = func(string) bool { return true }
	mBefore, _ := snapshot(ctx, p.mpg)
	id := uuid.NewString()
	if _, err := p.master.GetTuples(ctx, bit, 120, id); err == nil || err.Error() != MsgSharingFailed {
		t.Fatalf("want sharing failure, got %v", err)
	}
	mAfter, _ := snapshot(ctx, p.mpg)
	for c, fs := range mAfter {
		for _, f := range fs {
			if f.ReservationID != nil {
				t.Fatalf("chunk %s: fragment %s stayed reserved", c, fragString(f))
			}
		}
	}
	if fmt.Sprint(free(mBefore)) != fmt.Sprint(free(mAfter)) {
		t.Fatalf("rollback changed the free tuples: %v -> %v", free(mBefore), free(mAfter))
	}
	if r, err := p.master.Cache.GetReservation(ctx, id+"_"+bit.String()); r != nil || err != nil {
		t.Fatalf("reservation cached after failure: %v %v", r, err)
	}
	if n, _ := p.master.Cache.ConsumptionSince(ctx, 0, bit.String()); n != 0 {
		t.Fatalf("consumption %d after failed reservation", n)
	}
	// and the tuples are still obtainable afterwards
	p.link.failShare = nil
	data, err := p.master.GetTuples(ctx, bit, 250, uuid.NewString())
	if err != nil || len(data) != 250*bit.TupleSize() {
		t.Fatalf("all tuples must still be available: %v", err)
	}
	if _, err := p.master.GetTuples(ctx, bit, 1, uuid.NewString()); err == nil || !strings.HasPrefix(err.Error(), "Insufficient") {
		t.Fatalf("pool is empty, got %v", err)
	}
}
