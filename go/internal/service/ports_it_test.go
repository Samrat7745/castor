package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/Samrat7745/castor/go/internal/model"
	"github.com/Samrat7745/castor/go/internal/objstore"
)

// Ports of the Spring Boot integration tests that exercise the service against a real Postgres,
// Redis and Minio: ReservationCachingServiceIT, ConcurrencyIT and DefaultTuplesDownloadServiceAsMasterIT
// (the Spring context itself is the Go Service built by newRealFix).

// --- ReservationCachingServiceIT ---

const (
	itRequestID = "c8a0a467-16b0-4f03-b7d7-07cbe1b0e7e8"
	itChunk     = "80fbba1b-3da8-4b1e-8a2c-cebd65229fad"
	itResID     = itRequestID + "_multiplicationtriple_gfp"
)

func TestITGetUnlockedReservationFromEmptyCacheReturnsNil(t *testing.T) {
	f := newRealFix(t, true)
	if r, err := f.svc.getUnlockedReservation(context.Background(), itResID, mustType(mulTriple), 3); r != nil || err != nil {
		t.Fatal(r, err)
	}
}

func TestITReservationInCacheStaysUntouchedByGet(t *testing.T) {
	f := newRealFix(t, true)
	ctx := context.Background()
	stored := unlockedRes(itResID, mulTriple, itChunk, 3, 0)
	if err := f.svc.Cache.PutReservation(ctx, stored); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		r, err := f.svc.getUnlockedReservation(ctx, itResID, mustType(mulTriple), 3)
		if err != nil || r == nil || r.String() != stored.String() {
			t.Fatalf("read %d: %v %v", i, r, err)
		}
	}
	// the Redis key follows the Spring CacheKeyPrefix.simple() layout: "<store>::<id>"
	if n, _ := f.rdb.Exists(ctx, "castor-reservation-store::"+itResID).Result(); n != 1 {
		t.Fatal("key missing")
	}
}

// givenSharingReservationFails_whenCreateReservation_thenRollbackFragmentation: the very exception
// the client threw is propagated and the fragmentation is rolled back.
func TestITSharingFailureRollsBackFragmentation(t *testing.T) {
	f := newRealFix(t, true)
	typ := mustType(mulTriple)
	f.seed(t, frag(itChunk, mulTriple, 0, 24, "UNLOCKED", nil))
	expected := errors.New("sharing Reservation failed")
	f.peers.shareErr = expected
	_, err := f.svc.createReservation(context.Background(), itResID, typ, 12)
	if err != expected {
		t.Fatalf("got %#v", err)
	}
	if len(f.peers.shared) != 1 || f.peers.shared[0].Reservations[0].ReservedTuples != 12 || f.peers.shared[0].Reservations[0].StartIndex != 0 {
		t.Fatalf("%+v", f.peers.shared)
	}
	f.wantFragments(t, frag(itChunk, mulTriple, 0, 24, "UNLOCKED", nil))
}

func TestITForgetReservationRemovesItFromCache(t *testing.T) {
	f := newRealFix(t, true)
	ctx := context.Background()
	_ = f.svc.Cache.PutReservation(ctx, unlockedRes(itResID, mulTriple, itChunk, 3, 0))
	if r, _ := f.svc.Cache.GetReservation(ctx, itResID); r == nil {
		t.Fatal("stored")
	}
	if err := f.svc.Cache.DeleteReservation(ctx, itResID); err != nil {
		t.Fatal(err)
	}
	if r, _ := f.svc.Cache.GetReservation(ctx, itResID); r != nil {
		t.Fatal("must be gone")
	}
}

// whenReferencedSequenceIsSplitInFragments_whenApplyReservation_thenApplyAccordingly
func TestITApplyReservationAcrossSeveralFragments(t *testing.T) {
	f := newRealFix(t, false)
	ctx := context.Background()
	const start, length = 42, 21
	f.seed(t,
		frag(rUpChunk, mulTriple, 0, start, "UNLOCKED", nil),
		frag(rUpChunk, mulTriple, start, start+length-5, "UNLOCKED", nil),
		frag(rUpChunk, mulTriple, start+length-5, math.MaxInt64, "UNLOCKED", nil))
	r := &model.Reservation{ReservationID: "testReservation", TupleType: mulTriple, Status: model.Locked,
		Reservations: []model.ReservationElement{{TupleChunkID: rUpChunk, ReservedTuples: length, StartIndex: start}}}
	if err := f.svc.ApplyReservation(ctx, r); err != nil {
		t.Fatal(err)
	}
	// the fragment before is untouched, the fragment of interest reserved, the one containing the
	// rest split with its first part reserved and the remnant still available
	f.wantContains(t,
		frag(rUpChunk, mulTriple, 0, start, "UNLOCKED", nil),
		frag(rUpChunk, mulTriple, start, start+length-5, "UNLOCKED", rid("testReservation")),
		frag(rUpChunk, mulTriple, start+length-5, start+length, "UNLOCKED", rid("testReservation")),
		frag(rUpChunk, mulTriple, start+length, math.MaxInt64, "UNLOCKED", nil))
	if got := f.fragments(t); len(got) != 4 {
		t.Fatal(describe(got))
	}
	keys, _ := f.rdb.Keys(ctx, "castor-consumption-multiplicationtriple_gfp::*").Result()
	if len(keys) != 1 {
		t.Fatalf("exactly one consumption entry expected: %v", keys)
	}
	if n, err := f.svc.Cache.ConsumptionSince(ctx, math.MinInt64, "multiplicationtriple_gfp"); err != nil || n != length {
		t.Fatal(n, err)
	}
}

// --- ConcurrencyIT ---

// givenSubsequentReservationIsProcessedFirst_whenMasterSharesReservationOnParallelRequests_thenSucceedAsExpected
func TestITSubsequentReservationProcessedFirstSucceeds(t *testing.T) {
	f := newRealFix(t, true)
	ctx := context.Background()
	const chunk = "d1ea9d52-2b1d-42f0-85eb-e36caa6a623c"
	f.seed(t, frag(chunk, mulTriple, 0, 100, "UNLOCKED", nil))
	first := &model.Reservation{ReservationID: "1636913e-87b7-4331-97d7-4b14a1552604", TupleType: mulTriple, Status: model.Locked,
		Reservations: []model.ReservationElement{{TupleChunkID: chunk, ReservedTuples: 3, StartIndex: 0}}}
	second := &model.Reservation{ReservationID: "9b77009e-313e-4aa9-b0d0-da0235329139", TupleType: mulTriple, Status: model.Locked,
		Reservations: []model.ReservationElement{{TupleChunkID: chunk, ReservedTuples: 3, StartIndex: 3}}}
	if err := f.svc.keepReservation(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.keepReservation(ctx, first); err != nil {
		t.Fatal(err)
	}
	// ... and the same when both are processed in parallel
	f.rdb.FlushAll(ctx)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, r := range []*model.Reservation{second, first} {
		wg.Add(1)
		go func() { defer wg.Done(); errs[i] = f.svc.keepReservation(ctx, r) }()
	}
	wg.Wait()
	if errs[0] != nil || errs[1] != nil {
		t.Fatal(errs)
	}
	if n, _ := f.svc.Cache.ConsumptionSince(ctx, 0, "multiplicationtriple_gfp"); n != 6 {
		t.Fatal("both consumptions must be counted:", n)
	}
}

// --- DefaultTuplesDownloadServiceAsMasterIT ---

const (
	asReqID = "a345f933-bf70-4c7a-b6cd-312b55a6ff9c"
	asChunk = "80fbba1b-3da8-4b1e-8a2c-cebd65229fad"
	asResID = asReqID + "_multiplicationtriple_gfp"
)

func (f *fix) consumed(t *testing.T, typ string) int64 {
	t.Helper()
	n, err := f.svc.Cache.ConsumptionSince(context.Background(), 0, typ)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestITDownloadAsMasterRollsBackWhenSharingFails(t *testing.T) {
	f := newRealFix(t, true)
	ctx := context.Background()
	typ := mustType(mulTriple)
	f.seed(t, frag(asChunk, mulTriple, 0, 24, "UNLOCKED", nil))
	f.peers.shareOK = false
	_, err := f.svc.GetTuples(ctx, typ, 12, asReqID)
	if err == nil || err.Error() != "Sharing reservation with slave services failed." {
		t.Fatal(err)
	}
	f.wantFragments(t, frag(asChunk, mulTriple, 0, 24, "UNLOCKED", nil))
	if n := f.consumed(t, typ.String()); n != 0 {
		t.Fatal("no consumption may be recorded:", n)
	}
	if r, _ := f.svc.Cache.GetReservation(ctx, asResID); r != nil {
		t.Fatal("no reservation may be stored")
	}
}

func TestITDownloadAsMasterKeepsReservationWhenRetrievalFails(t *testing.T) {
	f := newRealFix(t, true)
	ctx := context.Background()
	typ := mustType(mulTriple)
	const count, start = 42, 2
	f.seed(t, frag(asChunk, mulTriple, start, start+2*count, "UNLOCKED", nil))
	// the chunk was never uploaded to the tuple store
	_, err := f.svc.GetTuples(ctx, typ, count, asReqID)
	var se *Error
	if !errors.As(err, &se) || se.Msg != "Failed retrieving tuples from database." {
		t.Fatalf("%#v", err)
	}
	var oe *objstore.Error
	if !errors.As(err, &oe) || oe.Msg != "Error while reading tuples from database." {
		t.Fatalf("cause: %#v", err)
	}
	f.wantContains(t,
		frag(asChunk, mulTriple, start+count, start+2*count, "UNLOCKED", nil),
		frag(asChunk, mulTriple, start, start+count, "UNLOCKED", rid(asResID)))
	stored, _ := f.svc.Cache.GetReservation(ctx, asResID)
	want := unlockedRes(asResID, mulTriple, asChunk, count, start)
	if stored == nil || stored.String() != want.String() {
		t.Fatalf("%v", stored)
	}
	if n := f.consumed(t, typ.String()); n != count {
		t.Fatal("the consumption marker remains untouched:", n)
	}
	if len(f.spy.deletes) != 0 {
		t.Fatal("no tuple chunk may be deleted:", f.spy.deletes)
	}
}

func TestITDownloadAsMasterReturnsTuplesAndCleansUp(t *testing.T) {
	f := newRealFix(t, true)
	ctx := context.Background()
	typ := mustType(mulTriple)
	const count, start = 42, 2
	data := randBytes(typ.TupleSize() * (start + count))
	if err := f.svc.Tuples.Save(ctx, asChunk, data); err != nil {
		t.Fatal(err)
	}
	f.seed(t, frag(asChunk, mulTriple, start, start+count, "UNLOCKED", nil))
	got, err := f.svc.GetTuples(ctx, typ, count, asReqID)
	if err != nil || !bytes.Equal(got, data[start*typ.TupleSize():(start+count)*typ.TupleSize()]) {
		t.Fatalf("%v", err)
	}
	if fr := f.fragments(t); len(fr) != 0 {
		t.Fatal("the fragment was reserved, consumed and deleted:", describe(fr))
	}
	if r, _ := f.svc.Cache.GetReservation(ctx, asResID); r != nil {
		t.Fatal("the reservation must be deleted after consumption")
	}
	if n := f.consumed(t, typ.String()); n != count {
		t.Fatal(n)
	}
	if objs := f.bucketObjects(t); len(objs) != 0 {
		t.Fatal("the chunk must be removed once all its fragments are consumed:", objs)
	}
	if len(f.peers.shared) != 1 || fmt.Sprint(f.peers.shared[0].Status) != "LOCKED" {
		t.Fatalf("%+v", f.peers.shared)
	}
}
