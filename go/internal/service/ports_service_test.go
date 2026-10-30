package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/carbynestack/castor-go/internal/javaser"
	"github.com/carbynestack/castor-go/internal/model"
	"github.com/carbynestack/castor-go/internal/peer"
	"github.com/carbynestack/castor-go/internal/pgstore"
)

// Ports of the castor-service unit tests: DefaultTuplesDownloadServiceTest, TelemetryServiceTest,
// DefaultCastorWebSocketServiceTest, ReservationCachingServiceTest, CreateReservationSupplierTest,
// UnlockReservationSupplierTest and WaitForReservationCallableTest. The Mockito mocks are replaced by
// the real cache / database (or miniredis where only Redis is involved) and by spies for the
// object store and the peer client. The Supplier/Callable classes have no Go counterpart; their
// observable behaviour is asserted through createReservation and GetTuples.

const (
	rReqID    = "c8a0a467-16b0-4f03-b7d7-07cbe1b0e7e8"
	rChunk    = "80fbba1b-3da8-4b1e-8a2c-cebd65229fad"
	rUpChunk  = "3fd7eaf7-cda3-4384-8d86-2c43450cbe63"
	inputMask = "INPUT_MASK_GFP"
	mulTriple = "MULTIPLICATION_TRIPLE_GFP"
)

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func unlockedRes(id, typ, chunk string, n, start int64) *model.Reservation {
	return &model.Reservation{ReservationID: id, TupleType: typ, Status: model.Unlocked,
		Reservations: []model.ReservationElement{{TupleChunkID: chunk, ReservedTuples: n, StartIndex: start}}}
}

// --- DefaultTuplesDownloadServiceTest ---

func TestDownloadAsSlaveFailsWhenTuplesCannotBeRetrieved(t *testing.T) {
	s, _, spy, _ := cacheOnly(t, false)
	ctx := context.Background()
	typ := mustType(inputMask)
	const count = 42
	if err := s.Cache.PutReservation(ctx, unlockedRes(rReqID+"_"+typ.String(), inputMask, rChunk, count, 0)); err != nil {
		t.Fatal(err)
	}
	expected := errors.New("expected")
	spy.readErr = expected
	_, err := s.GetTuples(ctx, typ, count, rReqID)
	if err == nil || err.Error() != MsgRetrieveFailed || !errors.Is(err, expected) {
		t.Fatalf("%v", err)
	}
	// the tuples were requested for the reserved range: offset 0, count * tupleSize bytes
	if len(spy.reads) != 1 || spy.reads[0] != [3]any{rChunk, int64(0), int64(count * typ.TupleSize())} {
		t.Fatalf("%v", spy.reads)
	}
}

func TestDownloadAsMasterReturnsTuplesOfExistingReservationAndCleansUp(t *testing.T) {
	f := newFix(t, true)
	ctx := context.Background()
	typ := mustType(inputMask)
	const count = 2
	id := rReqID + "_" + typ.String()
	data := randBytes(typ.TupleSize() * count)
	_ = f.store.Save(ctx, rChunk, data)
	if err := f.svc.Cache.PutReservation(ctx, unlockedRes(id, inputMask, rChunk, count, 0)); err != nil {
		t.Fatal(err)
	}
	got, err := f.svc.GetTuples(ctx, typ, count, rReqID)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("%v", err)
	}
	if _, ok := f.store.objs[rChunk]; ok {
		t.Fatal("the unreferenced chunk must be deleted from the tuple store (deleteTupleChunk)")
	}
	if r, _ := f.svc.Cache.GetReservation(ctx, id); r != nil {
		t.Fatal("forgetReservation: reservation must be removed from the cache")
	}
	if len(f.peers.shared) != 0 || len(f.peers.updates) != 0 {
		t.Fatal("an existing unlocked reservation is used as is")
	}
}

// A failing delete of the chunk object is only logged (MinioTupleStore.deleteTupleChunk swallows it).
func TestDownloadSucceedsEvenIfDeletingTheChunkFails(t *testing.T) {
	f := newFix(t, true)
	ctx := context.Background()
	typ := mustType(inputMask)
	data := randBytes(typ.TupleSize() * 2)
	_ = f.store.Save(ctx, rChunk, data)
	spy := &spyStore{TupleStore: f.store, deleteErr: errors.New("expected")}
	f.svc.Tuples = spy
	_ = f.svc.Cache.PutReservation(ctx, unlockedRes(rReqID+"_"+typ.String(), inputMask, rChunk, 2, 0))
	if got, err := f.svc.GetTuples(ctx, typ, 2, rReqID); err != nil || !bytes.Equal(got, data) || len(spy.deletes) != 1 {
		t.Fatal(err, spy.deletes)
	}
}

// --- TelemetryServiceTest ---

func TestTelemetryDataForInterval(t *testing.T) {
	f := newFix(t, true)
	ctx := context.Background()
	interval := 42 * time.Second
	for i, typ := range model.TupleTypes {
		available, rate := int64(i+1), int64(2)<<i // 1/2, 2/4, ... 10/1024
		f.seed(t, frag(fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1), typ.Name(), 0, available, "UNLOCKED", nil))
		if err := f.svc.Cache.KeepConsumption(ctx, f.now.UnixMilli(), typ.String(), rate*int64(interval/time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	td, err := f.svc.Telemetry(ctx, interval)
	if err != nil || td.Interval != interval.Milliseconds() || len(td.Metrics) != len(model.TupleTypes) {
		t.Fatalf("%+v %v", td, err)
	}
	for i, typ := range model.TupleTypes {
		want := model.TupleMetric{Available: int64(i + 1), ConsumptionRate: int64(2) << i, Type: typ.Name()}
		if td.Metrics[i] != want {
			t.Errorf("metric %d: got %+v want %+v", i, td.Metrics[i], want)
		}
	}
}

// Math.round(consumption / (float) seconds): half rounds up, tuples older than the interval do not count.
func TestTelemetryConsumptionRateRounding(t *testing.T) {
	f := newFix(t, true)
	ctx := context.Background()
	for _, c := range []struct{ consumed, want int64 }{{0, 0}, {20, 0}, {21, 1}, {62, 1}, {63, 2}, {84, 2}} {
		f.mr.FlushAll()
		_ = f.svc.Cache.KeepConsumption(ctx, f.now.UnixMilli(), "bit_gfp", c.consumed)
		_ = f.svc.Cache.KeepConsumption(ctx, f.now.UnixMilli()-42_001, "bit_gfp", 1000) // outside the interval
		td, err := f.svc.Telemetry(ctx, 42*time.Second)
		if err != nil || td.Metrics[0].ConsumptionRate != c.want {
			t.Errorf("consumed %d: got %+v (%v), want rate %d", c.consumed, td.Metrics[0], err, c.want)
		}
	}
}

// --- DefaultCastorWebSocketServiceTest ---

func failureFrame(chunk, msg string) []byte { return javaser.EncodeUploadResponse(chunk, &msg, false) }

func upload(t *testing.T, s *Service, payload []byte) (frames [][]byte, err error) {
	t.Helper()
	err = s.HandleUpload(context.Background(), payload, func(b []byte) { frames = append(frames, b) })
	return
}

func TestUploadInvalidPayload(t *testing.T) {
	s, _, _, _ := cacheOnly(t, true)
	frames, err := upload(t, s, []byte{})
	if err == nil || err.Error() != MsgUnsupportedPayload {
		t.Fatal(err)
	}
	if len(frames) != 1 || !bytes.Equal(frames[0], failureFrame("", MsgUnsupportedPayload)) {
		t.Fatalf("expected one failure response without chunk id, got %x", frames)
	}
}

func TestUploadChunkWithoutTuples(t *testing.T) {
	s, _, _, _ := cacheOnly(t, true)
	frames, err := upload(t, s, javaser.EncodeTupleChunk(rUpChunk, mulTriple, []byte{}))
	if err == nil || err.Error() != MsgChunkEmpty {
		t.Fatal(err)
	}
	if len(frames) != 1 || !bytes.Equal(frames[0], failureFrame(rUpChunk, MsgChunkEmpty)) {
		t.Fatalf("%x", frames)
	}
}

func TestUploadFailsWhenWritingToTheTupleStoreFails(t *testing.T) {
	s, _, spy, _ := cacheOnly(t, true)
	expected := errors.New("expected")
	spy.saveErr = expected
	typ := mustType(inputMask)
	frames, err := upload(t, s, javaser.EncodeTupleChunk(rUpChunk, inputMask, randBytes(typ.TupleSize())))
	var se *Error
	if !errors.As(err, &se) || se.Msg != MsgFailedWriting || !errors.Is(err, expected) {
		t.Fatalf("%#v", err)
	}
	if len(frames) != 1 || !bytes.Equal(frames[0], failureFrame(rUpChunk, MsgFailedWriting)) {
		t.Fatalf("%x", frames)
	}
}

func TestUploadSuccess(t *testing.T) {
	f := newFix(t, true)
	f.svc.S.InitialFragmentSize = 7
	typ := mustType(inputMask)
	data := randBytes(typ.TupleSize())
	frames, err := upload(t, f.svc, javaser.EncodeTupleChunk(rUpChunk, inputMask, data))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f.store.objs[rUpChunk], data) {
		t.Fatal("tuple data must be saved under the chunk id")
	}
	f.wantFragments(t, frag(rUpChunk, inputMask, 0, 1, "LOCKED", nil))
	if len(frames) != 1 || !bytes.Equal(frames[0], javaser.EncodeUploadResponse(rUpChunk, nil, true)) {
		t.Fatalf("exactly one success response expected: %x", frames)
	}
}

// Like Java, trailing bytes that do not form a whole tuple are stored but not counted.
func TestUploadCountsOnlyWholeTuples(t *testing.T) {
	f := newFix(t, true)
	typ := mustType(inputMask)
	data := randBytes(3*typ.TupleSize() + 5)
	if _, err := upload(t, f.svc, javaser.EncodeTupleChunk(rUpChunk, inputMask, data)); err != nil {
		t.Fatal(err)
	}
	f.wantFragments(t, frag(rUpChunk, inputMask, 0, 3, "LOCKED", nil))
	if len(f.store.objs[rUpChunk]) != len(data) {
		t.Fatal("the whole payload is stored")
	}
}

func TestGenerateFragmentsForChunk(t *testing.T) {
	typ := mustType(inputMask)
	// givenChunkWithZeroTuples_whenGenerateFragments_thenReturnEmptyList
	if fr, err := GenerateFragments(rUpChunk, typ, 0, 7); err != nil || len(fr) != 0 {
		t.Fatal(fr, err)
	}
	// givenChunkWithMultipleTuples_whenGenerateFragments (size 7, 13 tuples): [0,7) and [7,13)
	fr, err := GenerateFragments(rUpChunk, typ, 7*2-1, 7)
	if err != nil || len(fr) != 2 {
		t.Fatal(fr, err)
	}
	for i, want := range []*pgstore.Fragment{frag(rUpChunk, inputMask, 0, 7, "LOCKED", nil), frag(rUpChunk, inputMask, 7, 13, "LOCKED", nil)} {
		if !fragEq(*fr[i], *want) {
			t.Errorf("fragment %d: %s want %s", i, fragString(*fr[i]), fragString(*want))
		}
	}
	// exact multiples and a size larger than the chunk
	if fr, _ := GenerateFragments(rUpChunk, typ, 14, 7); len(fr) != 2 || fr[1].End != 14 {
		t.Fatal(fr)
	}
	if fr, _ := GenerateFragments(rUpChunk, typ, 3, 7); len(fr) != 1 || fr[0].End != 3 {
		t.Fatal(fr)
	}
}

// --- ReservationCachingServiceTest: cache-only behaviour ---

func TestKeepReservationStoresAndEmitsConsumption(t *testing.T) {
	s, mr, _, _ := cacheOnly(t, true)
	ctx := context.Background()
	r := unlockedRes("reservationId", mulTriple, rChunk, 42, 0)
	r.Status = model.Locked
	if err := s.keepReservation(ctx, r); err != nil {
		t.Fatal(err)
	}
	got, err := s.Cache.GetReservation(ctx, "reservationId")
	if err != nil || got == nil || got.Status != model.Locked || got.Total() != 42 {
		t.Fatalf("%+v %v", got, err)
	}
	if !mr.Exists(`"castor-reservation-store"::reservationId`) {
		t.Fatalf("keys %v", mr.Keys())
	}
	if n, _ := s.Cache.ConsumptionSince(ctx, 0, "multiplicationtriple_gfp"); n != 42 {
		t.Fatal("consumption of 42 tuples must be emitted, got", n)
	}
}

func TestKeepReservationConflict(t *testing.T) {
	s, mr, _, _ := cacheOnly(t, true)
	ctx := context.Background()
	existing := unlockedRes("reservationId", mulTriple, rChunk, 5, 0)
	_ = s.Cache.PutReservation(ctx, existing)
	err := s.keepReservation(ctx, unlockedRes("reservationId", mulTriple, rChunk, 42, 0))
	if err == nil || err.Error() != "Reservation conflict. Reservation with ID #reservationId already exists." {
		t.Fatal(err)
	}
	if got, _ := s.Cache.GetReservation(ctx, "reservationId"); got.Total() != 5 {
		t.Fatal("the existing reservation must stay untouched")
	}
	if len(mr.Keys()) != 1 {
		t.Fatalf("no consumption must be emitted: %v", mr.Keys())
	}
}

func TestUpdateReservation(t *testing.T) {
	s, _, _, _ := cacheOnly(t, true)
	ctx := context.Background()
	if err := s.UpdateReservation(ctx, "reservationId", model.Unlocked); err == nil || err.Error() != "No reservation was found for requestId reservationId." {
		t.Fatal(err)
	}
	r := unlockedRes("reservationId", mulTriple, rChunk, 42, 0)
	r.Status = model.Locked
	_ = s.Cache.PutReservation(ctx, r)
	if err := s.UpdateReservation(ctx, "reservationId", model.Unlocked); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Cache.GetReservation(ctx, "reservationId"); got.Status != model.Unlocked || got.Total() != 42 {
		t.Fatalf("%+v", got)
	}
}

func TestGetUnlockedReservation(t *testing.T) {
	s, _, _, _ := cacheOnly(t, true)
	ctx := context.Background()
	typ := mustType(inputMask)
	const count = 42
	// no reservation: nil
	if r, err := s.getUnlockedReservation(ctx, "testReservationId", typ, count); r != nil || err != nil {
		t.Fatal(r, err)
	}
	stored := unlockedRes("testReservationId", inputMask, rChunk, count, 0)
	_ = s.Cache.PutReservation(ctx, stored)
	if r, err := s.getUnlockedReservation(ctx, "testReservationId", typ, count); err != nil || r == nil || r.String() != stored.String() {
		t.Fatalf("%v %v", r, err)
	}
	// LOCKED reservations are invisible, even if they do not match
	stored.Status = model.Locked
	_ = s.Cache.PutReservation(ctx, stored)
	if r, err := s.getUnlockedReservation(ctx, "testReservationId", typ, count+1); r != nil || err != nil {
		t.Fatal(r, err)
	}
	// mismatching type / count
	for name, mismatch := range map[string]*model.Reservation{
		"tuple type": unlockedRes("testReservationId", mulTriple, rChunk, count, 0),
		"count":      unlockedRes("testReservationId", inputMask, rChunk, count-1, 0),
	} {
		_ = s.Cache.PutReservation(ctx, mismatch)
		_, err := s.getUnlockedReservation(ctx, "testReservationId", typ, count)
		want := fmt.Sprintf(`Reservation does not match expected specification (id: "testReservationId", tupleType: "inputmask_gfp", count: 42): %s.`, mismatch)
		if err == nil || err.Error() != want {
			t.Errorf("%s: got %v\nwant %s", name, err, want)
		}
	}
}

func TestReservationToStringMatchesLombok(t *testing.T) {
	r := unlockedRes("rid", mulTriple, rChunk, 42, 3)
	r.Reservations = append(r.Reservations, model.ReservationElement{TupleChunkID: rUpChunk, ReservedTuples: 1, StartIndex: 0})
	want := "Reservation(reservationId=rid, tupleType=multiplicationtriple_gfp, reservations=[" +
		"ReservationElement(tupleChunkId=" + rChunk + ", reservedTuples=42, startIndex=3), " +
		"ReservationElement(tupleChunkId=" + rUpChunk + ", reservedTuples=1, startIndex=0)], status=UNLOCKED)"
	if r.String() != want {
		t.Fatalf("%s\n%s", r, want)
	}
}

func TestForgetReservation(t *testing.T) {
	s, mr, _, _ := cacheOnly(t, true)
	ctx := context.Background()
	_ = s.Cache.PutReservation(ctx, unlockedRes("reservationId", mulTriple, rChunk, 1, 0))
	if err := s.Cache.DeleteReservation(ctx, "reservationId"); err != nil || len(mr.Keys()) != 0 {
		t.Fatal(err, mr.Keys())
	}
	// forgetting an unknown reservation is fine
	if err := s.Cache.DeleteReservation(ctx, "reservationId"); err != nil {
		t.Fatal(err)
	}
}

// --- ReservationCachingServiceTest.getReservationWithRetry / WaitForReservationCallableTest ---

func TestSlaveTimesOutWithoutReleasedReservationAndStopsPolling(t *testing.T) {
	s, mr, _, _ := cacheOnly(t, false)
	ctx := context.Background()
	typ := mustType(mulTriple)
	s.S.ReservationTimeout = 50 * time.Millisecond
	start := time.Now()
	_, err := s.GetTuples(ctx, typ, 42, rReqID)
	if err == nil || err.Error() != fmt.Sprintf("No released tuple reservation was found for the given request ID #%s_multiplicationtriple_gfp.", rReqID) {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 50*time.Millisecond || d > 2*time.Second {
		t.Fatalf("returned after %s", d)
	}
	// givenNoReservationRetrievedAndCancelled: the polling loop is cancelled when the wait is over
	n := mr.CommandCount()
	time.Sleep(60 * time.Millisecond)
	if mr.CommandCount() != n {
		t.Fatal("polling must stop after the timeout")
	}
}

func TestSlaveWaitsUntilReservationIsUnlocked(t *testing.T) {
	s, _, _, _ := cacheOnly(t, false)
	ctx := context.Background()
	typ := mustType(mulTriple)
	id := rReqID + "_" + typ.String()
	data := randBytes(typ.TupleSize() * 42)
	_ = s.Tuples.Save(ctx, rChunk, data)
	r := unlockedRes(id, mulTriple, rChunk, 42, 0)
	r.Status = model.Locked // givenReservationLockedInitially
	_ = s.Cache.PutReservation(ctx, r)
	s.S.ReservationTimeout = 5 * time.Second
	go func() {
		time.Sleep(40 * time.Millisecond)
		_ = s.UpdateReservation(ctx, id, model.Unlocked)
	}()
	// the delete bookkeeping needs a database; only the retrieval is of interest here
	res, err := s.getReservationWithRetry(ctx, id, typ, 42)
	if err != nil || res == nil || res.Status != model.Unlocked || res.Total() != 42 {
		t.Fatalf("%+v %v", res, err)
	}
}

// A mismatching reservation makes the polling fail; like Java the caller only sees the generic message.
func TestSlaveReportsGenericMessageWhenPolledReservationDoesNotMatch(t *testing.T) {
	s, _, _, _ := cacheOnly(t, false)
	ctx := context.Background()
	typ := mustType(mulTriple)
	_ = s.Cache.PutReservation(ctx, unlockedRes(rReqID+"_"+typ.String(), mulTriple, rChunk, 41, 0))
	_, err := s.getReservationWithRetry(ctx, rReqID+"_"+typ.String(), typ, 42)
	if err == nil || err.Error() != fmt.Sprintf(MsgNoReleased, rReqID+"_"+typ.String()) {
		t.Fatal(err)
	}
}

// --- ReservationCachingServiceTest.createReservation* (masters) ---

func TestCreateReservationRequiresMaster(t *testing.T) {
	s, _, _, _ := cacheOnly(t, true)
	s.Peers = nil // neither dedicated transaction service nor inter-VCP client
	if _, err := s.createReservation(context.Background(), "testReservationId", mustType(inputMask), 42); err == nil || err.Error() != "Operating as master even though service is not declared to be the master." {
		t.Fatal(err)
	}
}

// givenInsufficientTuples_whenGet_thenThrowCastorServiceException
func TestCreateReservationInsufficientTuples(t *testing.T) {
	f := newFix(t, true)
	f.seed(t, frag(rChunk, inputMask, 0, 41, "UNLOCKED", nil))
	_, err := f.svc.createReservation(context.Background(), rReqID+"_inputmask_gfp", mustType(inputMask), 42)
	if err == nil || err.Error() != "Insufficient Tuples of type inputmask_gfp available (41 out of 42)." {
		t.Fatal(err)
	}
	if len(f.peers.shared) != 0 {
		t.Fatal("nothing must be shared")
	}
}

// givenProvidedChunksDoNotHaveEnoughTuplesAvailable_whenGet_thenThrowCastorServiceException
func TestComposeElementsWithoutLocatableFragment(t *testing.T) {
	f := newFix(t, true)
	ctx := context.Background()
	f.seed(t, frag(rChunk, inputMask, 0, 42, "UNLOCKED", nil))
	err := f.pg.InTx(ctx, func(q pgstore.Querier) error {
		_, err := f.svc.composeElements(ctx, hideFragments{q}, mustType(inputMask), 42, "rid")
		return err
	})
	var se *Error
	if !errors.As(err, &se) || se.Msg != "Failed to reserve the tuples." || se.Cause == nil || se.Cause.Error() != "Unable to locate available tuples." {
		t.Fatalf("%#v", err)
	}
}

// givenSharingReservationFails_whenGet_thenThrowCastorServiceException: the fragment is reserved
// before sharing and handed back by the rollback.
func TestCreateReservationSharingFails(t *testing.T) {
	f := newFix(t, true)
	typ := mustType(inputMask)
	f.seed(t, frag(rChunk, inputMask, 0, 42, "UNLOCKED", nil))
	f.peers.shareOK = false
	id := rReqID + "_" + typ.String()
	_, err := f.svc.createReservation(context.Background(), id, typ, 42)
	if err == nil || err.Error() != "Sharing reservation with slave services failed." {
		t.Fatal(err)
	}
	if len(f.peers.shared) != 1 || f.peers.shared[0].ReservationID != id || f.peers.shared[0].Reservations[0].ReservedTuples != 42 {
		t.Fatalf("%+v", f.peers.shared)
	}
	f.wantFragments(t, frag(rChunk, inputMask, 0, 42, "UNLOCKED", nil))
	if r, _ := f.svc.Cache.GetReservation(context.Background(), id); r != nil {
		t.Fatal("no reservation must be stored")
	}
}

// givenSuccessfulRequest_whenGet_thenReturnExpectedReservation +
// UnlockReservationSupplierTest + givenNoCachedReservationAndSuccessfulRequest_whenGetOrCreateReservation
func TestCreateReservationSuccess(t *testing.T) {
	f := newFix(t, true)
	ctx := context.Background()
	typ := mustType(inputMask)
	f.seed(t, frag(rChunk, inputMask, 0, 43, "UNLOCKED", nil))
	id := rReqID + "_" + typ.String()
	got, err := f.svc.createReservation(ctx, id, typ, 42)
	if err != nil {
		t.Fatal(err)
	}
	want := unlockedRes(id, inputMask, rChunk, 42, 0)
	if got.String() != want.String() {
		t.Fatalf("%s\n%s", got, want)
	}
	// shared as LOCKED reservation (the expected reservation of the supplier test), then unlocked on all slaves ...
	if len(f.peers.shared) != 1 || f.peers.shared[0].Status != model.Locked || f.peers.shared[0].String() != strings.Replace(want.String(), "UNLOCKED", "LOCKED", 1) {
		t.Fatalf("%+v", f.peers.shared)
	}
	if len(f.peers.updates) != 1 || f.peers.updates[0] != id+":UNLOCKED" {
		t.Fatal(f.peers.updates)
	}
	// ... and locally; the fragment was split: 42 reserved, one left
	if stored, _ := f.svc.Cache.GetReservation(ctx, id); stored == nil || stored.Status != model.Unlocked {
		t.Fatalf("%+v", stored)
	}
	f.wantFragments(t, frag(rChunk, inputMask, 0, 42, "UNLOCKED", rid(id)), frag(rChunk, inputMask, 42, 43, "UNLOCKED", nil))
	if n, _ := f.svc.Cache.ConsumptionSince(ctx, 0, "inputmask_gfp"); n != 42 {
		t.Fatal("consumption", n)
	}
}

// givenNoCachedReservationButCreationThrows / ...ActivationThrows: communication failures with the
// slaves surface as "Creating the reservation failed." with the failure as cause.
func TestCreateReservationPeerCommunicationFails(t *testing.T) {
	typ := mustType(inputMask)
	id := rReqID + "_" + typ.String()

	// sharing throws
	f := newFix(t, true)
	f.seed(t, frag(rChunk, inputMask, 0, 42, "UNLOCKED", nil))
	cause := &peer.ClientError{Msg: peer.FailedSharing, Cause: errors.New("expected")}
	f.peers.shareErr = cause
	_, err := f.svc.createReservation(context.Background(), id, typ, 42)
	var se *Error
	if !errors.As(err, &se) || se.Msg != "Creating the reservation failed." || !errors.Is(err, cause) {
		t.Fatalf("%#v", err)
	}
	f.wantFragments(t, frag(rChunk, inputMask, 0, 42, "UNLOCKED", nil))

	// activation throws: sharing worked, so the reservation stays reserved and LOCKED
	f = newFix(t, true)
	f.seed(t, frag(rChunk, inputMask, 0, 42, "UNLOCKED", nil))
	cause = &peer.ClientError{Msg: peer.FailedUpdating, Cause: errors.New("expected")}
	f.peers.updErr = cause
	_, err = f.svc.createReservation(context.Background(), id, typ, 42)
	if !errors.As(err, &se) || se.Msg != "Creating the reservation failed." || !errors.Is(err, cause) {
		t.Fatalf("%#v", err)
	}
	if stored, _ := f.svc.Cache.GetReservation(context.Background(), id); stored == nil || stored.Status != model.Locked {
		t.Fatalf("%+v", stored)
	}
	f.wantFragments(t, frag(rChunk, inputMask, 0, 42, "UNLOCKED", rid(id)))
}

// --- ReservationCachingServiceTest.keepAndApplyReservation* (slaves) ---

func TestApplyReservationConflictLeavesEverythingUntouched(t *testing.T) {
	f := newFix(t, false)
	ctx := context.Background()
	f.seed(t, frag(rUpChunk, mulTriple, 0, 99, "UNLOCKED", nil))
	existing := unlockedRes("testReservation", mulTriple, rUpChunk, 3, 0)
	_ = f.svc.Cache.PutReservation(ctx, existing)
	r := unlockedRes("testReservation", mulTriple, rUpChunk, 21, 42)
	if err := f.svc.ApplyReservation(ctx, r); err == nil || err.Error() != "Reservation conflict. Reservation with ID #testReservation already exists." {
		t.Fatal(err)
	}
	f.wantFragments(t, frag(rUpChunk, mulTriple, 0, 99, "UNLOCKED", nil))
	if n, _ := f.svc.Cache.ConsumptionSince(ctx, 0, "multiplicationtriple_gfp"); n != 0 {
		t.Fatal(n)
	}
}

func TestApplyReservationWithoutMatchingFragment(t *testing.T) {
	f := newFix(t, false)
	ctx := context.Background()
	r := &model.Reservation{ReservationID: "testReservation", TupleType: mulTriple, Status: model.Locked,
		Reservations: []model.ReservationElement{{TupleChunkID: rUpChunk, ReservedTuples: 21, StartIndex: 42}}}
	err := f.svc.ApplyReservation(ctx, r)
	if err == nil || err.Error() != fmt.Sprintf("No fragment found to fulfill given reservation: %s.", r) {
		t.Fatal(err)
	}
	// LOCKED (not yet activated) fragments cannot satisfy a reservation either
	f.seed(t, frag(rUpChunk, mulTriple, 0, 99, "LOCKED", nil))
	f.mr.FlushAll()
	if err := f.svc.ApplyReservation(ctx, r); err == nil || !strings.HasPrefix(err.Error(), "No fragment found to fulfill given reservation: ") {
		t.Fatal(err)
	}
}

// givenReferencedSequenceLiesWithin_whenKeepAndApplyReservation_thenSplitFragmentAccordingly
func TestApplyReservationSplitsFragmentAroundTheReservedRange(t *testing.T) {
	f := newFix(t, false)
	ctx := context.Background()
	f.seed(t, frag(rUpChunk, mulTriple, 0, 99, "UNLOCKED", nil))
	r := &model.Reservation{ReservationID: "testReservation", TupleType: mulTriple, Status: model.Locked,
		Reservations: []model.ReservationElement{{TupleChunkID: rUpChunk, ReservedTuples: 21, StartIndex: 42}}}
	if err := f.svc.ApplyReservation(ctx, r); err != nil {
		t.Fatal(err)
	}
	f.wantContains(t,
		frag(rUpChunk, mulTriple, 0, 42, "UNLOCKED", nil),
		frag(rUpChunk, mulTriple, 42, 63, "UNLOCKED", rid("testReservation")),
		frag(rUpChunk, mulTriple, 63, 99, "UNLOCKED", nil))
	if got := f.fragments(t); len(got) != 3 {
		t.Fatal(describe(got))
	}
	if n, _ := f.svc.Cache.ConsumptionSince(ctx, 0, "multiplicationtriple_gfp"); n != 21 {
		t.Fatalf("consumption of the reserved tuples must be emitted, got %d", n)
	}
	// the reservation is stored LOCKED until the master releases it
	if stored, _ := f.svc.Cache.GetReservation(ctx, "testReservation"); stored == nil || stored.Status != model.Locked {
		t.Fatalf("%+v", stored)
	}
}
