package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/carbynestack/castor-go/internal/javaser"
	"github.com/carbynestack/castor-go/internal/model"
	"github.com/carbynestack/castor-go/internal/peer"
	"github.com/carbynestack/castor-go/internal/pgstore"
	"github.com/carbynestack/castor-go/internal/rediscache"
	"github.com/redis/go-redis/v9"
)

type memStore struct {
	mu   sync.Mutex
	objs map[string][]byte
}

func (m *memStore) Save(_ context.Context, id string, d []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[id] = append([]byte(nil), d...)
	return nil
}
func (m *memStore) Read(_ context.Context, id string, off, n int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[id]
	if !ok || off+n > int64(len(o)) {
		return nil, errors.New("no such object")
	}
	return o[off : off+n], nil
}
func (m *memStore) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, id)
	return nil
}

type fakePeers struct {
	shareOK  bool
	shareErr error
	updErr   error
	shared   []*model.Reservation
	updates  []string
}

func (f *fakePeers) ShareReservation(_ context.Context, r *model.Reservation) (bool, error) {
	c := *r
	f.shared = append(f.shared, &c)
	return f.shareOK, f.shareErr
}
func (f *fakePeers) UpdateReservationStatus(_ context.Context, id string, s model.ActivationStatus) error {
	f.updates = append(f.updates, id+":"+string(s))
	return f.updErr
}

const chunkA = "123e4567-e89b-12d3-a456-426614174000"

type fix struct {
	svc   *Service
	store *memStore
	peers *fakePeers
	pg    *pgstore.Store
	mr    *miniredis.Miniredis
	now   time.Time
}

func newFix(t *testing.T, master bool) *fix {
	dsn := os.Getenv("CASTOR_TEST_PG")
	if dsn == "" {
		t.Skip("CASTOR_TEST_PG not set")
	}
	ctx := context.Background()
	pg, err := pgstore.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Close)
	if _, err := pg.Pool.Exec(ctx, "DELETE FROM tuple_chunk_fragment"); err != nil {
		t.Fatal(err)
	}
	mr := miniredis.RunT(t)
	cache := rediscache.New(redis.NewClient(&redis.Options{Addr: mr.Addr()}), `"castor-reservation-store"`, `"castor-consumption-"`, 1000)
	f := &fix{store: &memStore{objs: map[string][]byte{}}, peers: &fakePeers{shareOK: true}, pg: pg, mr: mr, now: time.UnixMilli(1_700_000_000_000)}
	f.svc = &Service{S: Settings{IsMaster: master, InitialFragmentSize: 1000, ReservationTimeout: 300 * time.Millisecond, RetryDelay: 10 * time.Millisecond},
		PG: pg, Cache: cache, Tuples: f.store, Now: func() time.Time { return f.now }}
	if master {
		f.svc.Peers = f.peers
	}
	return f
}

// upload stores n tuples of type tt with bytes derived from the index, and activates the chunk.
func (f *fix) upload(t *testing.T, chunk string, tt model.TupleType, n int) []byte {
	data := make([]byte, n*tt.TupleSize())
	for i := range data {
		data[i] = byte(i * 31)
	}
	var out [][]byte
	err := f.svc.HandleUpload(context.Background(), javaser.EncodeTupleChunk(chunk, tt.Name(), data), func(b []byte) { out = append(out, b) })
	if err != nil || len(out) != 1 {
		t.Fatal(err, len(out))
	}
	if r, _ := javaser.DecodeUploadResponse(out[0]); !r.Success || r.ChunkID != chunk {
		t.Fatalf("%+v", r)
	}
	if err := f.svc.ActivateChunk(context.Background(), chunk); err != nil {
		t.Fatal(err)
	}
	return data
}

func tt(name string) model.TupleType { t, _ := model.ParseTupleType(name); return t }

func TestUploadFragmentsAndErrors(t *testing.T) {
	f := newFix(t, true)
	ctx := context.Background()
	bit := tt("BIT_GFP")
	data := make([]byte, 2500*bit.TupleSize())
	var out [][]byte
	send := func(b []byte) { out = append(out, b) }
	if err := f.svc.HandleUpload(ctx, javaser.EncodeTupleChunk(chunkA, bit.Name(), data), send); err != nil {
		t.Fatal(err)
	}
	rows, _ := f.pg.Pool.Query(ctx, "SELECT start_index,end_index,activation_status FROM tuple_chunk_fragment ORDER BY start_index")
	var got []string
	for rows.Next() {
		var a, b int64
		var s string
		_ = rows.Scan(&a, &b, &s)
		got = append(got, fmt.Sprintf("%d-%d-%s", a, b, s))
	}
	if strings.Join(got, ",") != "0-1000-LOCKED,1000-2000-LOCKED,2000-2500-LOCKED" {
		t.Fatal(got)
	}
	// duplicate -> failure frame with chunk id
	out = nil
	if err := f.svc.HandleUpload(ctx, javaser.EncodeTupleChunk(chunkA, bit.Name(), data), send); err == nil {
		t.Fatal("expected error")
	}
	r, _ := javaser.DecodeUploadResponse(out[0])
	if r.Success || r.ChunkID != chunkA || *r.ErrorMsg != MsgFailedWriting {
		t.Fatalf("%+v %s", r, *r.ErrorMsg)
	}
	// empty chunk
	out = nil
	_ = f.svc.HandleUpload(ctx, javaser.EncodeTupleChunk(chunkA, bit.Name(), nil), send)
	if r, _ := javaser.DecodeUploadResponse(out[0]); *r.ErrorMsg != MsgChunkEmpty || r.ChunkID != chunkA {
		t.Fatalf("%+v", r)
	}
	// garbage / wrong class
	for _, p := range [][]byte{[]byte("garbage"), javaser.EncodeLong(1)} {
		out = nil
		_ = f.svc.HandleUpload(ctx, p, send)
		if r, _ := javaser.DecodeUploadResponse(out[0]); *r.ErrorMsg != MsgUnsupportedPayload || r.ChunkID != "" {
			t.Fatalf("%+v", r)
		}
	}
}

func TestMasterDownloadFlow(t *testing.T) {
	f := newFix(t, true)
	ctx := context.Background()
	bit := tt("BIT_GFP")
	data := f.upload(t, chunkA, bit, 2500)
	rid := "aaaaaaaa-e89b-12d3-a456-426614174000"

	got, err := f.svc.GetTuples(ctx, bit, 1500, rid)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data[:1500*bit.TupleSize()]) {
		t.Fatal("tuple bytes differ")
	}
	if len(f.peers.shared) != 1 || f.peers.shared[0].Status != model.Locked || f.peers.shared[0].ReservationID != rid+"_bit_gfp" || f.peers.shared[0].Total() != 1500 ||
		len(f.peers.shared[0].Reservations) != 2 {
		t.Fatalf("%+v", f.peers.shared)
	}
	if len(f.peers.updates) != 1 || f.peers.updates[0] != rid+"_bit_gfp:UNLOCKED" {
		t.Fatal(f.peers.updates)
	}
	// consumption recorded, reservation key removed
	if !f.mr.Exists(`"castor-consumption-"bit_gfp::1700000000000`) || f.mr.Exists(`"castor-reservation-store"::`+rid+"_bit_gfp") {
		t.Fatalf("keys %v", f.mr.Keys())
	}
	// remaining 1000: fragments [1500,2000) and [2000,2500) -> exactly the rest
	if n := pgstore.AvailableTuples(ctx, f.pg.Pool, "BIT_GFP"); n != 1000 {
		t.Fatal(n)
	}
	// not enough
	_, err = f.svc.GetTuples(ctx, bit, 1001, "bbbbbbbb-e89b-12d3-a456-426614174000")
	if err == nil || err.Error() != "Insufficient Tuples of type bit_gfp available (1000 out of 1001)." {
		t.Fatal(err)
	}
	// consume rest -> chunk object removed
	got, err = f.svc.GetTuples(ctx, bit, 1000, "cccccccc-e89b-12d3-a456-426614174000")
	// ORDER BY fragment_id: the split-off tail [1500,2000) has a newer id than [2000,2500)
	want := append(append([]byte{}, data[2000*bit.TupleSize():]...), data[1500*bit.TupleSize():2000*bit.TupleSize()]...)
	if err != nil || string(got) != string(want) {
		t.Fatal(err)
	}
	if _, ok := f.store.objs[chunkA]; ok {
		t.Fatal("object must be deleted when no fragment remains")
	}
	// telemetry: 2500 tuples within an hour -> round(2500/3600)=1
	td, err := f.svc.Telemetry(ctx, time.Hour)
	if err != nil || td.Interval != 3600000 || len(td.Metrics) != 10 || td.Metrics[0].Type != "BIT_GFP" || td.Metrics[0].ConsumptionRate != 1 || td.Metrics[0].Available != 0 {
		t.Fatalf("%+v %v", td, err)
	}
	td, _ = f.svc.Telemetry(ctx, 10*time.Second) // 2500/10 = 250
	if td.Metrics[0].ConsumptionRate != 250 {
		t.Fatal(td.Metrics[0])
	}
}

func TestMasterShareFailures(t *testing.T) {
	f := newFix(t, true)
	ctx := context.Background()
	bit := tt("BIT_GFP")
	f.upload(t, chunkA, bit, 100)
	f.peers.shareOK = false
	if _, err := f.svc.GetTuples(ctx, bit, 10, "aaaaaaaa-e89b-12d3-a456-426614174000"); err == nil || err.Error() != MsgSharingFailed {
		t.Fatal(err)
	}
	// TX-A rolled back: fragments still free
	if n := pgstore.AvailableTuples(ctx, f.pg.Pool, "BIT_GFP"); n != 100 {
		t.Fatal(n)
	}
	f.peers.shareOK, f.peers.shareErr = true, &peer.ClientError{Msg: peer.FailedSharing}
	if _, err := f.svc.GetTuples(ctx, bit, 10, "aaaaaaaa-e89b-12d3-a456-426614174000"); err == nil || err.Error() != MsgCreateFailed {
		t.Fatal(err)
	}
}

func TestIdempotentRetryAndMismatch(t *testing.T) {
	f := newFix(t, true)
	ctx := context.Background()
	bit := tt("BIT_GFP")
	f.upload(t, chunkA, bit, 100)
	rid := "aaaaaaaa-e89b-12d3-a456-426614174000_bit_gfp"
	r := &model.Reservation{ReservationID: rid, TupleType: "BIT_GFP", Status: model.Unlocked,
		Reservations: []model.ReservationElement{{TupleChunkID: chunkA, ReservedTuples: 10, StartIndex: 0}}}
	if err := f.svc.Cache.PutReservation(ctx, r); err != nil {
		t.Fatal(err)
	}
	_, err := f.svc.GetTuples(ctx, bit, 11, "aaaaaaaa-e89b-12d3-a456-426614174000")
	if err == nil || !strings.HasPrefix(err.Error(), `Reservation does not match expected specification (id: "`+rid+`", tupleType: "bit_gfp", count: 11): Reservation(reservationId=`) {
		t.Fatal(err)
	}
	// matching UNLOCKED reservation is reused without a new compose
	if _, err := f.svc.GetTuples(ctx, bit, 10, "aaaaaaaa-e89b-12d3-a456-426614174000"); err != nil {
		t.Fatal(err)
	}
	if len(f.peers.shared) != 0 {
		t.Fatal("must not reshare")
	}
	// LOCKED leftover blocks retry with conflict (Java behaviour)
	r.Status = model.Locked
	_ = f.svc.Cache.PutReservation(ctx, r)
	_, err = f.svc.GetTuples(ctx, bit, 10, "aaaaaaaa-e89b-12d3-a456-426614174000")
	if err == nil || err.Error() != fmt.Sprintf(MsgConflict, rid) {
		t.Fatal(err)
	}
}

func TestSlaveApplyAndDownload(t *testing.T) {
	f := newFix(t, false)
	ctx := context.Background()
	bit := tt("BIT_GFP")
	data := f.upload(t, chunkA, bit, 2500)
	rid := "aaaaaaaa-e89b-12d3-a456-426614174000"
	res := &model.Reservation{ReservationID: rid + "_bit_gfp", TupleType: "BIT_GFP", Status: model.Locked,
		Reservations: []model.ReservationElement{{TupleChunkID: chunkA, ReservedTuples: 1200, StartIndex: 300}}}
	if err := f.svc.ApplyReservation(ctx, res); err != nil {
		t.Fatal(err)
	}
	// [0,300) free, [300,1000) and [1000,1500) reserved, [1500,2000),[2000,2500) free -> 1300 left
	if n := pgstore.AvailableTuples(ctx, f.pg.Pool, "BIT_GFP"); n != 1300 {
		t.Fatal(n)
	}
	// conflict on reapply
	if err := f.svc.ApplyReservation(ctx, res); err == nil || err.Error() != fmt.Sprintf(MsgConflict, rid+"_bit_gfp") {
		t.Fatal(err)
	}
	// download before unlock times out with the generic message
	if _, err := f.svc.GetTuples(ctx, bit, 1200, rid); err == nil || err.Error() != fmt.Sprintf(MsgNoReleased, rid+"_bit_gfp") {
		t.Fatal(err)
	}
	// unlock concurrently while the slave polls
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = f.svc.UpdateReservation(ctx, rid+"_bit_gfp", model.Unlocked)
	}()
	f.svc.S.ReservationTimeout = 2 * time.Second
	got, err := f.svc.GetTuples(ctx, bit, 1200, rid)
	if err != nil || string(got) != string(data[300*32:1500*32]) {
		t.Fatal(err)
	}
	if n := pgstore.AvailableTuples(ctx, f.pg.Pool, "BIT_GFP"); n != 1300 {
		t.Fatal(n)
	}
	// unsatisfiable
	bad := &model.Reservation{ReservationID: "x_bit_gfp", TupleType: "BIT_GFP", Status: model.Locked,
		Reservations: []model.ReservationElement{{TupleChunkID: chunkA, ReservedTuples: 10, StartIndex: 400}}}
	if err := f.svc.ApplyReservation(ctx, bad); err == nil || !strings.HasPrefix(err.Error(), "No fragment found to fulfill given reservation: Reservation(") || !strings.HasSuffix(err.Error(), ").") {
		t.Fatal(err)
	}
	// unknown reservation update
	if err := f.svc.UpdateReservation(ctx, "nope", model.Unlocked); err == nil || err.Error() != "No reservation was found for requestId nope." {
		t.Fatal(err)
	}
	// master-only guard
	f.svc.S.IsMaster = true
	if _, err := f.svc.GetTuples(ctx, bit, 5, "dddddddd-e89b-12d3-a456-426614174000"); err == nil || err.Error() != MsgNotMaster {
		t.Fatal(err)
	}
}

func TestActivateMissing(t *testing.T) {
	f := newFix(t, true)
	if err := f.svc.ActivateChunk(context.Background(), chunkA); err == nil || err.Error() != MsgNoFragmentForChunk {
		t.Fatal(err)
	}
}

func TestGenerateFragments(t *testing.T) {
	fr, _ := GenerateFragments(chunkA, tt("BIT_GFP"), 2500, 1000)
	if len(fr) != 3 || fr[2].Start != 2000 || fr[2].End != 2500 || fr[0].Status != "LOCKED" {
		t.Fatal(fr)
	}
	if fr, _ := GenerateFragments(chunkA, tt("BIT_GFP"), 2000, 1000); len(fr) != 2 {
		t.Fatal(len(fr))
	}
}
