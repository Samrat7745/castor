package client

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Samrat7745/castor/go/internal/httpapi"
	"github.com/Samrat7745/castor/go/internal/model"
	"github.com/Samrat7745/castor/go/internal/pgstore"
	"github.com/Samrat7745/castor/go/internal/rediscache"
	"github.com/Samrat7745/castor/go/internal/service"
	"github.com/Samrat7745/castor/go/internal/stomp"
	"github.com/Samrat7745/castor/go/internal/testenv"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testenv.Shutdown()
	os.Exit(code)
}

// Contract tests: the public clients against the real in-process Castor handlers (httpapi, stomp,
// service) backed by Postgres (container) and Redis (miniredis), no rig needed. A master and a
// slave instance are wired together through the public InterVcpClient, as in a deployment.

type memTuples struct {
	mu   sync.Mutex
	objs map[string][]byte
}

func (m *memTuples) Save(_ context.Context, id string, d []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[id] = append([]byte(nil), d...)
	return nil
}

func (m *memTuples) Read(_ context.Context, id string, off, n int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[id]
	if !ok || off+n > int64(len(o)) {
		return nil, errors.New("no such object")
	}
	return o[off : off+n], nil
}

func (m *memTuples) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, id)
	return nil
}

type castorInstance struct {
	URL    string
	Svc    *service.Service
	PG     *pgstore.Store
	Tuples *memTuples

	mu    sync.Mutex
	auths []string // Authorization header of every request
}

func (c *castorInstance) lastAuths() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.auths...)
}

func startCastor(t *testing.T, dsn string, master bool, slaves []string, bearer func(string) string) *castorInstance {
	t.Helper()
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
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cache := rediscache.New(rdb, "castor-reservation-store", "castor-consumption-", 1000)
	inst := &castorInstance{PG: pg, Tuples: &memTuples{objs: map[string][]byte{}}}
	inst.Svc = &service.Service{
		S:  service.Settings{IsMaster: master, InitialFragmentSize: 1000, ReservationTimeout: 400 * time.Millisecond, RetryDelay: 10 * time.Millisecond},
		PG: pg, Cache: cache, Tuples: inst.Tuples,
	}
	if master {
		ic, err := NewInterVcpClient(Options{ServiceAddresses: slaves, BearerTokenProvider: bearer})
		if err != nil {
			t.Fatal(err)
		}
		inst.Svc.Peers = ic
	}
	mux := http.NewServeMux()
	mux.Handle("/intra-vcp/ws", stomp.NewServer(stomp.Config{MaxMessage: 10 << 20, ServerReceiveMs: 10000}, inst.Svc.HandleUpload, nil))
	h := httpapi.New(inst.Svc, []httpapi.HealthCheck{pg.Ping, cache.Ping}, httpapi.Info{Name: "castor-service"}, mux, nil)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst.mu.Lock()
		inst.auths = append(inst.auths, r.Header.Get("Authorization"))
		inst.mu.Unlock()
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	inst.URL = ts.URL
	return inst
}

func secondDSN(t *testing.T, dsn string) string {
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
	if _, err := c.Exec(ctx, "CREATE DATABASE castor_client_slave"); err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatal(err)
	}
	u.Path = "/castor_client_slave"
	return u.String()
}

type vcps struct{ master, slave *castorInstance }

func startVCPs(t *testing.T, bearer func(string) string) vcps {
	t.Helper()
	dsn := testenv.PostgresDSN(t)
	slave := startCastor(t, secondDSN(t, dsn), false, nil, bearer)
	master := startCastor(t, dsn, true, []string{slave.URL}, bearer)
	return vcps{master, slave}
}

func opts(inst *castorInstance, tok string) Options {
	o := Options{ServiceAddresses: []string{inst.URL}, Timeout: 10 * time.Second}
	if tok != "" {
		o.BearerTokenProvider = func(string) string { return tok }
	}
	return o
}

// numbered tuple data so downloaded shares can be matched to what was uploaded.
func numbered(tt TupleType, n int) []byte {
	d := make([]byte, n*tt.TupleSize())
	for i := range d {
		d[i] = byte(i/tt.TupleSize()*7 + i%251)
	}
	return d
}

func uploadAndActivate(t *testing.T, inst *castorInstance, tok string, chunk TupleChunk) {
	t.Helper()
	up, err := NewUploadClient(opts(inst, tok))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := up.Connect(ctx, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	defer up.Disconnect()
	if ok, err := up.UploadTupleChunk(ctx, chunk, 10*time.Second); err != nil || !ok {
		t.Fatalf("upload: %v %v", ok, err)
	}
	if err := up.ActivateTupleChunk(ctx, chunk.ChunkID); err != nil {
		t.Fatal(err)
	}
}

func checkShares(t *testing.T, list *TupleList, tt TupleType, data []byte, first, n int) {
	t.Helper()
	if list.TupleCls != tt.Cls || list.Field.Name != tt.Field.Name || list.Field.ElementSize != tt.Field.ElementSize || len(list.Tuples) != n {
		t.Fatalf("list %s/%s with %d tuples, want %s/%s with %d", list.TupleCls, list.Field.Name, len(list.Tuples), tt.Cls, tt.Field.Name, n)
	}
	es, ts := tt.Field.ElementSize, tt.TupleSize()
	for i, tu := range list.Tuples {
		if len(tu.Shares) != tt.Arity {
			t.Fatalf("tuple %d has %d shares", i, len(tu.Shares))
		}
		for s, sh := range tu.Shares {
			off := (first+i)*ts + s*2*es
			if !bytes.Equal(sh.Value, data[off:off+es]) || !bytes.Equal(sh.Mac, data[off+es:off+2*es]) {
				t.Fatalf("tuple %d share %d differs from the uploaded data", i, s)
			}
		}
	}
}

func TestContractUploadDownloadTelemetry(t *testing.T) {
	v := startVCPs(t, func(string) string { return "tok" })
	tt, _ := ParseTupleType("MULTIPLICATION_TRIPLE_GFP")
	chunk := TupleChunk{ChunkID: uuid.New(), TupleType: tt, Tuples: numbered(tt, 100)}
	uploadAndActivate(t, v.master, "tok", chunk)
	uploadAndActivate(t, v.slave, "tok", chunk)
	ctx := context.Background()

	master, _ := NewIntraVcpClient(opts(v.master, "tok"))
	slave, _ := NewIntraVcpClient(opts(v.slave, "tok"))

	td, err := master.GetTelemetryData(ctx)
	if err != nil || len(td.Metrics) != 10 || td.Interval != 60000 {
		t.Fatalf("%+v %v", td, err)
	}
	for _, m := range td.Metrics {
		want := int64(0)
		if m.Type == tt.Name() {
			want = 100
		}
		if m.Available != want || m.ConsumptionRate != 0 {
			t.Fatalf("before download: %+v", m)
		}
	}

	// both VCPs ask for the same request id at the same time (the slave waits for the master's reservation)
	req := uuid.New()
	var mList, sList *TupleList
	var mErr, sErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); mList, mErr = master.DownloadTupleShares(ctx, req, tt, 30) }()
	go func() { defer wg.Done(); sList, sErr = slave.DownloadTupleShares(ctx, req, tt, 30) }()
	wg.Wait()
	if mErr != nil || sErr != nil {
		t.Fatal(mErr, sErr)
	}
	checkShares(t, mList, tt, chunk.Tuples, 0, 30)
	checkShares(t, sList, tt, chunk.Tuples, 0, 30)

	// the tuples are consumed: gone from the stock and visible in the consumption rate
	for _, c := range []IntraVcpClient{master, slave} {
		td, err = c.GetTelemetryDataFor(ctx, 10)
		if err != nil || td.Interval != 10000 {
			t.Fatalf("%+v %v", td, err)
		}
		for _, m := range td.Metrics {
			if m.Type == tt.Name() && (m.Available != 70 || m.ConsumptionRate != 3) {
				t.Fatalf("after download: %+v", m)
			}
		}
	}
	// the next request continues behind the consumed tuples
	next, err := master.DownloadTupleShares(ctx, uuid.New(), tt, 5)
	if err != nil {
		t.Fatal(err)
	}
	checkShares(t, next, tt, chunk.Tuples, 30, 5)

	// error cases come back as ClientErrors carrying the HTTP status
	for _, c := range []struct {
		name  string
		count int64
		want  string
	}{{"more than available", 1000, "Insufficient Tuples of type multiplicationtriple_gfp available (65 out of 1000)"}, {"zero", 0, "HTTP 400"}, {"negative", -3, "HTTP 400"}} {
		_, err := master.DownloadTupleShares(ctx, uuid.New(), tt, c.count)
		var ce *ClientError
		if !errors.As(err, &ce) || !strings.Contains(ce.Error(), c.want) || !strings.Contains(ce.Error(), "Failed downloading tuples from service "+v.master.URL) {
			t.Fatalf("%s: %v", c.name, err)
		}
	}
	// the slave only knows reservations announced by the master
	_, err = slave.DownloadTupleShares(ctx, uuid.New(), tt, 1)
	if err == nil || !strings.Contains(err.Error(), "No released tuple reservation was found") {
		t.Fatalf("unannounced request on the slave: %v", err)
	}

	// every request of the clients carried the bearer token, also master -> slave and the handshake
	for _, inst := range []*castorInstance{v.master, v.slave} {
		for _, a := range inst.lastAuths() {
			if a != "Bearer tok" {
				t.Fatalf("%s saw Authorization %q", inst.URL, a)
			}
		}
	}
}

func TestContractInterVcpClient(t *testing.T) {
	v := startVCPs(t, nil)
	tt, _ := ParseTupleType("BIT_GFP")
	chunk := TupleChunk{ChunkID: uuid.New(), TupleType: tt, Tuples: numbered(tt, 50)}
	uploadAndActivate(t, v.slave, "", chunk)
	ctx := context.Background()
	inter, err := NewInterVcpClient(opts(v.slave, ""))
	if err != nil {
		t.Fatal(err)
	}
	req := uuid.NewString() + "_" + tt.String()
	res := &Reservation{ReservationID: req, TupleType: tt.Name(), Status: model.Locked,
		Reservations: []model.ReservationElement{{TupleChunkID: chunk.ChunkID.String(), ReservedTuples: 10, StartIndex: 20}}}
	if ok, err := inter.ShareReservation(ctx, res); err != nil || !ok {
		t.Fatalf("share: %v %v", ok, err)
	}
	// the same id again conflicts, a range that is not free (or an unknown chunk) is refused: no 2xx
	if ok, err := inter.ShareReservation(ctx, res); err != nil || ok {
		t.Fatalf("duplicate share: %v %v", ok, err)
	}
	overlap := *res
	overlap.ReservationID = uuid.NewString() + "_" + tt.String()
	if ok, err := inter.ShareReservation(ctx, &overlap); err != nil || ok {
		t.Fatalf("overlapping share: %v %v", ok, err)
	}
	unknown := *res
	unknown.ReservationID = uuid.NewString() + "_" + tt.String()
	unknown.Reservations = []model.ReservationElement{{TupleChunkID: uuid.NewString(), ReservedTuples: 1, StartIndex: 0}}
	if ok, err := inter.ShareReservation(ctx, &unknown); err != nil || ok {
		t.Fatalf("unknown chunk: %v %v", ok, err)
	}
	// locked: the slave does not hand the tuples out until the master releases them
	intra, _ := NewIntraVcpClient(opts(v.slave, ""))
	reqID, _ := uuid.Parse(strings.SplitN(req, "_", 2)[0])
	if _, err := intra.DownloadTupleShares(ctx, reqID, tt, 10); err == nil || !strings.Contains(err.Error(), "No released tuple reservation") {
		t.Fatalf("locked reservation was served: %v", err)
	}
	if err := inter.UpdateReservationStatus(ctx, req, model.Unlocked); err != nil {
		t.Fatal(err)
	}
	list, err := intra.DownloadTupleShares(ctx, reqID, tt, 10)
	if err != nil {
		t.Fatal(err)
	}
	checkShares(t, list, tt, chunk.Tuples, 20, 10)
	// updating an unknown reservation is answered with an error status the client does not inspect
	if err := inter.UpdateReservationStatus(ctx, uuid.NewString(), model.Unlocked); err != nil {
		t.Fatal(err)
	}
	// a bad slave among the addresses: transport errors are reported, non-2xx makes share false
	down := httptest.NewServer(http.NotFoundHandler())
	down.Close()
	two, _ := NewInterVcpClient(Options{ServiceAddresses: []string{v.slave.URL, down.URL}, Timeout: 2 * time.Second})
	var ce *ClientError
	if ok, err := two.ShareReservation(ctx, &Reservation{ReservationID: uuid.NewString() + "_bit_gfp", TupleType: tt.Name(), Status: model.Locked,
		Reservations: []model.ReservationElement{{TupleChunkID: chunk.ChunkID.String(), ReservedTuples: 1, StartIndex: 0}}}); ok || !errors.As(err, &ce) || ce.Msg != FailedSharing {
		t.Fatalf("share with a dead slave: %v %v", ok, err)
	}
	if err := two.UpdateReservationStatus(ctx, req, model.Unlocked); !errors.As(err, &ce) || ce.Msg != FailedUpdating {
		t.Fatalf("update with a dead slave: %v", err)
	}
}

func TestContractUploadErrors(t *testing.T) {
	v := startVCPs(t, nil)
	tt, _ := ParseTupleType("INPUT_MASK_GFP")
	ctx := context.Background()
	up, _ := NewUploadClient(opts(v.master, ""))
	// not connected
	if _, err := up.UploadTupleChunk(ctx, TupleChunk{ChunkID: uuid.New(), TupleType: tt, Tuples: numbered(tt, 1)}, time.Second); err == nil {
		t.Fatal("upload without connection")
	}
	if err := up.Connect(ctx, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	defer up.Disconnect()
	// a chunk without tuples is rejected by the service (response with failure)
	if ok, err := up.UploadTupleChunk(ctx, TupleChunk{ChunkID: uuid.New(), TupleType: tt}, 5*time.Second); err != nil || ok {
		t.Fatalf("empty chunk: %v %v", ok, err)
	}
	// the session survives and the next upload works
	good := TupleChunk{ChunkID: uuid.New(), TupleType: tt, Tuples: numbered(tt, 4)}
	if ok, err := up.UploadTupleChunk(ctx, good, 5*time.Second); err != nil || !ok {
		t.Fatalf("upload after failure: %v %v", ok, err)
	}
	// re-uploading the same chunk overlaps its own fragments
	if ok, err := up.UploadTupleChunk(ctx, good, 5*time.Second); err != nil || ok {
		t.Fatalf("duplicate chunk: %v %v", ok, err)
	}
	// activation: unknown chunk fails with the service's message, activating twice is fine
	var ce *ClientError
	err := up.ActivateTupleChunk(ctx, uuid.New())
	if !errors.As(err, &ce) || ce.Msg != FailedActivateChunkMsg || !strings.Contains(err.Error(), "HTTP 500") || !strings.Contains(err.Error(), "Not a single fragment associated") {
		t.Fatalf("unknown chunk: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := up.ActivateTupleChunk(ctx, good.ChunkID); err != nil {
			t.Fatal(err)
		}
	}
	// the uploaded and activated tuples are served
	master, _ := NewIntraVcpClient(opts(v.master, ""))
	slave, _ := NewIntraVcpClient(opts(v.slave, ""))
	if _, err := slave.DownloadTupleShares(ctx, uuid.New(), tt, 1); err == nil {
		t.Fatal("slave served an unannounced reservation")
	}
	// the master cannot reach the slave's database state (the chunk was never uploaded there): sharing fails
	_, err = master.DownloadTupleShares(ctx, uuid.New(), tt, 2)
	if err == nil || !strings.Contains(err.Error(), "Sharing reservation with slave services failed.") {
		t.Fatalf("download without the chunk on the slave: %v", err)
	}
	// ... and the failed attempt did not consume anything
	td, err := master.GetTelemetryData(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range td.Metrics {
		if m.Type == tt.Name() && (m.Available != 4 || m.ConsumptionRate != 0) {
			t.Fatalf("%+v", m)
		}
	}
}

func TestContractClientsAgainstUnreachableService(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	o := Options{ServiceAddresses: []string{dead.URL}, Timeout: time.Second}
	intra, _ := NewIntraVcpClient(o)
	tt, _ := ParseTupleType("BIT_GFP")
	var ce *ClientError
	if _, err := intra.GetTelemetryData(context.Background()); !errors.As(err, &ce) || !strings.HasPrefix(ce.Msg, "Failed fetching telemetry data from service") {
		t.Fatal(err)
	}
	if _, err := intra.DownloadTupleShares(context.Background(), uuid.New(), tt, 1); !errors.As(err, &ce) {
		t.Fatal(err)
	}
	up, _ := NewUploadClient(o)
	if err := up.Connect(context.Background(), time.Second); !errors.As(err, &ce) {
		t.Fatal(err)
	}
	if err := up.ActivateTupleChunk(context.Background(), uuid.New()); !errors.As(err, &ce) || ce.Msg != FailedActivateChunkMsg {
		t.Fatal(err)
	}
}
