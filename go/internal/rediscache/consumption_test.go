package rediscache

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/carbynestack/castor-go/internal/javaser"
	"github.com/carbynestack/castor-go/internal/model"
	"github.com/carbynestack/castor-go/internal/testenv"
	"github.com/redis/go-redis/v9"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testenv.Shutdown()
	os.Exit(code)
}

// Ports of ConsumptionCachingServiceTest (Mockito on RedisTemplate: here miniredis, which lets us
// look at the real keys and values) and ConsumptionCachingServiceIT (a real Redis 7.0).

const prefix = "testCache_"

func newCache(t *testing.T, ttl int64) (*Cache, *miniredis.Miniredis) {
	mr := miniredis.RunT(t)
	return New(redis.NewClient(&redis.Options{Addr: mr.Addr()}), `"castor-reservation-store"`, prefix, ttl), mr
}

func decode(t *testing.T, mr *miniredis.Miniredis, key string) int64 {
	t.Helper()
	v, err := mr.Get(key)
	if err != nil {
		t.Fatal(key, err)
	}
	n, err := javaser.DecodeLong([]byte(v))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// givenNoValueForTimeStampInCache_whenKeepConsumption_thenPersistGivenValue and
// givenValueForSameTimeInCache_whenKeepConsumption_thenPersistGivenValue
func TestKeepConsumptionKeyValueAndTTL(t *testing.T) {
	c, mr := newCache(t, 100)
	ctx := context.Background()
	tt, _ := model.ParseTupleType("INPUT_MASK_GFP")
	key := prefix + tt.String() + "::100" // CacheKeyPrefix.simple().compute(prefix + tupleType) + time

	if err := c.KeepConsumption(ctx, 100, tt.String(), 42); err != nil {
		t.Fatal(err)
	}
	if got := decode(t, mr, key); got != 42 || mr.TTL(key) != 100*time.Second {
		t.Fatalf("value %d ttl %v", got, mr.TTL(key))
	}
	mr.FastForward(40 * time.Second)
	if err := c.KeepConsumption(ctx, 100, tt.String(), 42); err != nil {
		t.Fatal(err)
	}
	if got := decode(t, mr, key); got != 84 || mr.TTL(key) != 100*time.Second {
		t.Fatalf("value %d ttl %v (every write renews the time to live)", got, mr.TTL(key))
	}
	if len(mr.Keys()) != 1 {
		t.Fatal(mr.Keys())
	}
	// the prefix helper is the same string
	if c.ConsumptionPrefix(tt.String())+"100" != key {
		t.Fatal(c.ConsumptionPrefix(tt.String()))
	}
}

// givenTimeStamp_whenGetConsumptionForTupleType_thenFilterAccordinglyAndReturnAggregatedValue
func TestConsumptionSinceFilters(t *testing.T) {
	c, _ := newCache(t, 100)
	ctx := context.Background()
	const typ = "inputmask_gfp"
	for ts, v := range map[int64]int64{80: 12, 90: 97, 110: 27, 120: 15} {
		if err := c.KeepConsumption(ctx, ts, typ, v); err != nil {
			t.Fatal(err)
		}
	}
	// another type must never be counted
	_ = c.KeepConsumption(ctx, 110, "bit_gfp", 1000)
	for _, tc := range []struct{ since, want int64 }{{0, 12 + 97 + 27 + 15}, {100, 27 + 15}, {110, 27 + 15}, {111, 15}, {121, 0}, {-1 << 63, 151}} {
		if got, err := c.ConsumptionSince(ctx, tc.since, typ); err != nil || got != tc.want {
			t.Errorf("since %d: got %d (%v), want %d", tc.since, got, err, tc.want)
		}
	}
}

// --- ConsumptionCachingServiceIT against a real Redis ---

func realCache(t *testing.T, ttl int64) (*Cache, *redis.Client) {
	rdb := redis.NewClient(&redis.Options{Addr: testenv.RedisAddr(t)})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return New(rdb, "castor-reservation-store", "castor-consumption-", ttl), rdb
}

func TestITGivenCacheIsEmptyReturnZero(t *testing.T) {
	c, _ := realCache(t, 100)
	if got, err := c.ConsumptionSince(context.Background(), 0, "inputmask_gfp"); err != nil || got != 0 {
		t.Fatal(got, err)
	}
}

func TestITMultipleConsumptionValuesForTimeFrame(t *testing.T) {
	c, _ := realCache(t, 1000)
	ctx := context.Background()
	for _, tt := range model.TupleTypes {
		_ = c.KeepConsumption(ctx, 100, tt.String(), 11)
		_ = c.KeepConsumption(ctx, 200, tt.String(), 12)
	}
	const typ = "multiplicationtriple_gfp"
	for _, tc := range []struct{ since, want int64 }{{0, 23}, {101, 12}, {201, 0}} {
		if got, err := c.ConsumptionSince(ctx, tc.since, typ); err != nil || got != tc.want {
			t.Errorf("since %d: got %d (%v), want %d", tc.since, got, err, tc.want)
		}
	}
}

func TestITTimeToLiveRemovesValueAutomatically(t *testing.T) {
	c, rdb := realCache(t, 1)
	ctx := context.Background()
	if err := c.KeepConsumption(ctx, 100, "multiplicationtriple_gfp", 42); err != nil {
		t.Fatal(err)
	}
	key := "castor-consumption-multiplicationtriple_gfp::100"
	if n, _ := rdb.Exists(ctx, key).Result(); n != 1 {
		t.Fatal("value must be stored")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, _ := rdb.Exists(ctx, key).Result(); n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("value was not removed after its time to live")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got, _ := c.ConsumptionSince(ctx, 0, "multiplicationtriple_gfp"); got != 0 {
		t.Fatal(got)
	}
}

func TestITMultipleValuesForSameTimestampAreSummed(t *testing.T) {
	c, rdb := realCache(t, 1000)
	ctx := context.Background()
	var total int64
	for _, v := range []int64{1, 12, 23, 34, 42} {
		total += v
		if err := c.KeepConsumption(ctx, 100, "multiplicationtriple_gfp", v); err != nil {
			t.Fatal(err)
		}
	}
	b, err := rdb.Get(ctx, "castor-consumption-multiplicationtriple_gfp::100").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := javaser.DecodeLong(b); err != nil || got != total {
		t.Fatal(got, err)
	}
	// (Go improvement over the lossy Java read-modify-write) concurrent increments are not lost
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = c.KeepConsumption(ctx, 200, "bit_gfp", 1) }()
	}
	wg.Wait()
	if got, _ := c.ConsumptionSince(ctx, 200, "bit_gfp"); got != 40 {
		t.Fatalf("concurrent increments: %d", got)
	}
}
