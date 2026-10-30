package service

import (
	"context"
	"testing"
	"time"
)

// Telemetry against a controlled clock (Service.Now): window edges, empty windows, rounding and
// the time to live of the consumption counters (miniredis FastForward) are deterministic.

func rateFor(t *testing.T, f *fix, interval time.Duration) int64 {
	t.Helper()
	td, err := f.svc.Telemetry(context.Background(), interval)
	if err != nil {
		t.Fatal(err)
	}
	if td.Interval != interval.Milliseconds() {
		t.Fatalf("interval %d", td.Interval)
	}
	return td.Metrics[0].ConsumptionRate // BIT_GFP is the first type
}

func TestTelemetryWindowBoundaries(t *testing.T) {
	f := newFix(t, true)
	ctx := context.Background()
	for _, iv := range []time.Duration{30 * time.Second, 8 * time.Second} {
		t.Run(iv.String(), func(t *testing.T) {
			f.mr.FlushAll()
			secs := int64(iv / time.Second)
			now := f.now.UnixMilli()
			edge := now - iv.Milliseconds()
			// exactly on the lower edge counts, one millisecond earlier does not
			_ = f.svc.Cache.KeepConsumption(ctx, edge, "bit_gfp", 10*secs)
			_ = f.svc.Cache.KeepConsumption(ctx, edge-1, "bit_gfp", 1000*secs)
			if got := rateFor(t, f, iv); got != 10 {
				t.Fatalf("edge sample: rate %d, want 10", got)
			}
			// the window slides with the clock: a millisecond later the edge sample is out
			f.now = f.now.Add(time.Millisecond)
			if got := rateFor(t, f, iv); got != 0 {
				t.Fatalf("after sliding: rate %d, want 0", got)
			}
			// samples at "now" and in the future still count
			f.now = f.now.Add(-time.Millisecond)
			_ = f.svc.Cache.KeepConsumption(ctx, now, "bit_gfp", secs)
			_ = f.svc.Cache.KeepConsumption(ctx, now+5000, "bit_gfp", secs)
			if got := rateFor(t, f, iv); got != 12 {
				t.Fatalf("with newest samples: rate %d, want 12", got)
			}
			f.now = time.UnixMilli(now)
		})
	}
}

func TestTelemetryEmptyWindowsAndOtherTypes(t *testing.T) {
	f := newFix(t, true)
	ctx := context.Background()
	for _, iv := range []time.Duration{30 * time.Second, 8 * time.Second, time.Second} {
		td, err := f.svc.Telemetry(ctx, iv)
		if err != nil || len(td.Metrics) != 10 {
			t.Fatal(err, td)
		}
		for _, m := range td.Metrics {
			if m.ConsumptionRate != 0 || m.Available != 0 {
				t.Fatalf("%s: empty store reports %+v", iv, m)
			}
		}
	}
	// consumption of one type does not leak into another, nor into a similarly named key
	_ = f.svc.Cache.KeepConsumption(ctx, f.now.UnixMilli(), "inputmask_gfp", 80)
	td, _ := f.svc.Telemetry(ctx, 8*time.Second)
	for _, m := range td.Metrics {
		want := int64(0)
		if m.Type == "INPUT_MASK_GFP" {
			want = 10
		}
		if m.ConsumptionRate != want {
			t.Fatalf("%s: rate %d, want %d", m.Type, m.ConsumptionRate, want)
		}
	}
}

// Rounding of consumption/seconds for both window sizes, halves round up (Math.round).
func TestTelemetryRoundingPerWindow(t *testing.T) {
	f := newFix(t, true)
	ctx := context.Background()
	for _, c := range []struct {
		iv           time.Duration
		consumed, rt int64
	}{
		{8 * time.Second, 3, 0}, {8 * time.Second, 4, 1}, {8 * time.Second, 11, 1}, {8 * time.Second, 12, 2},
		{30 * time.Second, 14, 0}, {30 * time.Second, 15, 1}, {30 * time.Second, 44, 1}, {30 * time.Second, 45, 2},
	} {
		f.mr.FlushAll()
		_ = f.svc.Cache.KeepConsumption(ctx, f.now.UnixMilli(), "bit_gfp", c.consumed)
		if got := rateFor(t, f, c.iv); got != c.rt {
			t.Errorf("%s consumed %d: rate %d, want %d", c.iv, c.consumed, got, c.rt)
		}
	}
}

// Samples of several seconds add up inside the window; the counter of every second expires on its own.
func TestTelemetryCounterExpiry(t *testing.T) {
	f := newFix(t, true)
	ctx := context.Background()
	now := f.now.UnixMilli()
	for i := int64(0); i < 8; i++ {
		_ = f.svc.Cache.KeepConsumption(ctx, now-i*1000, "bit_gfp", 2)
	}
	if got := rateFor(t, f, 8*time.Second); got != 2 {
		t.Fatalf("8 samples of 2 over 8s: rate %d", got)
	}
	// fixture TTL is 1000s: just before it nothing is lost, just after everything is gone
	f.mr.FastForward(999 * time.Second)
	if got := rateFor(t, f, 8*time.Second); got != 2 {
		t.Fatalf("before expiry: rate %d", got)
	}
	// a write renews the time to live of its own counter only
	_ = f.svc.Cache.KeepConsumption(ctx, now, "bit_gfp", 8)
	f.mr.FastForward(2 * time.Second)
	if got := rateFor(t, f, 8*time.Second); got != 1 {
		t.Fatalf("only the renewed sample survives (8/8): rate %d", got)
	}
	f.mr.FastForward(1000 * time.Second)
	if got := rateFor(t, f, 8*time.Second); got != 0 {
		t.Fatalf("after expiry: rate %d", got)
	}
}

// Consumption is stamped with the service clock when reservations are created or applied.
func TestTelemetryStampedWithServiceClock(t *testing.T) {
	f := newFix(t, true)
	ctx := context.Background()
	bit := tt("BIT_GFP")
	f.upload(t, chunkA, bit, 40)
	start := f.now
	if _, err := f.svc.GetTuples(ctx, bit, 16, "5b1b0ba4-0f1c-4a52-9b19-111111111111"); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		advance time.Duration
		want    int64
	}{{0, 2}, {8 * time.Second, 2}, {8*time.Second + time.Millisecond, 0}} {
		f.now = start.Add(c.advance)
		if got := rateFor(t, f, 8*time.Second); got != c.want {
			t.Fatalf("clock +%s: rate %d, want %d", c.advance, got, c.want)
		}
	}
	f.now = start.Add(time.Hour)
	if got := rateFor(t, f, 30*time.Second); got != 0 {
		t.Fatal(got)
	}
}
