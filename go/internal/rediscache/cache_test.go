package rediscache

import (
	"context"
	"testing"

	"github.com/Samrat7745/castor/go/internal/javaser"
	"github.com/Samrat7745/castor/go/internal/model"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const rs, cs = `"castor-reservation-store"`, `"castor-consumption-"`

func setup(t *testing.T) (*Cache, *miniredis.Miniredis) {
	mr := miniredis.RunT(t)
	return New(redis.NewClient(&redis.Options{Addr: mr.Addr()}), rs, cs, 100), mr
}

func res() *model.Reservation {
	return &model.Reservation{ReservationID: "abc_bit_gfp", TupleType: "BIT_GFP", Status: model.Locked,
		Reservations: []model.ReservationElement{{TupleChunkID: "123e4567-e89b-12d3-a456-426614174000", ReservedTuples: 10, StartIndex: 5}}}
}

func TestReservationRoundTripAndKey(t *testing.T) {
	c, mr := setup(t)
	ctx := context.Background()
	if r, err := c.GetReservation(ctx, "abc_bit_gfp"); r != nil || err != nil {
		t.Fatal(r, err)
	}
	ok, err := c.PutReservationIfAbsent(ctx, res())
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	if ok, _ := c.PutReservationIfAbsent(ctx, res()); ok {
		t.Fatal("second put must report existing")
	}
	// literal-quote key, Java serialized value
	v, err := mr.Get(`"castor-reservation-store"::abc_bit_gfp`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := javaser.DecodeReservation([]byte(v)); err != nil {
		t.Fatal(err)
	}
	r := res()
	r.Status = model.Unlocked
	if err := c.PutReservation(ctx, r); err != nil {
		t.Fatal(err)
	}
	got, _ := c.GetReservation(ctx, "abc_bit_gfp")
	if got.Status != model.Unlocked || got.Reservations[0].StartIndex != 5 {
		t.Fatalf("%+v", got)
	}
	_ = c.DeleteReservation(ctx, "abc_bit_gfp")
	if got, _ := c.GetReservation(ctx, "abc_bit_gfp"); got != nil {
		t.Fatal("deleted")
	}
}

func TestConsumption(t *testing.T) {
	c, mr := setup(t)
	ctx := context.Background()
	_ = c.KeepConsumption(ctx, 1000, "bit_gfp", 5)
	_ = c.KeepConsumption(ctx, 1000, "bit_gfp", 7)
	_ = c.KeepConsumption(ctx, 2000, "bit_gfp", 1)
	_ = c.KeepConsumption(ctx, 2000, "bit_gf2n", 100)
	if !mr.Exists(`"castor-consumption-"bit_gfp::1000`) {
		t.Fatalf("keys: %v", mr.Keys())
	}
	if ttl := mr.TTL(`"castor-consumption-"bit_gfp::1000`); ttl.Seconds() != 100 {
		t.Fatal(ttl)
	}
	v, _ := mr.Get(`"castor-consumption-"bit_gfp::1000`)
	if n, _ := javaser.DecodeLong([]byte(v)); n != 12 {
		t.Fatal(n)
	}
	if s, _ := c.ConsumptionSince(ctx, 1500, "bit_gfp"); s != 1 {
		t.Fatal(s)
	}
	if s, _ := c.ConsumptionSince(ctx, 0, "bit_gfp"); s != 13 {
		t.Fatal(s)
	}
}
