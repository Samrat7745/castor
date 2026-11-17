// Package rediscache stores reservations and consumption counters in Redis using the same key
// layout and Java-serialized values as the Java service.
package rediscache

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/carbynestack/castor-go/internal/javaser"
	"github.com/carbynestack/castor-go/internal/model"
	"github.com/redis/go-redis/v9"
)

// Cache provides reservation and consumption storage.
type Cache struct {
	rdb               redis.UniversalClient
	reservationPrefix string
	consumptionStore  string
	ttl               time.Duration
}

// New creates a cache. reservationStore / consumptionStore are the raw property values
// (including their literal quotes).
func New(rdb redis.UniversalClient, reservationStore, consumptionStore string, ttlSeconds int64) *Cache {
	return &Cache{rdb: rdb, reservationPrefix: reservationStore + "::", consumptionStore: consumptionStore, ttl: time.Duration(ttlSeconds) * time.Second}
}

// Ping checks Redis reachability.
func (c *Cache) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }

func toData(r *model.Reservation) javaser.ReservationData {
	d := javaser.ReservationData{ID: r.ReservationID, Type: r.TupleType, Status: string(r.Status)}
	for _, e := range r.Reservations {
		d.Elements = append(d.Elements, javaser.ReservationElementData{ChunkID: e.TupleChunkID, Reserved: e.ReservedTuples, Start: e.StartIndex})
	}
	return d
}

func fromData(d *javaser.ReservationData) *model.Reservation {
	r := &model.Reservation{ReservationID: d.ID, TupleType: d.Type, Status: model.ActivationStatus(d.Status)}
	for _, e := range d.Elements {
		r.Reservations = append(r.Reservations, model.ReservationElement{TupleChunkID: e.ChunkID, ReservedTuples: e.Reserved, StartIndex: e.Start})
	}
	return r
}

// GetReservation returns nil when no reservation is stored.
func (c *Cache) GetReservation(ctx context.Context, id string) (*model.Reservation, error) {
	b, err := c.rdb.Get(ctx, c.reservationPrefix+id).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	d, err := javaser.DecodeReservation(b)
	if err != nil {
		return nil, err
	}
	return fromData(d), nil
}

// PutReservationIfAbsent stores r unless the key exists; returns false when it already existed.
func (c *Cache) PutReservationIfAbsent(ctx context.Context, r *model.Reservation) (bool, error) {
	return c.rdb.SetNX(ctx, c.reservationPrefix+r.ReservationID, javaser.EncodeReservation(toData(r)), 0).Result()
}

// PutReservation overwrites the stored reservation.
func (c *Cache) PutReservation(ctx context.Context, r *model.Reservation) error {
	return c.rdb.Set(ctx, c.reservationPrefix+r.ReservationID, javaser.EncodeReservation(toData(r)), 0).Err()
}

// DeleteReservation removes the reservation key.
func (c *Cache) DeleteReservation(ctx context.Context, id string) error {
	return c.rdb.Del(ctx, c.reservationPrefix+id).Err()
}

// ConsumptionPrefix returns the key prefix for a tuple type (toString form).
func (c *Cache) ConsumptionPrefix(tupleTypeString string) string {
	return c.consumptionStore + tupleTypeString + "::"
}

// KeepConsumption adds n to the counter for (type, timestamp), refreshing the TTL. The update is
// atomic (WATCH/MULTI) where the Java implementation is a lossy read-modify-write.
func (c *Cache) KeepConsumption(ctx context.Context, tsMillis int64, tupleTypeString string, n int64) error {
	// Same as ConsumptionCachingService.keepConsumption in Java (Redis transaction support is disabled there):
	// a plain GET followed by a SET of the sum with the telemetry TTL. Like Java, concurrent updates of the
	// same key are not atomic, and a stored value that is not a Long is an error (ClassCastException in Java).
	key := c.ConsumptionPrefix(tupleTypeString) + strconv.FormatInt(tsMillis, 10)
	total := n
	b, err := c.rdb.Get(ctx, key).Bytes()
	switch {
	case err == nil:
		cur, derr := javaser.DecodeLong(b)
		if derr != nil {
			return derr
		}
		total += cur
	case !errors.Is(err, redis.Nil):
		return err
	}
	return c.rdb.Set(ctx, key, javaser.EncodeLong(total), c.ttl).Err()
}

// ConsumptionSince sums counters of the type whose timestamp suffix is >= sinceMillis.
func (c *Cache) ConsumptionSince(ctx context.Context, sinceMillis int64, tupleTypeString string) (int64, error) {
	prefix := c.ConsumptionPrefix(tupleTypeString)
	var sum int64
	iter := c.rdb.Scan(ctx, 0, prefix+"*", 500).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		ts, err := strconv.ParseInt(strings.TrimPrefix(key, prefix), 10, 64)
		if err != nil || ts < sinceMillis {
			continue
		}
		b, err := c.rdb.Get(ctx, key).Bytes()
		if err != nil {
			continue // expired in between
		}
		if v, err := javaser.DecodeLong(b); err == nil {
			sum += v
		}
	}
	return sum, iter.Err()
}
