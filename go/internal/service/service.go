// Package service holds Castor's business logic (download, reservations, telemetry, upload).
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/Samrat7745/castor/go/internal/model"
	"github.com/Samrat7745/castor/go/internal/peer"
	"github.com/Samrat7745/castor/go/internal/pgstore"
	"github.com/Samrat7745/castor/go/internal/rediscache"
)

// BadRequestError maps to HTTP 400 (Java IllegalArgumentException).
type BadRequestError struct{ Msg string }

func (e *BadRequestError) Error() string { return e.Msg }

// Error is a failure with a fixed Java message and the underlying cause (CastorServiceException(msg, cause)).
type Error struct {
	Msg   string
	Cause error
}

func (e *Error) Error() string { return e.Msg }
func (e *Error) Unwrap() error { return e.Cause }

// Error messages, identical to the Java constants.
const (
	MsgInsufficient       = "Insufficient Tuples of type %s available (%s out of %s)."
	MsgSharingFailed      = "Sharing reservation with slave services failed."
	MsgFailedReserve      = "Failed to reserve the tuples."
	MsgNoAvailableFrag    = "Unable to locate available tuples."
	MsgConflict           = "Reservation conflict. Reservation with ID #%s already exists."
	MsgNoReservation      = "No reservation was found for requestId %s."
	MsgCannotSatisfy      = "No fragment found to fulfill given reservation: %s."
	MsgNoReleased         = "No released tuple reservation was found for the given request ID #%s."
	MsgNotMaster          = "Operating as master even though service is not declared to be the master."
	MsgMismatch           = "Reservation does not match expected specification (id: \"%s\", tupleType: \"%s\", count: %d): %s."
	MsgCreateFailed       = "Creating the reservation failed."
	MsgRetrieveFailed     = "Failed retrieving tuples from database."
	MsgNoFragmentForChunk = "Not a single fragment associated with the given identifier."
)

// TupleStore is the object store (Minio) abstraction.
type TupleStore interface {
	Save(ctx context.Context, chunkID string, data []byte) error
	Read(ctx context.Context, chunkID string, offset, length int64) ([]byte, error)
	Delete(ctx context.Context, chunkID string) error
}

// InterVCP is the master -> slave client.
type InterVCP interface {
	ShareReservation(ctx context.Context, r *model.Reservation) (bool, error)
	UpdateReservationStatus(ctx context.Context, id string, status model.ActivationStatus) error
}

// Settings are the tunables the service needs.
type Settings struct {
	IsMaster            bool
	InitialFragmentSize int64
	ReservationTimeout  time.Duration
	RetryDelay          time.Duration
}

// Service implements all Castor operations.
type Service struct {
	S      Settings
	PG     *pgstore.Store
	Cache  *rediscache.Cache
	Tuples TupleStore
	Peers  InterVCP // nil on slaves
	Log    *slog.Logger
	Now    func() time.Time
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Service) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

// GetTuples returns the raw tuple bytes (concatenated reservation elements) for the request.
func (s *Service) GetTuples(ctx context.Context, tt model.TupleType, count int64, requestID string) ([]byte, error) {
	rid := requestID + "_" + tt.String()
	var res *model.Reservation
	var err error
	if s.S.IsMaster {
		if res, err = s.getUnlockedReservation(ctx, rid, tt, count); err != nil {
			return nil, err
		}
		if res == nil {
			if res, err = s.createReservation(ctx, rid, tt, count); err != nil {
				return nil, err
			}
		}
	} else {
		if res, err = s.getReservationWithRetry(ctx, rid, tt, count); err != nil {
			return nil, err
		}
	}
	data, err := s.consume(ctx, tt, res)
	if err != nil {
		return nil, err
	}
	if err := s.deleteReserved(ctx, res); err != nil {
		return nil, err
	}
	if err := s.Cache.DeleteReservation(ctx, rid); err != nil {
		return nil, err
	}
	return data, nil
}

func (s *Service) consume(ctx context.Context, tt model.TupleType, res *model.Reservation) ([]byte, error) {
	var out []byte
	ts := int64(tt.TupleSize())
	for _, e := range res.Reservations {
		b, err := s.Tuples.Read(ctx, e.TupleChunkID, e.StartIndex*ts, e.ReservedTuples*ts)
		if err != nil {
			s.log().Error("reading tuples failed", "chunk", e.TupleChunkID, "err", err)
			return nil, &Error{MsgRetrieveFailed, err}
		}
		out = append(out, b...)
	}
	return out, nil
}

func (s *Service) deleteReserved(ctx context.Context, res *model.Reservation) error {
	if err := pgstore.DeleteByReservation(ctx, s.PG.Pool, res.ReservationID); err != nil {
		return err
	}
	for _, e := range res.Reservations {
		ref, err := pgstore.ChunkReferenced(ctx, s.PG.Pool, e.TupleChunkID)
		if err != nil {
			return err
		}
		if !ref {
			if err := s.Tuples.Delete(ctx, e.TupleChunkID); err != nil {
				s.log().Error("deleting object failed", "chunk", e.TupleChunkID, "err", err)
			}
		}
	}
	return nil
}

func (s *Service) getUnlockedReservation(ctx context.Context, rid string, tt model.TupleType, count int64) (*model.Reservation, error) {
	r, err := s.Cache.GetReservation(ctx, rid)
	if err != nil || r == nil {
		return nil, err
	}
	if r.Status == model.Locked {
		return nil, nil
	}
	if r.TupleType != tt.Name() || r.Total() != count {
		return nil, fmt.Errorf(MsgMismatch, rid, tt.String(), count, r.String())
	}
	return r, nil
}

func (s *Service) getReservationWithRetry(ctx context.Context, rid string, tt model.TupleType, count int64) (*model.Reservation, error) {
	ctx, cancel := context.WithTimeout(ctx, s.S.ReservationTimeout)
	defer cancel()
	delay := s.S.RetryDelay
	if delay <= 0 {
		delay = 50 * time.Millisecond
	}
	for {
		r, err := s.getUnlockedReservation(ctx, rid, tt, count)
		if err != nil {
			// Java swallows the failure of the polling future and reports the generic message.
			s.log().Error("getReservationWithRetry failed", "err", err)
			break
		}
		if r != nil {
			return r, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf(MsgNoReleased, rid)
		case <-time.After(delay):
		}
	}
	return nil, fmt.Errorf(MsgNoReleased, rid)
}

func (s *Service) applyConsumption(ctx context.Context, r *model.Reservation) error {
	tt, err := model.ParseTupleType(r.TupleType)
	if err != nil {
		return err
	}
	return s.Cache.KeepConsumption(ctx, s.now().UnixMilli(), tt.String(), r.Total())
}

// keepReservation stores the reservation in Redis if absent and records consumption.
func (s *Service) keepReservation(ctx context.Context, r *model.Reservation) error {
	ok, err := s.Cache.PutReservationIfAbsent(ctx, r)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf(MsgConflict, r.ReservationID)
	}
	return s.applyConsumption(ctx, r)
}

func (s *Service) createReservation(ctx context.Context, rid string, tt model.TupleType, count int64) (*model.Reservation, error) {
	if s.Peers == nil {
		return nil, errors.New(MsgNotMaster)
	}
	var res *model.Reservation
	// TX-A: fragments are committed before the slaves are asked to unlock.
	err := s.PG.InTx(ctx, func(q pgstore.Querier) error {
		els, err := s.composeElements(ctx, q, tt, count, rid)
		if err != nil {
			return err
		}
		r := &model.Reservation{ReservationID: rid, TupleType: tt.Name(), Status: model.Locked, Reservations: els}
		ok, err := s.Peers.ShareReservation(ctx, r)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New(MsgSharingFailed)
		}
		res = r
		return nil
	})
	if err != nil {
		return nil, wrapClient(err)
	}
	if err := s.keepReservation(ctx, res); err != nil {
		return nil, err
	}
	// TX-B
	res.Status = model.Unlocked
	if err := s.Peers.UpdateReservationStatus(ctx, rid, model.Unlocked); err != nil {
		return nil, wrapClient(err)
	}
	if err := s.UpdateReservation(ctx, rid, model.Unlocked); err != nil {
		return nil, err
	}
	return res, nil
}

// wrapClient converts peer transport errors (CastorClientException) to the generic create failure.
func wrapClient(err error) error {
	var ce *peer.ClientError
	if errors.As(err, &ce) || errors.Is(err, pgstore.ErrConflict) {
		return &Error{MsgCreateFailed, err}
	}
	return err
}

func (s *Service) composeElements(ctx context.Context, q pgstore.Querier, tt model.TupleType, count int64, rid string) ([]model.ReservationElement, error) {
	avail := pgstore.AvailableTuples(ctx, q, tt.Name())
	if avail < count {
		return nil, fmt.Errorf(MsgInsufficient, tt.String(), fmt.Sprint(avail), fmt.Sprint(count))
	}
	var els []model.ReservationElement
	remaining := count
	for remaining > 0 {
		f, err := pgstore.FindAvailableByType(ctx, q, tt.Name())
		if err == nil && f == nil {
			err = errors.New(MsgNoAvailableFrag)
		}
		if err != nil {
			s.log().Error("compose failed", "err", err)
			return nil, &Error{MsgFailedReserve, err}
		}
		inFrag := f.End - f.Start
		if inFrag > remaining {
			if f, err = pgstore.SplitAt(ctx, q, f, f.Start+remaining); err != nil {
				return nil, &Error{MsgFailedReserve, err}
			}
		}
		f.ReservationID = &rid
		if err := pgstore.Update(ctx, q, f); err != nil {
			return nil, &Error{MsgFailedReserve, err}
		}
		taken := min(inFrag, remaining)
		remaining -= taken
		els = append(els, model.ReservationElement{TupleChunkID: f.ChunkID, ReservedTuples: taken, StartIndex: f.Start})
	}
	return els, nil
}

// UpdateReservation changes the stored status (R5).
func (s *Service) UpdateReservation(ctx context.Context, id string, status model.ActivationStatus) error {
	r, err := s.Cache.GetReservation(ctx, id)
	if err != nil {
		return err
	}
	if r == nil {
		return fmt.Errorf(MsgNoReservation, id)
	}
	r.Status = status
	return s.Cache.PutReservation(ctx, r)
}

// ApplyReservation is the slave side of R4: store the reservation and mark the same ranges.
func (s *Service) ApplyReservation(ctx context.Context, r *model.Reservation) error {
	r.Status = model.Locked
	ok, err := s.Cache.PutReservationIfAbsent(ctx, r)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf(MsgConflict, r.ReservationID)
	}
	for _, re := range r.Reservations {
		start, end := re.StartIndex, re.StartIndex+re.ReservedTuples
		for start < end {
			var next int64
			err := s.PG.InTx(ctx, func(q pgstore.Querier) error {
				f, err := pgstore.FindAvailableForChunkAt(ctx, q, re.TupleChunkID, start)
				if err != nil {
					return err
				}
				if f == nil {
					return fmt.Errorf(MsgCannotSatisfy, r.String())
				}
				if f.Start < start {
					if f, err = pgstore.SplitBefore(ctx, q, f, start); err != nil {
						return err
					}
				}
				if end < f.End {
					if f, err = pgstore.SplitAt(ctx, q, f, end); err != nil {
						return err
					}
				}
				f.ReservationID = &r.ReservationID
				if err := pgstore.Update(ctx, q, f); err != nil {
					return err
				}
				next = f.End
				return nil
			})
			if err != nil {
				return err
			}
			start = next
		}
	}
	return s.applyConsumption(ctx, r)
}

// ActivateChunk unlocks all fragments of a chunk (R2).
func (s *Service) ActivateChunk(ctx context.Context, chunkID string) error {
	n, err := pgstore.UnlockChunk(ctx, s.PG.Pool, chunkID)
	if err != nil {
		return err
	}
	if n <= 0 {
		return errors.New(MsgNoFragmentForChunk)
	}
	return nil
}

// Telemetry computes TelemetryData for the interval (R3).
func (s *Service) Telemetry(ctx context.Context, interval time.Duration) (*model.TelemetryData, error) {
	secs := int64(interval / time.Second)
	out := &model.TelemetryData{Metrics: []model.TupleMetric{}, Interval: interval.Milliseconds()}
	for _, tt := range model.TupleTypes {
		since := s.now().UnixMilli() - interval.Milliseconds()
		sum, err := s.Cache.ConsumptionSince(ctx, since, tt.String())
		if err != nil {
			return nil, err
		}
		// Math.round(long / (float) seconds) -> int
		rate := int64(math.Floor(float64(float32(sum)/float32(secs)) + 0.5))
		out.Metrics = append(out.Metrics, model.TupleMetric{
			Available: pgstore.AvailableTuples(ctx, s.PG.Pool, tt.Name()), ConsumptionRate: rate, Type: tt.Name()})
	}
	return out, nil
}
