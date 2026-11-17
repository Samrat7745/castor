package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ReservationElement is a slice of a chunk reserved for a request.
type ReservationElement struct {
	TupleChunkID   string `json:"tupleChunkId"`
	ReservedTuples int64  `json:"reservedTuples"`
	StartIndex     int64  `json:"startIndex"`
}

// Reservation is the inter-VCP / Redis reservation.
type Reservation struct {
	ReservationID string               `json:"reservationId"`
	TupleType     string               `json:"tupleType"` // enum name
	Status        ActivationStatus     `json:"status"`
	Reservations  []ReservationElement `json:"reservations"`
}

// Total returns the sum of reserved tuples.
func (r *Reservation) Total() int64 {
	var s int64
	for _, e := range r.Reservations {
		s += e.ReservedTuples
	}
	return s
}

// String mimics the Lombok toString of the Java class (used inside error messages).
func (r *Reservation) String() string {
	els := make([]string, 0, len(r.Reservations))
	for _, e := range r.Reservations {
		els = append(els, fmt.Sprintf("ReservationElement(tupleChunkId=%s, reservedTuples=%d, startIndex=%d)", e.TupleChunkID, e.ReservedTuples, e.StartIndex))
	}
	tt := r.TupleType
	if t, err := ParseTupleType(tt); err == nil {
		tt = t.String()
	}
	return fmt.Sprintf("Reservation(reservationId=%s, tupleType=%s, reservations=[%s], status=%s)", r.ReservationID, tt, strings.Join(els, ", "), r.Status)
}

// ErrBadRequest marks validation failures mapped to HTTP 400.
var ErrBadRequest = errors.New("bad request")

// UnmarshalJSON enforces the Jackson @JsonCreator required-field rules.
func (r *Reservation) UnmarshalJSON(b []byte) error {
	var raw struct {
		ReservationID *string          `json:"reservationId"`
		TupleType     *string          `json:"tupleType"`
		Status        *string          `json:"status"`
		Reservations  *json.RawMessage `json:"reservations"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	// Same checks and order as Jackson's required creator properties on the Java entity.
	for _, m := range []struct {
		name    string
		missing bool
	}{{"reservationId", raw.ReservationID == nil}, {"tupleType", raw.TupleType == nil}, {"status", raw.Status == nil}, {"reservations", raw.Reservations == nil}} {
		if m.missing {
			return missingProperty(m.name)
		}
	}
	if *raw.ReservationID == "" {
		return errors.New("ReservationId must not be empty.")
	}
	tt, err := ParseTupleType(*raw.TupleType)
	if err != nil {
		return err
	}
	st := ActivationStatus(*raw.Status)
	if st != Locked && st != Unlocked {
		return fmt.Errorf("invalid status %q", *raw.Status)
	}
	var els []struct {
		ID  *string `json:"tupleChunkId"`
		Res *int64  `json:"reservedTuples"`
		St  *int64  `json:"startIndex"`
	}
	if err := json.Unmarshal(*raw.Reservations, &els); err != nil || els == nil {
		return errors.New("invalid reservations")
	}
	if len(els) == 0 {
		return errors.New("At least one ReservationElement required.")
	}
	out := make([]ReservationElement, 0, len(els))
	for _, e := range els {
		for _, m := range []struct {
			name    string
			missing bool
		}{{"tupleChunkId", e.ID == nil}, {"reservedTuples", e.Res == nil}, {"startIndex", e.St == nil}} {
			if m.missing {
				return missingProperty(m.name)
			}
		}
		id, ok := JacksonUUID(*e.ID) // Jackson binds tupleChunkId to java.util.UUID
		if !ok {
			return errors.New("invalid UUID")
		}
		if *e.Res < 1 {
			return errors.New("At least 1 tuple share needs to be reserved.")
		}
		if *e.St < 0 {
			return errors.New("StartIndex must not be negative.")
		}
		// java.util.UUID#toString is the canonical lower-case form; object keys and Redis values depend on it.
		out = append(out, ReservationElement{id, *e.Res, *e.St})
	}
	r.ReservationID, r.TupleType, r.Status, r.Reservations = *raw.ReservationID, tt.Name(), st, out
	return nil
}

func missingProperty(name string) error {
	return fmt.Errorf("Missing required creator property '%s'", name)
}

func validUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
				return false
			}
		}
	}
	return true
}

// IsUUID reports whether s is a canonical UUID string.
func IsUUID(s string) bool { return validUUID(s) }
