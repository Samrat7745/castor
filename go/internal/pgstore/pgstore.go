// Package pgstore is the Postgres fragment store; table and sequence names match Hibernate.
package pgstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Fragment is a row of tuple_chunk_fragment. End is exclusive.
type Fragment struct {
	ID            int64
	ChunkID       string
	TupleType     string // enum name
	Start, End    int64
	Status        string // LOCKED | UNLOCKED
	ReservationID *string
}

// Querier is satisfied by *pgxpool.Pool and pgx.Tx.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Store wraps a connection pool.
type Store struct{ Pool *pgxpool.Pool }

const schema = `
CREATE TABLE IF NOT EXISTS tuple_chunk_fragment (
  fragment_id int8 NOT NULL,
  activation_status varchar(255),
  end_index int8 NOT NULL,
  reservation_id varchar(255),
  start_index int8,
  tuple_chunk_id uuid,
  tuple_type varchar(255),
  PRIMARY KEY (fragment_id));
CREATE SEQUENCE IF NOT EXISTS hibernate_sequence START 1 INCREMENT 1;`

// Open connects and ensures the (non-destructive) schema exists, like ddl-auto=update.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	s := &Store{Pool: pool}
	if err := s.EnsureSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// EnsureSchema creates table and sequence if missing.
func (s *Store) EnsureSchema(ctx context.Context) error {
	_, err := s.Pool.Exec(ctx, schema)
	return err
}

// Ping checks DB reachability (health).
func (s *Store) Ping(ctx context.Context) error { return s.Pool.Ping(ctx) }

// Close releases the pool.
func (s *Store) Close() { s.Pool.Close() }

// InTx runs fn in a dedicated transaction (REQUIRES_NEW semantics: uses its own connection).
func (s *Store) InTx(ctx context.Context, fn func(q Querier) error) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(context.Background())
		return err
	}
	return tx.Commit(ctx)
}

// DSN builds the connection string like the JDBC url (database fixed to "castor").
func DSN(host string, port int, user, password, db string) string {
	u := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=prefer", urlEsc(user), urlEsc(password), host, port, db)
	return u
}

func urlEsc(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~' {
			out = append(out, c)
		} else {
			out = append(out, fmt.Sprintf("%%%02X", c)...)
		}
	}
	return string(out)
}

const cols = "fragment_id, tuple_chunk_id::text, tuple_type, start_index, end_index, activation_status, reservation_id"

func scan(r pgx.Row) (*Fragment, error) {
	f := &Fragment{}
	if err := r.Scan(&f.ID, &f.ChunkID, &f.TupleType, &f.Start, &f.End, &f.Status, &f.ReservationID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return f, nil
}

func setLockTimeout(ctx context.Context, q Querier) error {
	_, err := q.Exec(ctx, "SET LOCAL lock_timeout = '3000ms'")
	return err
}

// FindAvailableByType returns (and row-locks) the oldest free UNLOCKED fragment, or nil.
// Must run inside a transaction for the lock to matter.
func FindAvailableByType(ctx context.Context, q Querier, tupleType string) (*Fragment, error) {
	if err := setLockTimeout(ctx, q); err != nil {
		return nil, err
	}
	return scan(q.QueryRow(ctx, "SELECT "+cols+" FROM tuple_chunk_fragment WHERE tuple_type=$1 AND activation_status='UNLOCKED' AND reservation_id IS NULL ORDER BY fragment_id ASC LIMIT 1 FOR UPDATE", tupleType))
}

// FindAvailableForChunkAt returns (and row-locks) the free UNLOCKED fragment containing idx, or nil.
func FindAvailableForChunkAt(ctx context.Context, q Querier, chunkID string, idx int64) (*Fragment, error) {
	if err := setLockTimeout(ctx, q); err != nil {
		return nil, err
	}
	return scan(q.QueryRow(ctx, "SELECT "+cols+" FROM tuple_chunk_fragment WHERE tuple_chunk_id=$1::uuid AND activation_status='UNLOCKED' AND reservation_id IS NULL AND start_index<=$2 AND end_index>$2 ORDER BY start_index DESC LIMIT 1 FOR UPDATE", chunkID, idx))
}

// HasConflict reports whether any fragment of the chunk overlaps [start,end).
func HasConflict(ctx context.Context, q Querier, chunkID string, start, end int64) (bool, error) {
	var one int
	err := q.QueryRow(ctx, "SELECT 1 FROM tuple_chunk_fragment WHERE tuple_chunk_id=$1::uuid AND start_index<$3 AND end_index>$2 LIMIT 1", chunkID, start, end).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// Insert stores a new fragment using hibernate_sequence and sets f.ID.
func Insert(ctx context.Context, q Querier, f *Fragment) error {
	return q.QueryRow(ctx, "INSERT INTO tuple_chunk_fragment (fragment_id, tuple_chunk_id, tuple_type, start_index, end_index, activation_status, reservation_id) VALUES (nextval('hibernate_sequence'), $1::uuid, $2, $3, $4, $5, $6) RETURNING fragment_id",
		f.ChunkID, f.TupleType, f.Start, f.End, f.Status, f.ReservationID).Scan(&f.ID)
}

// Update persists the mutable columns of a fragment.
func Update(ctx context.Context, q Querier, f *Fragment) error {
	_, err := q.Exec(ctx, "UPDATE tuple_chunk_fragment SET start_index=$2, end_index=$3, activation_status=$4, reservation_id=$5 WHERE fragment_id=$1", f.ID, f.Start, f.End, f.Status, f.ReservationID)
	return err
}

// AvailableTuples sums free UNLOCKED tuples of the type; any error or empty set yields 0 (Java parity).
func AvailableTuples(ctx context.Context, q Querier, tupleType string) int64 {
	var n *int64
	if err := q.QueryRow(ctx, "SELECT SUM(end_index - start_index) FROM tuple_chunk_fragment WHERE activation_status='UNLOCKED' AND reservation_id IS NULL AND tuple_type=$1", tupleType).Scan(&n); err != nil || n == nil {
		return 0
	}
	return *n
}

// UnlockChunk sets all fragments of the chunk to UNLOCKED and returns the number of rows.
func UnlockChunk(ctx context.Context, q Querier, chunkID string) (int64, error) {
	ct, err := q.Exec(ctx, "UPDATE tuple_chunk_fragment SET activation_status='UNLOCKED' WHERE tuple_chunk_id = $1::uuid", chunkID)
	return ct.RowsAffected(), err
}

// DeleteByReservation removes all fragments reserved under the id.
func DeleteByReservation(ctx context.Context, q Querier, reservationID string) error {
	_, err := q.Exec(ctx, "DELETE FROM tuple_chunk_fragment WHERE reservation_id=$1", reservationID)
	return err
}

// ChunkReferenced reports whether any fragment still references the chunk.
func ChunkReferenced(ctx context.Context, q Querier, chunkID string) (bool, error) {
	var ex bool
	err := q.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM tuple_chunk_fragment WHERE tuple_chunk_id=$1::uuid)", chunkID).Scan(&ex)
	return ex, err
}

// NewFragment validates like TupleChunkFragmentEntity.of.
func NewFragment(chunkID, tupleType string, start, end int64, status string, reservationID *string) (*Fragment, error) {
	if start < 0 {
		return nil, fmt.Errorf("Illegal start index (%d). (must be >= 0).", start)
	}
	if end < start {
		return nil, fmt.Errorf("Illegal last index (%d).", end)
	}
	return &Fragment{ChunkID: chunkID, TupleType: tupleType, Start: start, End: end, Status: status, ReservationID: reservationID}, nil
}

// SplitAt creates [idx,end) as a new row and shrinks f to [start,idx); returns f (head).
func SplitAt(ctx context.Context, q Querier, f *Fragment, idx int64) (*Fragment, error) {
	nf, err := NewFragment(f.ChunkID, f.TupleType, idx, f.End, f.Status, f.ReservationID)
	if err != nil {
		return nil, err
	}
	if err := Insert(ctx, q, nf); err != nil {
		return nil, err
	}
	if idx < f.Start {
		return nil, fmt.Errorf("Illegal last index (%d).", idx)
	}
	f.End = idx
	return f, Update(ctx, q, f)
}

// SplitBefore splits like SplitAt but returns the new tail; no-op (returns f) when idx is outside (start,end).
func SplitBefore(ctx context.Context, q Querier, f *Fragment, idx int64) (*Fragment, error) {
	if idx <= f.Start || idx >= f.End {
		return f, nil
	}
	nf, err := NewFragment(f.ChunkID, f.TupleType, idx, f.End, f.Status, f.ReservationID)
	if err != nil {
		return nil, err
	}
	f.End = idx
	if err := Update(ctx, q, f); err != nil {
		return nil, err
	}
	return nf, Insert(ctx, q, nf)
}

// KeepFragments inserts fragments after verifying none overlaps an existing fragment of its chunk.
// ErrConflict is returned on overlap.
func KeepFragments(ctx context.Context, q Querier, frags []*Fragment) error {
	for _, f := range frags {
		c, err := HasConflict(ctx, q, f.ChunkID, f.Start, f.End)
		if err != nil {
			return err
		}
		if c {
			return ErrConflict
		}
	}
	for _, f := range frags {
		if err := Insert(ctx, q, f); err != nil {
			return err
		}
	}
	return nil
}

// ErrConflict matches CastorClientException(CONFLICT_EXCEPTION_MSG).
var ErrConflict = errors.New("At least one tuple described by the given sequence is already referenced by another TupleChunkFragment.")
