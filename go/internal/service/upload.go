package service

import (
	"context"
	"errors"

	"github.com/carbynestack/castor-go/internal/javaser"
	"github.com/carbynestack/castor-go/internal/model"
	"github.com/carbynestack/castor-go/internal/pgstore"
)

// Upload messages.
const (
	MsgUnsupportedPayload = "Unsupported payload type."
	MsgChunkEmpty         = "Chunk must contain at least one tuple."
	MsgFailedWriting      = "Failed writing tuple chunk to database."
)

// HandleUpload processes one STOMP SEND payload. Responses are handed to broadcast (all
// subscribers of /queue/response). The returned error mirrors the exception Java throws after
// the failure frame has been sent (logged only).
func (s *Service) HandleUpload(ctx context.Context, payload []byte, broadcast func([]byte)) error {
	fail := func(id, msg string) { broadcast(javaser.EncodeUploadResponse(id, &msg, false)) }
	tc, err := javaser.DecodeTupleChunk(payload)
	var tt model.TupleType
	if err == nil {
		tt, err = model.ParseTupleType(tc.Type)
	}
	if err != nil {
		fail("", MsgUnsupportedPayload)
		return errors.New(MsgUnsupportedPayload)
	}
	if len(tc.Tuples) == 0 {
		fail(tc.ChunkID, MsgChunkEmpty)
		return errors.New(MsgChunkEmpty)
	}
	if err := s.storeChunk(ctx, tc, tt); err != nil {
		s.log().Error("Exception occurred while writing TupleChunk to database.", "err", err)
		fail(tc.ChunkID, MsgFailedWriting)
		return errors.New(MsgFailedWriting)
	}
	broadcast(javaser.EncodeUploadResponse(tc.ChunkID, nil, true))
	return nil
}

func (s *Service) storeChunk(ctx context.Context, tc *javaser.TupleChunk, tt model.TupleType) error {
	if err := s.Tuples.Save(ctx, tc.ChunkID, tc.Tuples); err != nil {
		return err
	}
	frags, err := GenerateFragments(tc.ChunkID, tt, int64(len(tc.Tuples)/tt.TupleSize()), s.S.InitialFragmentSize)
	if err != nil {
		return err
	}
	return s.PG.InTx(ctx, func(q pgstore.Querier) error { return pgstore.KeepFragments(ctx, q, frags) })
}

// GenerateFragments splits n tuples into LOCKED fragments of at most size tuples.
func GenerateFragments(chunkID string, tt model.TupleType, n, size int64) ([]*pgstore.Fragment, error) {
	var out []*pgstore.Fragment
	for i := int64(0); i*size < n; i++ {
		f, err := pgstore.NewFragment(chunkID, tt.Name(), i*size, min((i+1)*size, n), string(model.Locked), nil)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, nil
}
