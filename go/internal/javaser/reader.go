package javaser

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// ErrFormat is returned for any stream that is not the expected Java serialization shape.
var ErrFormat = errors.New("javaser: unsupported or corrupt stream")

type rdesc struct {
	name   string
	flags  byte
	fields []field
	super  *rdesc
}

// Object is a decoded serialized object.
type Object struct {
	Class  string
	Fields map[string]any // prim: int64/bool/..., refs: *Object, string, []byte, EnumVal, nil
	Annots []any          // objects/blocks written by custom writeObject (e.g. ArrayList elements)
}

// EnumVal is a decoded enum constant name.
type EnumVal string

// BlockData is raw custom-serialization block data.
type BlockData []byte

type reader struct {
	b       []byte
	pos     int
	handles []any
}

func (r *reader) need(n int) error {
	if n < 0 || r.pos+n > len(r.b) {
		return ErrFormat
	}
	return nil
}
func (r *reader) u8() (byte, error) {
	if err := r.need(1); err != nil {
		return 0, err
	}
	v := r.b[r.pos]
	r.pos++
	return v, nil
}
func (r *reader) u16() (int, error) {
	if err := r.need(2); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint16(r.b[r.pos:])
	r.pos += 2
	return int(v), nil
}
func (r *reader) i32() (int32, error) {
	if err := r.need(4); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint32(r.b[r.pos:])
	r.pos += 4
	return int32(v), nil
}
func (r *reader) i64() (int64, error) {
	if err := r.need(8); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint64(r.b[r.pos:])
	r.pos += 8
	return int64(v), nil
}
func (r *reader) utf() (string, error) {
	n, err := r.u16()
	if err != nil {
		return "", err
	}
	if err := r.need(n); err != nil {
		return "", err
	}
	s := string(r.b[r.pos : r.pos+n])
	r.pos += n
	return s, nil
}
func (r *reader) assign(v any) int { r.handles = append(r.handles, v); return len(r.handles) - 1 }

// parse decodes a full stream into its root content object.
func parse(b []byte) (any, error) {
	r := &reader{b: b}
	if len(b) < 4 || b[0] != 0xac || b[1] != 0xed || b[2] != 0 || b[3] != 5 {
		return nil, ErrFormat
	}
	r.pos = 4
	return r.content()
}

func (r *reader) content() (any, error) {
	tc, err := r.u8()
	if err != nil {
		return nil, err
	}
	switch tc {
	case tcNull:
		return nil, nil
	case tcReference:
		h, err := r.i32()
		if err != nil {
			return nil, err
		}
		i := int(h) - baseHandle
		if i < 0 || i >= len(r.handles) {
			return nil, ErrFormat
		}
		return r.handles[i], nil
	case tcString:
		s, err := r.utf()
		if err != nil {
			return nil, err
		}
		r.assign(s)
		return s, nil
	case tcLongString:
		n, err := r.i64()
		if err != nil || n < 0 || n > math.MaxInt32 {
			return nil, ErrFormat
		}
		if err := r.need(int(n)); err != nil {
			return nil, err
		}
		s := string(r.b[r.pos : r.pos+int(n)])
		r.pos += int(n)
		r.assign(s)
		return s, nil
	case tcEnum:
		if _, err := r.classDesc(); err != nil {
			return nil, err
		}
		idx := r.assign(nil)
		name, err := r.content()
		s, ok := name.(string)
		if err != nil || !ok {
			return nil, ErrFormat
		}
		r.handles[idx] = EnumVal(s)
		return EnumVal(s), nil
	case tcArray:
		d, err := r.classDesc()
		if err != nil || d == nil {
			return nil, ErrFormat
		}
		idx := r.assign(nil)
		n, err := r.i32()
		if err != nil || n < 0 {
			return nil, ErrFormat
		}
		if d.name == "[B" {
			if err := r.need(int(n)); err != nil {
				return nil, err
			}
			out := make([]byte, n)
			copy(out, r.b[r.pos:])
			r.pos += int(n)
			r.handles[idx] = out
			return out, nil
		}
		return nil, ErrFormat // other arrays are not part of the Castor wire
	case tcObject:
		return r.object()
	}
	return nil, fmt.Errorf("%w: tc 0x%x", ErrFormat, tc)
}

func (r *reader) classDesc() (*rdesc, error) {
	tc, err := r.u8()
	if err != nil {
		return nil, err
	}
	switch tc {
	case tcNull:
		return nil, nil
	case tcReference:
		h, err := r.i32()
		if err != nil {
			return nil, err
		}
		i := int(h) - baseHandle
		if i < 0 || i >= len(r.handles) {
			return nil, ErrFormat
		}
		d, ok := r.handles[i].(*rdesc)
		if !ok {
			return nil, ErrFormat
		}
		return d, nil
	case tcClassDesc:
		name, err := r.utf()
		if err != nil {
			return nil, err
		}
		if _, err = r.i64(); err != nil {
			return nil, err
		}
		d := &rdesc{name: name}
		r.assign(d)
		if d.flags, err = r.u8(); err != nil {
			return nil, err
		}
		n, err := r.u16()
		if err != nil {
			return nil, err
		}
		for i := 0; i < n; i++ {
			c, err := r.u8()
			if err != nil {
				return nil, err
			}
			fn, err := r.utf()
			if err != nil {
				return nil, err
			}
			f := field{code: c, name: fn}
			if c == 'L' || c == '[' {
				s, err := r.content()
				if err != nil {
					return nil, err
				}
				f.sig, _ = s.(string)
			}
			d.fields = append(d.fields, f)
		}
		// class annotation
		for {
			b, err := r.u8()
			if err != nil {
				return nil, err
			}
			if b == tcEndBlock {
				break
			}
			return nil, ErrFormat
		}
		if d.super, err = r.classDesc(); err != nil {
			return nil, err
		}
		return d, nil
	}
	return nil, ErrFormat
}

func (r *reader) object() (any, error) {
	d, err := r.classDesc()
	if err != nil || d == nil {
		return nil, ErrFormat
	}
	obj := &Object{Class: d.name, Fields: map[string]any{}}
	r.assign(obj)
	var chain []*rdesc
	for c := d; c != nil; c = c.super {
		chain = append([]*rdesc{c}, chain...)
	}
	for _, c := range chain {
		if c.flags&scSerializable == 0 {
			return nil, ErrFormat
		}
		for _, f := range c.fields {
			v, err := r.fieldValue(f)
			if err != nil {
				return nil, err
			}
			obj.Fields[f.name] = v
		}
		if c.flags&scWriteMethod != 0 {
			for {
				if err := r.need(1); err != nil {
					return nil, err
				}
				if r.b[r.pos] == tcEndBlock {
					r.pos++
					break
				}
				if r.b[r.pos] == tcBlockData {
					r.pos++
					n, err := r.u8()
					if err != nil {
						return nil, err
					}
					if err := r.need(int(n)); err != nil {
						return nil, err
					}
					obj.Annots = append(obj.Annots, BlockData(r.b[r.pos:r.pos+int(n)]))
					r.pos += int(n)
					continue
				}
				v, err := r.content()
				if err != nil {
					return nil, err
				}
				obj.Annots = append(obj.Annots, v)
			}
		}
	}
	return obj, nil
}

func (r *reader) fieldValue(f field) (any, error) {
	switch f.code {
	case 'B', 'Z':
		v, err := r.u8()
		if f.code == 'Z' {
			return v != 0, err
		}
		return int64(int8(v)), err
	case 'C', 'S':
		v, err := r.u16()
		return int64(v), err
	case 'I', 'F':
		v, err := r.i32()
		return int64(v), err
	case 'J', 'D':
		return r.i64()
	case 'L', '[':
		return r.content()
	}
	return nil, ErrFormat
}

// FormatUUID renders msb/lsb like java.util.UUID.toString.
func FormatUUID(msb, lsb int64) string {
	m, l := uint64(msb), uint64(lsb)
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", m>>32, (m>>16)&0xffff, m&0xffff, l>>48, l&0xffffffffffff)
}

// ParseUUID parses canonical UUID text.
func ParseUUID(s string) (msb, lsb int64, ok bool) {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return 0, 0, false
	}
	var hi, lo uint64
	n := 0
	for _, c := range s {
		if c == '-' {
			continue
		}
		var v uint64
		switch {
		case c >= '0' && c <= '9':
			v = uint64(c - '0')
		case c >= 'a' && c <= 'f':
			v = uint64(c-'a') + 10
		case c >= 'A' && c <= 'F':
			v = uint64(c-'A') + 10
		default:
			return 0, 0, false
		}
		if n < 16 {
			hi = hi<<4 | v
		} else {
			lo = lo<<4 | v
		}
		n++
	}
	return int64(hi), int64(lo), true
}

func uuidFrom(v any) (string, error) {
	if v == nil {
		return "", nil
	}
	o, ok := v.(*Object)
	if !ok || o.Class != "java.util.UUID" {
		return "", ErrFormat
	}
	m, ok1 := o.Fields["mostSigBits"].(int64)
	l, ok2 := o.Fields["leastSigBits"].(int64)
	if !ok1 || !ok2 {
		return "", ErrFormat
	}
	return FormatUUID(m, l), nil
}

// TupleChunk is the decoded upload payload.
type TupleChunk struct {
	ChunkID string
	Type    string // TupleType enum name
	Tuples  []byte
}

// DecodeTupleChunk parses a Java-serialized TupleChunk. Any other class yields ErrFormat.
func DecodeTupleChunk(b []byte) (*TupleChunk, error) {
	v, err := parse(b)
	if err != nil {
		return nil, err
	}
	o, ok := v.(*Object)
	if !ok || o.Class != "io.carbynestack.castor.common.entities.TupleChunk" {
		return nil, ErrFormat
	}
	id, err := uuidFrom(o.Fields["chunkId"])
	if err != nil {
		return nil, err
	}
	tt, _ := o.Fields["tupleType"].(EnumVal)
	tuples, _ := o.Fields["tuples"].([]byte)
	return &TupleChunk{ChunkID: id, Type: string(tt), Tuples: tuples}, nil
}

// UploadResponse is the decoded UploadTupleChunkResponse.
type UploadResponse struct {
	ChunkID  string // "" when null
	ErrorMsg *string
	Success  bool
}

// DecodeUploadResponse parses a Java-serialized UploadTupleChunkResponse.
func DecodeUploadResponse(b []byte) (*UploadResponse, error) {
	v, err := parse(b)
	if err != nil {
		return nil, err
	}
	o, ok := v.(*Object)
	if !ok || o.Class != "io.carbynestack.castor.common.websocket.UploadTupleChunkResponse" {
		return nil, ErrFormat
	}
	id, err := uuidFrom(o.Fields["chunkId"])
	if err != nil {
		return nil, err
	}
	resp := &UploadResponse{ChunkID: id}
	resp.Success, _ = o.Fields["isSuccess"].(bool)
	if s, ok := o.Fields["errorMsg"].(string); ok {
		resp.ErrorMsg = &s
	}
	return resp, nil
}

// DecodeReservation parses a Java-serialized Reservation.
func DecodeReservation(b []byte) (*ReservationData, error) {
	v, err := parse(b)
	if err != nil {
		return nil, err
	}
	o, ok := v.(*Object)
	if !ok || o.Class != "io.carbynestack.castor.common.entities.Reservation" {
		return nil, ErrFormat
	}
	out := &ReservationData{}
	out.ID, _ = o.Fields["reservationId"].(string)
	t, _ := o.Fields["tupleType"].(EnumVal)
	s, _ := o.Fields["status"].(EnumVal)
	out.Type, out.Status = string(t), string(s)
	list, _ := o.Fields["reservations"].(*Object)
	if list == nil {
		return nil, ErrFormat
	}
	for _, a := range list.Annots {
		eo, ok := a.(*Object)
		if !ok {
			continue
		}
		if eo.Class != "io.carbynestack.castor.common.entities.ReservationElement" {
			return nil, ErrFormat
		}
		id, err := uuidFrom(eo.Fields["tupleChunkId"])
		if err != nil {
			return nil, err
		}
		rt, _ := eo.Fields["reservedTuples"].(int64)
		st, _ := eo.Fields["startIndex"].(int64)
		out.Elements = append(out.Elements, ReservationElementData{id, rt, st})
	}
	return out, nil
}

// DecodeLong parses a Java-serialized java.lang.Long.
func DecodeLong(b []byte) (int64, error) {
	v, err := parse(b)
	if err != nil {
		return 0, err
	}
	o, ok := v.(*Object)
	if !ok || o.Class != "java.lang.Long" {
		return 0, ErrFormat
	}
	n, ok := o.Fields["value"].(int64)
	if !ok {
		return 0, ErrFormat
	}
	return n, nil
}
