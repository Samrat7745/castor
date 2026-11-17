// Package javaser is a hand-written, minimal Java native serialization codec for exactly the
// classes Castor puts on the wire (STOMP payloads) and into Redis values.
package javaser

import (
	"bytes"
	"encoding/binary"
	"math"
)

const (
	tcNull       = 0x70
	tcReference  = 0x71
	tcClassDesc  = 0x72
	tcObject     = 0x73
	tcString     = 0x74
	tcArray      = 0x75
	tcBlockData  = 0x77
	tcEndBlock   = 0x78
	tcEnum       = 0x7e
	tcLongString = 0x7c

	scWriteMethod  = 0x01
	scSerializable = 0x02
	scEnum         = 0x10
	baseHandle     = 0x7e0000
)

type field struct {
	code byte // 'B','J','I','Z','L','[' ...
	name string
	sig  string // for L and [ : JVM type signature
}

type desc struct {
	name   string
	suid   int64
	flags  byte
	fields []field // must be in canonical order (primitives then objects, each sorted by name)
	super  *desc
}

var (
	descUUID = &desc{"java.util.UUID", -4856846361193249489, scSerializable,
		[]field{{'J', "leastSigBits", ""}, {'J', "mostSigBits", ""}}, nil}
	descEnumBase = &desc{"java.lang.Enum", 0, scSerializable | scEnum, nil, nil}
	descByteArr  = &desc{"[B", -5984413125824719648, scSerializable, nil, nil}
	descNumber   = &desc{"java.lang.Number", -8742448824652078965, scSerializable, nil, nil}
	descLong     = &desc{"java.lang.Long", 4290774380558885855, scSerializable,
		[]field{{'J', "value", ""}}, descNumber}
	descArrayList = &desc{"java.util.ArrayList", 8683452581122892189, scSerializable | scWriteMethod,
		[]field{{'I', "size", ""}}, nil}

	descTupleChunk = &desc{"io.carbynestack.castor.common.entities.TupleChunk", -5025431542939631318, scSerializable,
		[]field{{'L', "chunkId", "Ljava/util/UUID;"}, {'L', "tupleType", "Lio/carbynestack/castor/common/entities/TupleType;"}, {'[', "tuples", "[B"}}, nil}
	descResponse = &desc{"io.carbynestack.castor.common.websocket.UploadTupleChunkResponse", 9192982150705341482, scSerializable,
		[]field{{'Z', "isSuccess", ""}, {'L', "chunkId", "Ljava/util/UUID;"}, {'L', "errorMsg", "Ljava/lang/String;"}}, nil}
	descReservation = &desc{"io.carbynestack.castor.common.entities.Reservation", -7176250403711227416, scSerializable,
		[]field{{'L', "reservationId", "Ljava/lang/String;"}, {'L', "reservations", "Ljava/util/List;"},
			{'L', "status", "Lio/carbynestack/castor/common/entities/ActivationStatus;"},
			{'L', "tupleType", "Lio/carbynestack/castor/common/entities/TupleType;"}}, nil}
	descResElement = &desc{"io.carbynestack.castor.common.entities.ReservationElement", -5294934943093478981, scSerializable,
		[]field{{'J', "reservedTuples", ""}, {'J', "startIndex", ""}, {'L', "tupleChunkId", "Ljava/util/UUID;"}}, nil}
	descTupleTypeEnum = &desc{"io.carbynestack.castor.common.entities.TupleType", 0, scSerializable | scEnum, nil, descEnumBase}
	descStatusEnum    = &desc{"io.carbynestack.castor.common.entities.ActivationStatus", 0, scSerializable | scEnum, nil, descEnumBase}
)

type writer struct {
	b       bytes.Buffer
	next    int
	descs   map[*desc]int
	strings map[string]int // only signature strings are shared
}

func newWriter() *writer {
	w := &writer{descs: map[*desc]int{}, strings: map[string]int{}}
	w.b.Write([]byte{0xac, 0xed, 0x00, 0x05})
	return w
}

func (w *writer) assign() int  { h := baseHandle + w.next; w.next++; return h }
func (w *writer) u8(v byte)    { w.b.WriteByte(v) }
func (w *writer) u16(v int)    { _ = binary.Write(&w.b, binary.BigEndian, uint16(v)) }
func (w *writer) i32(v int32)  { _ = binary.Write(&w.b, binary.BigEndian, v) }
func (w *writer) i64(v int64)  { _ = binary.Write(&w.b, binary.BigEndian, v) }
func (w *writer) utf(s string) { w.u16(len(s)); w.b.WriteString(modUTF8(s)) }
func (w *writer) ref(h int)    { w.u8(tcReference); w.i32(int32(h)) }

// modUTF8: strings here are ASCII/UTF-8 BMP; Java's modified UTF-8 differs only for NUL and
// supplementary chars, which are not expected in Castor identifiers/messages.
func modUTF8(s string) string { return s }

func (w *writer) str(s string, share bool) {
	if share {
		if h, ok := w.strings[s]; ok {
			w.ref(h)
			return
		}
	}
	h := w.assign()
	if share {
		w.strings[s] = h
	}
	if len(s) > math.MaxUint16 {
		w.u8(tcLongString)
		w.i64(int64(len(s)))
		w.b.WriteString(s)
		return
	}
	w.u8(tcString)
	w.utf(s)
}

func (w *writer) writeDesc(d *desc) {
	if d == nil {
		w.u8(tcNull)
		return
	}
	if h, ok := w.descs[d]; ok {
		w.ref(h)
		return
	}
	w.u8(tcClassDesc)
	w.utf(d.name)
	w.i64(d.suid)
	w.descs[d] = w.assign()
	w.u8(d.flags)
	w.u16(len(d.fields))
	for _, f := range d.fields {
		w.u8(f.code)
		w.utf(f.name)
		if f.code == 'L' || f.code == '[' {
			w.str(f.sig, true)
		}
	}
	w.u8(tcEndBlock)
	w.writeDesc(d.super)
}

func (w *writer) newObject(d *desc) {
	w.u8(tcObject)
	w.writeDesc(d)
	w.assign()
}

func (w *writer) null() { w.u8(tcNull) }

func (w *writer) enum(d *desc, name string) {
	w.u8(tcEnum)
	w.writeDesc(d)
	w.assign()
	w.str(name, false)
}

func (w *writer) uuid(id string) {
	if id == "" {
		w.null()
		return
	}
	msb, lsb, ok := ParseUUID(id)
	if !ok {
		w.null()
		return
	}
	w.newObject(descUUID)
	w.i64(lsb)
	w.i64(msb)
}

func (w *writer) byteArray(b []byte) {
	if b == nil {
		w.null()
		return
	}
	w.u8(tcArray)
	w.writeDesc(descByteArr)
	w.assign()
	w.i32(int32(len(b)))
	w.b.Write(b)
}

// EncodeUploadResponse serialises UploadTupleChunkResponse. chunkID "" encodes null; errMsg nil encodes null.
func EncodeUploadResponse(chunkID string, errMsg *string, success bool) []byte {
	w := newWriter()
	w.newObject(descResponse)
	if success {
		w.u8(1)
	} else {
		w.u8(0)
	}
	w.uuid(chunkID)
	if errMsg == nil {
		w.null()
	} else {
		w.str(*errMsg, false)
	}
	return w.b.Bytes()
}

// EncodeTupleChunk serialises a TupleChunk (used by tests and Go upload tooling).
func EncodeTupleChunk(chunkID, tupleTypeName string, tuples []byte) []byte {
	w := newWriter()
	w.newObject(descTupleChunk)
	w.uuid(chunkID)
	w.enum(descTupleTypeEnum, tupleTypeName)
	w.byteArray(tuples)
	return w.b.Bytes()
}

// ReservationData is the codec-level form of a Reservation.
type ReservationData struct {
	ID       string
	Type     string // TupleType enum name
	Status   string // ActivationStatus enum name
	Elements []ReservationElementData
}

type ReservationElementData struct {
	ChunkID  string
	Reserved int64
	Start    int64
}

// EncodeReservation serialises a Reservation exactly as Java does (reservations as ArrayList).
func EncodeReservation(r ReservationData) []byte {
	w := newWriter()
	w.newObject(descReservation)
	w.str(r.ID, false)
	// List field
	w.newObject(descArrayList)
	w.i32(int32(len(r.Elements)))
	w.u8(tcBlockData)
	w.u8(4)
	w.i32(int32(len(r.Elements)))
	for _, e := range r.Elements {
		w.newObject(descResElement)
		w.i64(e.Reserved)
		w.i64(e.Start)
		w.uuid(e.ChunkID)
	}
	w.u8(tcEndBlock)
	w.enum(descStatusEnum, r.Status)
	w.enum(descTupleTypeEnum, r.Type)
	return w.b.Bytes()
}

// EncodeLong serialises a java.lang.Long.
func EncodeLong(v int64) []byte {
	w := newWriter()
	w.newObject(descLong)
	w.i64(v)
	return w.b.Bytes()
}
