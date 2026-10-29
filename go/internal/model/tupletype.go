// Package model holds the Castor domain types and their wire (JSON) shapes.
package model

import "fmt"

// Field describes the algebraic field of a tuple.
type Field struct {
	Type        string // "Gfp" or "Gf2n" (Jackson @type)
	Name        string
	ElementSize int
}

var (
	GFP  = Field{"Gfp", "gfp", 16}
	GF2N = Field{"Gf2n", "gf2n", 8}
)

// TupleType mirrors the Java enum. Name() is the enum constant name, String() the tupleName.
type TupleType struct {
	name      string
	tupleName string
	Cls       string
	Field     Field
	Arity     int
}

// TupleTypes in Java declaration order.
var TupleTypes = []TupleType{
	{"BIT_GFP", "bit_gfp", "Bit", GFP, 1},
	{"BIT_GF2N", "bit_gf2n", "Bit", GF2N, 1},
	{"INPUT_MASK_GFP", "inputmask_gfp", "InputMask", GFP, 1},
	{"INPUT_MASK_GF2N", "inputmask_gf2n", "InputMask", GF2N, 1},
	{"INVERSE_TUPLE_GFP", "inversetuple_gfp", "InverseTuple", GFP, 2},
	{"INVERSE_TUPLE_GF2N", "inversetuple_gf2n", "InverseTuple", GF2N, 2},
	{"SQUARE_TUPLE_GFP", "squaretuple_gfp", "SquareTuple", GFP, 2},
	{"SQUARE_TUPLE_GF2N", "squaretuple_gf2n", "SquareTuple", GF2N, 2},
	{"MULTIPLICATION_TRIPLE_GFP", "multiplicationtriple_gfp", "MultiplicationTriple", GFP, 3},
	{"MULTIPLICATION_TRIPLE_GF2N", "multiplicationtriple_gf2n", "MultiplicationTriple", GF2N, 3},
}

// Name is the Java enum name(), used on REST, in PG and in JSON.
func (t TupleType) Name() string { return t.name }

// String is the Java toString(), used in reservation ids and Redis consumption keys.
func (t TupleType) String() string { return t.tupleName }

// ShareSize is value+mac bytes.
func (t TupleType) ShareSize() int { return t.Field.ElementSize * 2 }

// TupleSize is bytes per tuple.
func (t TupleType) TupleSize() int { return t.Arity * t.ShareSize() }

// Valid reports whether t is a real tuple type.
func (t TupleType) Valid() bool { return t.name != "" }

// ParseTupleType is Java TupleType.valueOf (by enum name).
func ParseTupleType(name string) (TupleType, error) {
	for _, t := range TupleTypes {
		if t.name == name {
			return t, nil
		}
	}
	return TupleType{}, fmt.Errorf("No enum constant io.carbynestack.castor.common.entities.TupleType.%s", name)
}

// ActivationStatus mirrors the Java enum.
type ActivationStatus string

const (
	Locked   ActivationStatus = "LOCKED"
	Unlocked ActivationStatus = "UNLOCKED"
)
