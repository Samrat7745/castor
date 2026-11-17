package client

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Samrat7745/castor/go/internal/model"
)

// The decoders below mirror the Jackson mapping of the Java entities: required creator
// properties, the @type discriminators and the invariants enforced by the Java constructors.

func missing(name string) error {
	return fmt.Errorf("Missing required creator property '%s'", name)
}

// UnmarshalJSON decodes {"@type":"Gfp"|"Gf2n","name":...,"elementSize":...}; name and elementSize are required.
func (f *Field) UnmarshalJSON(b []byte) error {
	var raw struct {
		Type        *string `json:"@type"`
		Name        *string `json:"name"`
		ElementSize *int    `json:"elementSize"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if raw.Type == nil {
		return errors.New("Could not resolve subtype of Field: missing type id property '@type'")
	}
	if *raw.Type != model.GFP.Type && *raw.Type != model.GF2N.Type {
		return fmt.Errorf("Could not resolve type id '%s' as a subtype of Field", *raw.Type)
	}
	if raw.Name == nil {
		return missing("name")
	}
	if raw.ElementSize == nil {
		return missing("elementSize")
	}
	*f = Field{Type: *raw.Type, Name: *raw.Name, ElementSize: *raw.ElementSize}
	return nil
}

// ErrShareLength is Share.DATA_MUST_BE_SAME_LENGTH_EXCEPTION_MESSAGE.
const ErrShareLength = "Value and mac must be of same length."

// UnmarshalJSON decodes {"value":base64,"mac":base64}; both are required and must have the same length.
func (s *Share) UnmarshalJSON(b []byte) error {
	var raw struct {
		Value *[]byte `json:"value"`
		Mac   *[]byte `json:"mac"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if raw.Value == nil {
		return missing("value")
	}
	if raw.Mac == nil {
		return missing("mac")
	}
	if len(*raw.Value) != len(*raw.Mac) {
		return errors.New(ErrShareLength)
	}
	s.Value, s.Mac = *raw.Value, *raw.Mac
	return nil
}

// UnmarshalJSON decodes a tuple, checking that @type is a known tuple class and that the number
// of shares matches its arity (ArrayBackedTuple.INVALID_NUMBER_OF_SHARES_EXCEPTION_MSG).
func (t *Tuple) UnmarshalJSON(b []byte) error {
	var raw struct {
		Type   *string  `json:"@type"`
		Field  *Field   `json:"field"`
		Shares *[]Share `json:"shares"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if raw.Type == nil {
		return errors.New("Could not resolve subtype of Tuple: missing type id property '@type'")
	}
	arity, ok := arityOf(*raw.Type)
	if !ok {
		return fmt.Errorf("Could not resolve type id '%s' as a subtype of Tuple", *raw.Type)
	}
	if raw.Field == nil {
		return missing("field")
	}
	if raw.Shares == nil {
		return missing("shares")
	}
	if len(*raw.Shares) != arity {
		return fmt.Errorf("Invalid number of shares: %s expects exactly %d shares.", *raw.Type, arity)
	}
	*t = Tuple{Type: *raw.Type, Field: *raw.Field, Shares: *raw.Shares}
	return nil
}

func arityOf(cls string) (int, bool) {
	for _, tt := range model.TupleTypes {
		if tt.Cls == cls {
			return tt.Arity, true
		}
	}
	return 0, false
}

// UnmarshalJSON decodes the GET /intra-vcp/tuples response (TupleListDeserializer).
func (l *TupleList) UnmarshalJSON(b []byte) error {
	var raw struct {
		TupleCls *string  `json:"tupleCls"`
		Field    *Field   `json:"field"`
		Tuples   *[]Tuple `json:"tuples"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if raw.TupleCls == nil {
		return missing("tupleCls")
	}
	if _, ok := arityOf(*raw.TupleCls); !ok {
		return fmt.Errorf("io.carbynestack.castor.common.entities.%s", *raw.TupleCls)
	}
	if raw.Field == nil {
		return missing("field")
	}
	if raw.Tuples == nil {
		return missing("tuples")
	}
	*l = TupleList{TupleCls: *raw.TupleCls, Field: *raw.Field, Tuples: *raw.Tuples}
	return nil
}
