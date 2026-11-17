package client

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Samrat7745/castor/go/internal/model"
)

// Ports of the castor-common ShareTest, FieldTest, ArrayBackedTupleTest and TupleListTest: the Go
// client decodes the same JSON the Jackson (de)serializers of the Java entities exchange.

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func TestShareJSON(t *testing.T) {
	var s Share
	// value and mac of different length (either way round) are rejected
	for _, j := range []string{
		`{"value":"` + b64(make([]byte, 16)) + `","mac":"` + b64(make([]byte, 1)) + `"}`,
		`{"value":"` + b64(make([]byte, 1)) + `","mac":"` + b64(make([]byte, 16)) + `"}`,
	} {
		if err := json.Unmarshal([]byte(j), &s); err == nil || err.Error() != ErrShareLength {
			t.Fatalf("%s: %v", j, err)
		}
	}
	v, m := randBytes(model.GFP.ElementSize), randBytes(model.GFP.ElementSize)
	if err := json.Unmarshal([]byte(`{"value":"`+b64(v)+`","mac":"`+b64(m)+`"}`), &s); err != nil || !bytes.Equal(s.Value, v) || !bytes.Equal(s.Mac, m) {
		t.Fatal(err, s)
	}
	for _, c := range []struct{ j, prop string }{{`{"mac":"AA=="}`, "value"}, {`{"value":"AA=="}`, "mac"}} {
		if err := json.Unmarshal([]byte(c.j), &Share{}); err == nil || !strings.HasPrefix(err.Error(), "Missing required creator property '"+c.prop+"'") {
			t.Fatalf("%s: %v", c.j, err)
		}
	}
}

func TestFieldJSON(t *testing.T) {
	for _, c := range []struct {
		name, typ string
		f         model.Field
	}{{"gfp", "Gfp", model.GFP}, {"gf2n", "Gf2n", model.GF2N}} {
		var got Field
		j := fmt.Sprintf(`{"@type":"%s","name":"%s","elementSize":%d}`, c.typ, c.f.Name, c.f.ElementSize)
		if err := json.Unmarshal([]byte(j), &got); err != nil || got != (Field{c.f.Type, c.f.Name, c.f.ElementSize}) {
			t.Fatalf("%s: %v %+v", c.name, err, got)
		}
	}
	for _, c := range []struct{ j, want string }{
		{`{"@type":"Gf2n","elementSize":16}`, "Missing required creator property 'name'"},
		{`{"@type":"Gfp","name":"gf2n"}`, "Missing required creator property 'elementSize'"},
		{`{"name":"gfp","elementSize":16}`, "Could not resolve subtype of Field"},
		{`{"@type":"Gfq","name":"gfp","elementSize":16}`, "Could not resolve type id 'Gfq' as a subtype of Field"},
	} {
		if err := json.Unmarshal([]byte(c.j), &Field{}); err == nil || !strings.HasPrefix(err.Error(), c.want) {
			t.Fatalf("%s: %v", c.j, err)
		}
	}
}

func list(t *testing.T, tt model.TupleType, data []byte) (string, *TupleList) {
	t.Helper()
	var b bytes.Buffer
	if err := model.WriteTupleListJSON(&b, tt, data); err != nil {
		t.Fatal(err)
	}
	var l TupleList
	if err := json.Unmarshal(b.Bytes(), &l); err != nil {
		t.Fatalf("%s: %v\n%s", tt.Name(), err, b.String())
	}
	return b.String(), &l
}

// TupleListTest.givenValidInputStream_whenCreateFromStream
func TestTupleListFromTupleData(t *testing.T) {
	bit, _ := model.ParseTupleType("BIT_GFP")
	vals := [][]byte{randBytes(16), randBytes(16), randBytes(16), randBytes(16)}
	_, l := list(t, bit, bytes.Join(vals, nil))
	if l.TupleCls != "Bit" || l.Field.Name != "gfp" || len(l.Tuples) != 2 {
		t.Fatalf("%+v", l)
	}
	for i, tu := range l.Tuples {
		if tu.Type != "Bit" || len(tu.Shares) != 1 || !bytes.Equal(tu.Shares[0].Value, vals[2*i]) || !bytes.Equal(tu.Shares[0].Mac, vals[2*i+1]) {
			t.Fatalf("tuple %d: %+v", i, tu)
		}
	}
}

// TupleListTest.givenJsonDoesNotContainRequiredMacValue_whenDeserializeFromJson
func TestTupleListMissingMacIsRejected(t *testing.T) {
	v := b64(randBytes(128))
	f := `{"@type":"Gfp","name":"gfp","elementSize":128}`
	j := `{"tupleCls":"InputMask","field":` + f + `,"tuples":[{"@type":"InputMask","field":` + f + `,"shares":[{"value":"` + v + `","mac":"` + v + `"}]},` +
		`{"@type":"InputMask","field":` + f + `,"shares":[{"value":"` + v + `"}]}]}`
	if err := json.Unmarshal([]byte(j), &TupleList{}); err == nil || !strings.HasPrefix(err.Error(), "Missing required creator property 'mac'") {
		t.Fatal(err)
	}
}

// TupleListTest.givenValidTupleList_whenJsonSerializationRoundTrip / givenEmptyTupleList_...
func TestTupleListRoundTripAllTypes(t *testing.T) {
	for _, tt := range model.TupleTypes {
		for _, n := range []int{2, 0} {
			data := randBytes(tt.TupleSize() * n)
			js, l := list(t, tt, data)
			if l.TupleCls != tt.Cls || l.Field != (Field{tt.Field.Type, tt.Field.Name, tt.Field.ElementSize}) || len(l.Tuples) != n {
				t.Fatalf("%s: %+v", tt.Name(), l)
			}
			var back []byte
			for _, tu := range l.Tuples {
				if len(tu.Shares) != tt.Arity {
					t.Fatalf("%s: %d shares", tt.Name(), len(tu.Shares))
				}
				for _, s := range tu.Shares {
					back = append(append(back, s.Value...), s.Mac...)
				}
			}
			if !bytes.Equal(back, data) && (len(back) != 0 || len(data) != 0) {
				t.Fatalf("%s: share data differs after round trip", tt.Name())
			}
			// and re-encoding yields the exact bytes the service wrote
			if out, err := json.Marshal(l); err != nil || string(out) != js {
				t.Fatalf("%s: re-marshal differs\n%s\n%s", tt.Name(), out, js)
			}
		}
	}
}

// ArrayBackedTupleTest.givenInvalidNumberOfShares_whenCreateNewTuple
func TestTupleWithWrongNumberOfSharesIsRejected(t *testing.T) {
	f := `{"@type":"Gfp","name":"gfp","elementSize":16}`
	err := json.Unmarshal([]byte(`{"@type":"Bit","field":`+f+`,"shares":[]}`), &Tuple{})
	if err == nil || err.Error() != "Invalid number of shares: Bit expects exactly 1 shares." {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"@type":"Foo","field":`+f+`,"shares":[]}`), &Tuple{}); err == nil || !strings.Contains(err.Error(), "'Foo'") {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"tupleCls":"Foo","field":`+f+`,"tuples":[]}`), &TupleList{}); err == nil || err.Error() != "io.carbynestack.castor.common.entities.Foo" {
		t.Fatal(err)
	}
}
