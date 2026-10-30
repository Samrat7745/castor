package client

import (
	"encoding/json"
	"testing"
)

// FuzzTupleListJSON: decoding a download response never panics, and an accepted list is
// internally consistent (tuple count, share count per arity, equal value and MAC lengths).
func FuzzTupleListJSON(f *testing.F) {
	f.Add(`{"tupleCls":"Bit","field":{"@type":"Gfp","name":"gfp","elementSize":16},"tuples":[{"@type":"Bit","field":{"@type":"Gfp","name":"gfp","elementSize":16},"shares":[{"value":"AAAAAAAAAAAAAAAAAAAAAA==","mac":"AAAAAAAAAAAAAAAAAAAAAA=="}]}]}`)
	f.Add(`{"tupleCls":"Bit","field":{"@type":"Gfp","name":"gfp","elementSize":16},"tuples":[]}`)
	f.Add(`{"tupleCls":"Nope"}`)
	f.Add(`{"tupleCls":"MultiplicationTriple","field":{},"tuples":[{"@type":"MultiplicationTriple"}]}`)
	f.Add(`[]`)
	f.Add(`{"tupleCls":"Bit","field":{"@type":"Gf2n","name":"gf2n","elementSize":8},"tuples":[{"@type":"Bit","field":{"@type":"Gf2n","name":"gf2n","elementSize":8},"shares":[{"value":"!!","mac":"AA=="}]}]}`)
	f.Fuzz(func(t *testing.T, in string) {
		var l TupleList
		if err := json.Unmarshal([]byte(in), &l); err != nil {
			return
		}
		for _, tu := range l.Tuples {
			arity, ok := arityOf(tu.Type)
			if !ok || len(tu.Shares) != arity {
				t.Fatalf("inconsistent tuple %+v", tu)
			}
			for _, s := range tu.Shares {
				if len(s.Value) != len(s.Mac) {
					t.Fatal("share length mismatch accepted")
				}
			}
		}
		if _, err := json.Marshal(&l); err != nil {
			t.Fatal(err)
		}
	})
}

// FuzzParseStomp: the upload client's frame parser (applied to whatever the server sends) never panics.
func FuzzParseStomp(f *testing.F) {
	f.Add([]byte("MESSAGE\ndestination:/queue/response\ncontent-length:3\n\nabc\x00"))
	f.Add([]byte("\n\nCONNECTED\nversion:1.2\n\n\x00"))
	f.Add([]byte("MESSAGE\ncontent-length:-1\n\nabc\x00"))
	f.Add([]byte("MESSAGE\ncontent-length:99\n\nabc\x00"))
	f.Add([]byte("ERROR"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		fr, err := parseStomp(raw)
		if err == nil {
			_ = fr.encode()
		}
	})
}
