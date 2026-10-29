package model

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"strconv"
)

// TelemetryData / TupleMetric JSON shapes.
type TupleMetric struct {
	Available       int64  `json:"available"`
	ConsumptionRate int64  `json:"consumptionRate"`
	Type            string `json:"type"`
}

type TelemetryData struct {
	Metrics  []TupleMetric `json:"metrics"`
	Interval int64         `json:"interval"`
}

// WriteTupleListJSON renders raw tuple bytes (concatenated chunks of whole tuples)
// as the Jackson TupleList shape, byte-compatible with the Java serializer.
func WriteTupleListJSON(w io.Writer, tt TupleType, data []byte) error {
	ts := tt.TupleSize()
	if ts == 0 || len(data)%ts != 0 {
		return fmt.Errorf("tuple data length %d not multiple of %d", len(data), ts)
	}
	es := tt.Field.ElementSize
	var b bytes.Buffer
	field := `{"@type":"` + tt.Field.Type + `","name":"` + tt.Field.Name + `","elementSize":` + strconv.Itoa(es) + `}`
	b.WriteString(`{"tupleCls":"` + tt.Cls + `","field":` + field + `,"tuples":[`)
	enc := base64.StdEncoding
	buf := make([]byte, enc.EncodedLen(es))
	for off := 0; off < len(data); off += ts {
		if off > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"@type":"` + tt.Cls + `","field":` + field + `,"shares":[`)
		for s := 0; s < tt.Arity; s++ {
			if s > 0 {
				b.WriteByte(',')
			}
			base := off + s*es*2
			b.WriteString(`{"value":"`)
			enc.Encode(buf, data[base:base+es])
			b.Write(buf)
			b.WriteString(`","mac":"`)
			enc.Encode(buf, data[base+es:base+2*es])
			b.Write(buf)
			b.WriteString(`"}`)
		}
		b.WriteString(`]}`)
	}
	b.WriteString(`]}`)
	_, err := w.Write(b.Bytes())
	return err
}
