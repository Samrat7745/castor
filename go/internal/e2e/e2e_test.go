// Package e2e runs the upload/activate/download flow against live Castor instances (Go or Java).
// Set E2E_MASTER and E2E_SLAVE (base URLs); the test is skipped otherwise.
package e2e

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/carbynestack/castor-go/internal/javaser"
	"github.com/carbynestack/castor-go/internal/model"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

func ws(t *testing.T, base string) *websocket.Conn {
	url := "ws" + strings.TrimPrefix(base, "http") + "/intra-vcp/ws"
	d := websocket.Dialer{Subprotocols: []string{"v12.stomp"}}
	c, _, err := d.Dial(url, http.Header{"Authorization": {"Bearer x"}})
	if err != nil {
		t.Fatal(err)
	}
	_ = c.WriteMessage(websocket.TextMessage, []byte("CONNECT\naccept-version:1.2\nheart-beat:10000,0\n\n\x00"))
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, d2, err := c.ReadMessage()
	if err != nil || !bytes.HasPrefix(d2, []byte("CONNECTED")) {
		t.Fatalf("connect: %q %v", d2, err)
	}
	_ = c.WriteMessage(websocket.TextMessage, []byte("SUBSCRIBE\nid:sub-0\ndestination:/queue/response\n\n\x00"))
	time.Sleep(300 * time.Millisecond)
	return c
}

func upload(t *testing.T, base, chunk, typ string, data []byte) *javaser.UploadResponse {
	c := ws(t, base)
	defer c.Close()
	payload := javaser.EncodeTupleChunk(chunk, typ, data)
	frame := append([]byte(fmt.Sprintf("SEND\ndestination:/app/tuples/upload\ncontent-type:application/octet-stream\ncontent-length:%d\n\n", len(payload))), payload...)
	frame = append(frame, 0)
	if err := c.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		t.Fatal(err)
	}
	for {
		_ = c.SetReadDeadline(time.Now().Add(20 * time.Second))
		_, m, err := c.ReadMessage()
		if err != nil {
			t.Fatal("no response:", err)
		}
		i := bytes.Index(m, []byte("\n\n"))
		if !bytes.HasPrefix(m, []byte("MESSAGE")) || i < 0 {
			continue
		}
		body := bytes.TrimSuffix(m[i+2:], []byte{0})
		r, err := javaser.DecodeUploadResponse(body)
		if err != nil {
			t.Fatal(err)
		}
		if r.ChunkID == chunk || r.ChunkID == "" {
			return r
		}
	}
}

func req(t *testing.T, method, url, body string) (int, string) {
	r, _ := http.NewRequest(method, url, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestFlow(t *testing.T) {
	master, slave := os.Getenv("E2E_MASTER"), os.Getenv("E2E_SLAVE")
	if master == "" || slave == "" {
		t.Skip("E2E_MASTER / E2E_SLAVE not set")
	}
	tt, _ := model.ParseTupleType("MULTIPLICATION_TRIPLE_GFP")
	const n = 2500
	chunk := uuid.NewString()
	dataM, dataS := make([]byte, n*tt.TupleSize()), make([]byte, n*tt.TupleSize())
	_, _ = rand.Read(dataM)
	_, _ = rand.Read(dataS)

	for _, x := range []struct {
		base string
		data []byte
	}{{master, dataM}, {slave, dataS}} {
		if r := upload(t, x.base, chunk, tt.Name(), x.data); !r.Success || r.ChunkID != chunk {
			msg := ""
			if r.ErrorMsg != nil {
				msg = *r.ErrorMsg
			}
			t.Fatalf("upload to %s failed: id=%s ok=%v msg=%q", x.base, r.ChunkID, r.Success, msg)
		}
		if c, b := req(t, "PUT", x.base+"/intra-vcp/tuple-chunks/activate/"+chunk, ""); c != 200 {
			t.Fatal("activate", c, b)
		}
	}
	// duplicate upload must report failure
	if r := upload(t, master, chunk, tt.Name(), dataM); r.Success {
		t.Fatal("duplicate upload must fail")
	}
	if c, b := req(t, "PUT", master+"/intra-vcp/tuple-chunks/activate/"+uuid.NewString(), ""); c != 500 || !strings.Contains(b, "Not a single fragment") {
		t.Fatal(c, b)
	}

	// download 1200 tuples concurrently from both (slave waits for master)
	rid := uuid.NewString()
	const count = 1200
	var wg sync.WaitGroup
	res := map[string]string{}
	var mu sync.Mutex
	for name, base := range map[string]string{"master": master, "slave": slave} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, b := req(t, "GET", fmt.Sprintf("%s/intra-vcp/tuples?tupletype=%s&count=%d&reservationId=%s", base, tt.Name(), count, rid), "")
			if c != 200 {
				t.Errorf("%s download: %d %s", name, c, b)
			}
			mu.Lock()
			res[name] = b
			mu.Unlock()
		}()
	}
	wg.Wait()
	for name, data := range map[string][]byte{"master": dataM, "slave": dataS} {
		var want bytes.Buffer
		_ = model.WriteTupleListJSON(&want, tt, data[:count*tt.TupleSize()])
		if res[name] != want.String() {
			var a, b any
			_ = json.Unmarshal([]byte(res[name]), &a)
			_ = json.Unmarshal(want.Bytes(), &b)
			same := fmt.Sprint(a) == fmt.Sprint(b)
			t.Errorf("%s: response differs from expected (semantically equal: %v, len %d vs %d)", name, same, len(res[name]), want.Len())
		}
	}
	// second download takes the remainder: 1300 left
	rid2 := uuid.NewString()
	for name, base := range map[string]string{"master": master, "slave": slave} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, b := req(t, "GET", fmt.Sprintf("%s/intra-vcp/tuples?tupletype=%s&count=%d&reservationId=%s", base, tt.Name(), 1300, rid2), "")
			if c != 200 {
				t.Errorf("%s download2: %d %s", name, c, b)
			}
		}()
	}
	wg.Wait()
	// nothing left -> insufficient
	c, b := req(t, "GET", fmt.Sprintf("%s/intra-vcp/tuples?tupletype=%s&count=1&reservationId=%s", master, tt.Name(), uuid.NewString()), "")
	if c != 500 || !strings.Contains(b, "Insufficient Tuples of type multiplicationtriple_gfp available (0 out of 1).") {
		t.Fatal(c, b)
	}
	// telemetry
	c, b = req(t, "GET", master+"/intra-vcp/telemetry?interval=3600", "")
	var td model.TelemetryData
	if c != 200 || json.Unmarshal([]byte(b), &td) != nil || len(td.Metrics) != 10 || td.Interval != 3600000 {
		t.Fatal(c, b)
	}
	for _, m := range td.Metrics {
		if m.Type == tt.Name() && m.ConsumptionRate < 1 {
			t.Fatalf("expected consumption >= 1/s? got %+v (2500 tuples / 3600s rounds to 1)", m)
		}
	}
	// bad requests
	if c, _ := req(t, "GET", master+"/intra-vcp/tuples?tupletype=NOPE&count=1&reservationId="+uuid.NewString(), ""); c != 400 {
		t.Fatal(c)
	}
}
