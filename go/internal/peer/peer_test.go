package peer

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Samrat7745/castor/go/internal/model"
)

type rec struct{ method, path, body, ct, auth string }

func server(t *testing.T, status int, out *[]rec, mu *sync.Mutex) *httptest.Server {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		*out = append(*out, rec{r.Method, r.URL.Path, string(b), r.Header.Get("Content-Type"), r.Header.Get("Authorization")})
		mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(s.Close)
	return s
}

func resv() *model.Reservation {
	return &model.Reservation{ReservationID: "x_bit_gfp", TupleType: "BIT_GFP", Status: model.Locked,
		Reservations: []model.ReservationElement{{TupleChunkID: "123e4567-e89b-12d3-a456-426614174000", ReservedTuples: 1}}}
}

func TestShareAndUpdate(t *testing.T) {
	var got []rec
	var mu sync.Mutex
	s1, s2 := server(t, 201, &got, &mu), server(t, 500, &got, &mu)
	c, err := New([]string{s1.URL + "/", s2.URL}, false, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := c.ShareReservation(context.Background(), resv())
	if err != nil || ok {
		t.Fatalf("want false,nil got %v %v", ok, err)
	}
	if len(got) != 2 || got[0].path != "/inter-vcp/reservation" || got[0].method != "POST" || got[0].ct != "application/json" || got[0].auth != "" {
		t.Fatalf("%+v", got)
	}
	got = nil
	if err := c.UpdateReservationStatus(context.Background(), "x_bit_gfp", model.Unlocked); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].method != "PUT" || got[0].path != "/inter-vcp/reservation/x_bit_gfp" || got[0].body != `"UNLOCKED"` {
		t.Fatalf("%+v", got)
	}
}

func TestTransportErrors(t *testing.T) {
	c, _ := New([]string{"http://127.0.0.1:1"}, false, nil, 300*time.Millisecond)
	_, err := c.ShareReservation(context.Background(), resv())
	var ce *ClientError
	if !asClient(err, &ce) || ce.Msg != FailedSharing {
		t.Fatal(err)
	}
	err = c.UpdateReservationStatus(context.Background(), "a", model.Unlocked)
	if !asClient(err, &ce) || ce.Msg != FailedUpdating {
		t.Fatal(err)
	}
	if _, err := New(nil, false, nil, time.Second); err == nil {
		t.Fatal("empty slaves")
	}
}

func asClient(err error, t **ClientError) bool {
	c, ok := err.(*ClientError)
	*t = c
	return ok
}

// DefaultCastorInterVcpClientTest on the service's own peer client: every slave is asked even if
// one fails, only the ones answering 2xx count as success, and transport errors carry their cause.
func TestShareReservationAsksEverySlave(t *testing.T) {
	for _, c := range []struct {
		name         string
		first, last  int
		wantAccepted bool
	}{{"all succeed", 201, 200, true}, {"second fails", 201, 404, false}, {"first fails", 404, 201, false}} {
		var got []rec
		var mu sync.Mutex
		s1, s2 := server(t, c.first, &got, &mu), server(t, c.last, &got, &mu)
		cl, err := New([]string{s1.URL, s2.URL}, false, nil, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		ok, err := cl.ShareReservation(context.Background(), resv())
		if err != nil || ok != c.wantAccepted || len(got) != 2 {
			t.Fatalf("%s: ok=%v err=%v calls=%d", c.name, ok, err, len(got))
		}
	}
}

func TestTransportErrorsCarryTheirCause(t *testing.T) {
	c, _ := New([]string{"http://127.0.0.1:1"}, false, nil, 300*time.Millisecond)
	_, err := c.ShareReservation(context.Background(), resv())
	var ce *ClientError
	if !asClient(err, &ce) || ce.Unwrap() == nil {
		t.Fatalf("%#v", err)
	}
	if err = c.UpdateReservationStatus(context.Background(), "a", model.Unlocked); !asClient(err, &ce) || ce.Unwrap() == nil {
		t.Fatalf("%#v", err)
	}
}

func TestEveryDeclaredSlaveReceivesTheStatusUpdate(t *testing.T) {
	var got []rec
	var mu sync.Mutex
	s1, s2 := server(t, 200, &got, &mu), server(t, 200, &got, &mu)
	c, _ := New([]string{s1.URL, s2.URL}, false, nil, time.Second)
	if err := c.UpdateReservationStatus(context.Background(), "reservationId", model.Unlocked); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].path != "/inter-vcp/reservation/reservationId" || got[1].path != got[0].path || got[0].body != `"UNLOCKED"` || got[1].body != `"UNLOCKED"` {
		t.Fatalf("%+v", got)
	}
}
