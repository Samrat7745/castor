package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
)

// Messages match DefaultCastorInterVcpClient.
const (
	FailedSharing  = "Failed sharing Reservation."
	FailedUpdating = "Failed sending reservation update."
)

// InterVcpClient talks to the slave Castor services (master side).
type InterVcpClient interface {
	// ShareReservation POSTs the reservation to every slave; false if any slave answered non-2xx.
	ShareReservation(ctx context.Context, r *Reservation) (bool, error)
	// UpdateReservationStatus PUTs the status to every slave.
	UpdateReservationStatus(ctx context.Context, reservationID string, status ActivationStatus) error
}

type inter struct {
	opt   Options
	bases []string
	http  *http.Client
}

// NewInterVcpClient builds the client for all addresses in o.ServiceAddresses.
func NewInterVcpClient(o Options) (InterVcpClient, error) {
	bases, err := o.bases()
	if err != nil {
		return nil, err
	}
	hc, err := o.httpClient()
	if err != nil {
		return nil, err
	}
	return &inter{opt: o, bases: bases, http: hc}, nil
}

func (c *inter) send(ctx context.Context, method, base, path string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.opt.auth(base, req.Header)
	return c.http.Do(req)
}

func (c *inter) ShareReservation(ctx context.Context, r *Reservation) (bool, error) {
	body, err := json.Marshal(r)
	if err != nil {
		return false, err
	}
	ok := true
	for _, b := range c.bases {
		resp, err := c.send(ctx, http.MethodPost, b, "/inter-vcp/reservation", body)
		if err != nil {
			return false, &ClientError{FailedSharing, err}
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			ok = false
		}
	}
	return ok, nil
}

func (c *inter) UpdateReservationStatus(ctx context.Context, id string, status ActivationStatus) error {
	body, _ := json.Marshal(string(status))
	for _, b := range c.bases {
		resp, err := c.send(ctx, http.MethodPut, b, "/inter-vcp/reservation/"+url.PathEscape(id), body)
		if err != nil {
			return &ClientError{FailedUpdating, err}
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return nil
}
