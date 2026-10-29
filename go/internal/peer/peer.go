// Package peer is the master -> slave inter-VCP REST client.
package peer

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/carbynestack/castor-go/internal/model"
)

// Messages match DefaultCastorInterVcpClient; both are CastorClientException in Java.
const (
	FailedSharing  = "Failed sharing Reservation."
	FailedUpdating = "Failed sending reservation update."
)

// ClientError marks transport-level failures (Java CastorClientException).
type ClientError struct {
	Msg   string
	Cause error
}

func (e *ClientError) Error() string { return e.Msg }
func (e *ClientError) Unwrap() error { return e.Cause }

// Client talks to the slave services. No Authorization header is sent (parity with Java).
type Client struct {
	slaves []string
	http   *http.Client
}

// New builds a client. trustedCerts are PEM file paths added to the system roots.
func New(slaveURIs []string, noSSLValidation bool, trustedCerts []string, timeout time.Duration) (*Client, error) {
	if len(slaveURIs) == 0 {
		return nil, errors.New("Declared service URIs must not be empty")
	}
	var bases []string
	for _, s := range slaveURIs {
		u, err := url.Parse(s)
		if err != nil || u.Hostname() == "" {
			return nil, fmt.Errorf("Invalid service address: %q", s)
		}
		scheme := "http"
		if u.Scheme == "https" || u.Scheme == "wss" {
			scheme = "https"
		}
		u.Scheme = scheme
		bases = append(bases, strings.TrimSuffix(u.String(), "/"))
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	for _, p := range trustedCerts {
		pem, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("reading trusted certificate %s: %w", p, err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in %s", p)
		}
	}
	tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, InsecureSkipVerify: noSSLValidation}, MaxIdleConnsPerHost: 8}
	return &Client{slaves: bases, http: &http.Client{Transport: tr, Timeout: timeout}}, nil
}

// ShareReservation POSTs the reservation to every slave; returns false if any answered non-2xx.
func (c *Client) ShareReservation(ctx context.Context, r *model.Reservation) (bool, error) {
	body, err := json.Marshal(r)
	if err != nil {
		return false, err
	}
	ok := true
	for _, s := range c.slaves {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, s+"/inter-vcp/reservation", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
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

// UpdateReservationStatus PUTs the JSON status string to every slave; response codes are not inspected.
func (c *Client) UpdateReservationStatus(ctx context.Context, id string, status model.ActivationStatus) error {
	body, _ := json.Marshal(string(status))
	for _, s := range c.slaves {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, s+"/inter-vcp/reservation/"+url.PathEscape(id), bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			return &ClientError{FailedUpdating, err}
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return nil
}
