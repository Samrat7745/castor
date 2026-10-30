package client

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/carbynestack/castor-go/internal/model"
)

// Wire types shared with the service (aliases keep the client and the service in lock step).
type (
	Reservation      = model.Reservation
	ActivationStatus = model.ActivationStatus
	TelemetryData    = model.TelemetryData
	TupleMetric      = model.TupleMetric
	TupleType        = model.TupleType
)

// ParseTupleType is Java TupleType.valueOf, looked up by enum name (e.g. "MULTIPLICATION_TRIPLE_GFP").
func ParseTupleType(name string) (TupleType, error) { return model.ParseTupleType(name) }

// ClientError is the Go counterpart of Java's CastorClientException.
type ClientError struct {
	Msg   string
	Cause error
}

func (e *ClientError) Error() string {
	if e.Cause == nil {
		return e.Msg
	}
	return e.Msg + ": " + e.Cause.Error()
}
func (e *ClientError) Unwrap() error { return e.Cause }

// BearerTokenProvider returns the bearer token for a service address (Java BearerTokenProvider).
type BearerTokenProvider func(serviceAddress string) string

// Options configure a client (Java ClientBuilderSupport).
type Options struct {
	// ServiceAddresses are the Castor base URIs; intra-VCP clients use the first, inter-VCP clients all of them.
	ServiceAddresses []string
	NoSSLValidation  bool
	// TrustedCertificates are PEM file paths added to the system roots.
	TrustedCertificates []string
	BearerTokenProvider BearerTokenProvider
	Timeout             time.Duration
}

const invalidAddressMsg = "Invalid service address.\nAddress must match the following examples:\n\t1. http://server:port\n\t2. https://server:port/path\n\t3. ws://server\n\t4. wss://server/path\n"

// restBase normalises a service address like CastorServiceUri: ws(s) becomes http(s), no trailing slash.
func restBase(addr string) (string, error) {
	if strings.TrimSpace(addr) == "" {
		return "", errors.New("serviceAddress must not be empty!")
	}
	u, err := url.Parse(addr)
	if err != nil || u.Hostname() == "" {
		return "", errors.New(invalidAddressMsg)
	}
	if u.Scheme == "https" || u.Scheme == "wss" {
		u.Scheme = "https"
	} else {
		u.Scheme = "http"
	}
	return strings.TrimSuffix(u.String(), "/"), nil
}

func (o Options) bases() ([]string, error) {
	if len(o.ServiceAddresses) == 0 {
		return nil, errors.New("Declared service URIs must not be empty")
	}
	out := make([]string, 0, len(o.ServiceAddresses))
	for _, a := range o.ServiceAddresses {
		b, err := restBase(a)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

func (o Options) httpClient() (*http.Client, error) {
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	for _, p := range o.TrustedCertificates {
		pem, err := os.ReadFile(p)
		if err != nil {
			return nil, &ClientError{"Failed to create CsHttpClient.", fmt.Errorf("reading trusted certificate %s: %w", p, err)}
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, &ClientError{"Failed to create CsHttpClient.", fmt.Errorf("no certificates found in %s", p)}
		}
	}
	to := o.Timeout
	if to == 0 {
		to = 60 * time.Second
	}
	tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, InsecureSkipVerify: o.NoSSLValidation}, MaxIdleConnsPerHost: 8}
	return &http.Client{Transport: tr, Timeout: to}, nil
}

func (o Options) auth(base string, h http.Header) {
	if o.BearerTokenProvider != nil {
		h.Set("Authorization", "Bearer "+o.BearerTokenProvider(base))
	}
}
