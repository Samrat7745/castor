package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/google/uuid"
)

// Messages match DefaultCastorIntraVcpClient.
const (
	FailedDownloadingTuples = "Failed downloading tuples from service %s: %s"
	FailedFetchingTelemetry = "Failed fetching telemetry data from service %s: %s"
)

// Field, Share, Tuple and TupleList are the JSON shapes of GET /intra-vcp/tuples.
type Field struct {
	Type        string `json:"@type"`
	Name        string `json:"name"`
	ElementSize int    `json:"elementSize"`
}

// Share is one secret share; value and MAC are base64 on the wire and plain bytes here.
type Share struct {
	Value []byte `json:"value"`
	Mac   []byte `json:"mac"`
}

// Tuple is one correlated-randomness tuple (arity depends on the type).
type Tuple struct {
	Type   string  `json:"@type"`
	Field  Field   `json:"field"`
	Shares []Share `json:"shares"`
}

// TupleList is the download response.
type TupleList struct {
	TupleCls string  `json:"tupleCls"`
	Field    Field   `json:"field"`
	Tuples   []Tuple `json:"tuples"`
}

// IntraVcpClient downloads tuple shares and telemetry from the local Castor service.
type IntraVcpClient interface {
	DownloadTupleShares(ctx context.Context, requestID uuid.UUID, tupleType TupleType, count int64) (*TupleList, error)
	GetTelemetryData(ctx context.Context) (*TelemetryData, error)
	GetTelemetryDataFor(ctx context.Context, intervalSeconds int64) (*TelemetryData, error)
}

type intra struct {
	opt  Options
	base string
	http *http.Client
}

// NewIntraVcpClient builds the client for the first address in o.ServiceAddresses.
func NewIntraVcpClient(o Options) (IntraVcpClient, error) {
	bases, err := o.bases()
	if err != nil {
		return nil, err
	}
	hc, err := o.httpClient()
	if err != nil {
		return nil, err
	}
	return &intra{opt: o, base: bases[0], http: hc}, nil
}

func (c *intra) get(ctx context.Context, rawURL string, out any, failMsg string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return &ClientError{fmt.Sprintf(failMsg, c.base, err.Error()), err}
	}
	req.Header.Set("Accept", "application/json")
	c.opt.auth(c.base, req.Header)
	resp, err := c.http.Do(req)
	if err != nil {
		return &ClientError{fmt.Sprintf(failMsg, c.base, err.Error()), err}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		e := fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
		return &ClientError{fmt.Sprintf(failMsg, c.base, e.Error()), e}
	}
	if err := json.Unmarshal(body, out); err != nil {
		return &ClientError{fmt.Sprintf(failMsg, c.base, err.Error()), err}
	}
	return nil
}

func (c *intra) DownloadTupleShares(ctx context.Context, requestID uuid.UUID, tupleType TupleType, count int64) (*TupleList, error) {
	q := url.Values{}
	q.Set("count", strconv.FormatInt(count, 10))
	q.Set("reservationId", requestID.String())
	q.Set("tupletype", tupleType.Name())
	var out TupleList
	if err := c.get(ctx, c.base+"/intra-vcp/tuples?"+q.Encode(), &out, FailedDownloadingTuples); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *intra) GetTelemetryData(ctx context.Context) (*TelemetryData, error) {
	var out TelemetryData
	if err := c.get(ctx, c.base+"/intra-vcp/telemetry", &out, FailedFetchingTelemetry); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *intra) GetTelemetryDataFor(ctx context.Context, interval int64) (*TelemetryData, error) {
	var out TelemetryData
	if err := c.get(ctx, c.base+"/intra-vcp/telemetry?interval="+strconv.FormatInt(interval, 10), &out, FailedFetchingTelemetry); err != nil {
		return nil, err
	}
	return &out, nil
}
