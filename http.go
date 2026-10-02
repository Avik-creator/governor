package governor

import (
	"context"
	"io"
	"net/http"
	"time"
)

// Names the HTTP transport charges and holds; configure caps under these names.
const (
	ResourceHTTP = "http"
	ClassHTTP    = "http"
)

// maxHold ends a lease whose holder forgot it, such as a response body never closed.
const maxHold = 5 * time.Minute

// Transport returns a RoundTripper that governs every request; nil wraps http.DefaultTransport.
func Transport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &transport{base: base}
}

type transport struct {
	base http.RoundTripper
}

// RoundTrip charges one http unit and holds an http lease until the response body is closed.
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	lease, err := admit(ctx)
	if err != nil {
		// A RoundTripper must close the request body even when it sends nothing.
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, err
	}
	start := time.Now()
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		_ = lease.Release(Report{})
		return nil, err
	}
	// The time to the response headers is what tells a slow server from a large body.
	report := Report{
		Latency:    time.Since(start),
		Overloaded: resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable,
	}
	resp.Body = &leasedBody{ReadCloser: resp.Body, lease: lease, report: report}
	return resp, nil
}

// admit charges the request to the task and waits for a lease to send it.
func admit(ctx context.Context) (*Lease, error) {
	if err := Consume(ctx, ResourceHTTP, 1); err != nil {
		return nil, err
	}
	return Acquire(ctx, ClassHTTP, WithMaxHold(maxHold))
}

// leasedBody returns its lease when the response body is closed.
type leasedBody struct {
	io.ReadCloser
	lease  *Lease
	report Report
}

func (b *leasedBody) Close() error {
	err := b.ReadCloser.Close()
	_ = b.lease.Release(b.report)
	return err
}
