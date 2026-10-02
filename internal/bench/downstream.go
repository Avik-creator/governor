package bench

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"
)

// Downstream simulates a service with limited capacity: it slows as it fills and sheds load when full.
type Downstream struct {
	server   *httptest.Server
	latency  time.Duration
	capacity atomic.Int64
	failing  atomic.Bool

	inflight atomic.Int64
	peak     atomic.Int64
	requests atomic.Int64
	shed     atomic.Int64 // answered 503 because the service was full
}

// Counts is what a Downstream has seen so far.
type Counts struct {
	Requests int64
	Shed     int64
	Peak     int64
}

// NewDownstream starts a service that handles capacity requests at once, each taking about latency.
func NewDownstream(capacity int, latency time.Duration) *Downstream {
	d := &Downstream{latency: latency}
	d.capacity.Store(int64(capacity))
	d.server = httptest.NewServer(http.HandlerFunc(d.handle))
	return d
}

func (d *Downstream) handle(w http.ResponseWriter, _ *http.Request) {
	d.requests.Add(1)
	now := d.inflight.Add(1)
	defer d.inflight.Add(-1)
	for peak := d.peak.Load(); now > peak && !d.peak.CompareAndSwap(peak, now); peak = d.peak.Load() {
	}
	capacity := d.capacity.Load()
	switch {
	case d.failing.Load():
		w.WriteHeader(http.StatusInternalServerError)
	case now > capacity:
		// Turning a request away is not free: it costs the service about one unit of work.
		d.shed.Add(1)
		time.Sleep(d.latency)
		w.WriteHeader(http.StatusServiceUnavailable)
	default:
		// Work takes up to three times as long as the service approaches its capacity.
		load := float64(now) / float64(capacity)
		time.Sleep(time.Duration(float64(d.latency) * (1 + 2*load*load)))
	}
}

// URL returns the address of the service.
func (d *Downstream) URL() string {
	return d.server.URL
}

// SetCapacity changes how many requests the service handles at once.
func (d *Downstream) SetCapacity(n int) {
	d.capacity.Store(int64(n))
}

// SetFailing makes every request fail with 500, as an outage does.
func (d *Downstream) SetFailing(failing bool) {
	d.failing.Store(failing)
}

// Counts returns what the service has seen since it started.
func (d *Downstream) Counts() Counts {
	return Counts{Requests: d.requests.Load(), Shed: d.shed.Load(), Peak: d.peak.Load()}
}

// Close stops the service.
func (d *Downstream) Close() {
	d.server.Close()
}

// plainClient returns an HTTP client with no governance, for the ungoverned side of a scenario.
func plainClient() *http.Client {
	return &http.Client{Transport: pooled()}
}

// pooled returns a transport that keeps enough idle connections for a burst.
func pooled() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns, t.MaxIdleConnsPerHost = 256, 256
	return t
}
