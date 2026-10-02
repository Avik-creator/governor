// Package metrics exposes what governord is doing to Prometheus.
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/Avik-creator/governor/internal/core"
)

// Buckets are chosen for what is measured: a call can queue for seconds, a commit should not.
var (
	requestBuckets = []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
	commitBuckets  = []float64{.00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5}
	batchBuckets   = prometheus.ExponentialBuckets(1, 2, 11)
)

// Metrics holds every metric of one governord, in a registry of its own.
type Metrics struct {
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	events   *prometheus.CounterVec
	commit   prometheus.Histogram
	batch    prometheus.Histogram
}

// New returns the metrics of one governord; nothing is shared with other instances in the process.
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "governor_grpc_requests_total",
			Help: "Calls that have finished, by method and gRPC status code.",
		}, []string{"method", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "governor_grpc_request_duration_seconds",
			Help:    "How long unary calls took, including the commit and, for Acquire, the time queued.",
			Buckets: requestBuckets,
		}, []string{"method"}),
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "governor_events_total",
			Help: "State changes the engine has made, by kind.",
		}, []string{"kind"}),
		commit: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "governor_commit_duration_seconds",
			Help:    "How long each transaction that records events took.",
			Buckets: commitBuckets,
		}),
		batch: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "governor_commit_batch_events",
			Help:    "How many events each transaction recorded.",
			Buckets: batchBuckets,
		}),
	}
	m.registry.MustRegister(
		m.requests, m.duration, m.events, m.commit, m.batch,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Source is what the state of the tree is read from; *core.Engine is one.
type Source interface {
	Stats() core.Stats
}

// Watch adds the state of the tree to the metrics, read from src on every scrape.
func (m *Metrics) Watch(src Source) error {
	if err := m.registry.Register(collector{src}); err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	return nil
}

// Handler serves the metrics in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry})
}

// Sink returns a sink that counts every event and passes it on to next, which may be nil.
func (m *Metrics) Sink(next core.Sink) core.Sink {
	return sink{events: m.events, next: next}
}

// sink counts events on their way to the record.
type sink struct {
	events *prometheus.CounterVec
	next   core.Sink
}

// Emit runs under the engine's lock, so it only bumps a counter.
func (s sink) Emit(ev core.Event) {
	s.events.WithLabelValues(string(ev.Kind)).Inc()
	if s.next != nil {
		s.next.Emit(ev)
	}
}

// ObserveCommit records one committed transaction of the store.
func (m *Metrics) ObserveCommit(events int, took time.Duration) {
	m.commit.Observe(took.Seconds())
	m.batch.Observe(float64(events))
}

// observe counts one finished call; the method is named without its service.
func (m *Metrics) observe(fullMethod string, err error) string {
	method := path.Base(fullMethod)
	m.requests.WithLabelValues(method, status.Code(err).String()).Inc()
	return method
}

// UnaryInterceptor counts and times unary calls; it must be the outermost interceptor.
func (m *Metrics) UnaryInterceptor(
	ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	method := m.observe(info.FullMethod, err)
	m.duration.WithLabelValues(method).Observe(time.Since(start).Seconds())
	return resp, err
}

// StreamInterceptor counts streams when they end; they are not timed, as a watch lasts as long as its node.
func (m *Metrics) StreamInterceptor(
	srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	err := handler(srv, ss)
	m.observe(info.FullMethod, err)
	return err
}
