package bench

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Avik-creator/governor"
	"github.com/Avik-creator/governor/internal/config"
	"github.com/Avik-creator/governor/internal/core"
)

// AdaptiveParams sizes the capacity drop scenario.
type AdaptiveParams struct {
	// Workers send requests back to back for Duration.
	Workers  int
	Duration time.Duration

	// The downstream's capacity falls from Before to After a third of the way in.
	Before, After int
	Latency       time.Duration

	// Backoff is how long a worker pauses after a request fails, as a real client would.
	Backoff time.Duration
}

// DefaultAdaptive has 40 workers hit a service whose capacity falls from 20 to 5.
var DefaultAdaptive = AdaptiveParams{
	Workers: 40, Duration: 6 * time.Second, Before: 20, After: 5,
	Latency: 5 * time.Millisecond, Backoff: 10 * time.Millisecond,
}

// phase is what the downstream saw during one part of the run.
type phase struct {
	requests, shed int64
}

// served is how many requests of the phase the downstream completed.
func (p phase) served() float64 {
	return float64(p.requests - p.shed)
}

// capacityDrop runs the workers behind an http limit that is fixed or tuned, and returns the first and last third.
func capacityDrop(ctx context.Context, p AdaptiveParams, tuned bool) (before, after phase, err error) {
	d := NewDownstream(p.Before, p.Latency)
	defer d.Close()
	opts := Options{Root: config.Caps{Limits: map[core.Class]int{governor.ClassHTTP: p.Before}}}
	if tuned {
		opts.Adaptive = []config.Adaptive{{
			Class:       governor.ClassHTTP,
			TargetP95:   4 * p.Latency,
			MaxOverload: 0.05,
			MinLimit:    1,
			MaxLimit:    p.Before,
			Interval:    p.Duration / 60,
			MinSamples:  5,
		}}
	}
	h, err := Start(opts)
	if err != nil {
		return before, after, err
	}
	defer h.Stop()
	c, err := h.Dial(ctx, KeyA)
	if err != nil {
		return before, after, err
	}
	defer c.Close()
	run, _, err := c.NewTask(ctx, governor.Spec{Name: "steady-load"})
	if err != nil {
		return before, after, err
	}

	client := &http.Client{Transport: governor.Transport(pooled())}
	start := time.Now()
	third := p.Duration / 3
	var wg sync.WaitGroup
	for range p.Workers {
		wg.Go(func() {
			for time.Since(start) < p.Duration {
				if get(run, client, d.URL()) != nil {
					time.Sleep(p.Backoff)
				}
			}
		})
	}
	// The downstream's own counters, read at the phase boundaries, give the shed rates.
	time.Sleep(third)
	atDrop := d.Counts()
	d.SetCapacity(p.After)
	time.Sleep(third)
	atLastThird := d.Counts()
	wg.Wait()
	atEnd := d.Counts()

	before = phase{requests: atDrop.Requests, shed: atDrop.Shed}
	after = phase{requests: atEnd.Requests - atLastThird.Requests, shed: atEnd.Shed - atLastThird.Shed}
	return before, after, nil
}

// Adaptive compares a fixed limit with a tuned one when the downstream loses capacity mid-run.
func Adaptive(ctx context.Context, p AdaptiveParams) (Report, error) {
	report := Report{
		Scenario: "Capacity drop",
		Note: fmt.Sprintf("%d workers send requests for %v, pausing %v after a failure. A third of the way in, "+
			"the downstream's capacity falls from %d to %d. Both runs are governed; only the limit differs.",
			p.Workers, p.Duration, p.Backoff, p.Before, p.After),
		Columns: []string{"fixed limit", "adaptive limit"},
	}
	fixedBefore, fixedAfter, err := capacityDrop(ctx, p, false)
	if err != nil {
		return report, err
	}
	tunedBefore, tunedAfter, err := capacityDrop(ctx, p, true)
	if err != nil {
		return report, err
	}
	report.add("before the drop: requests shed (%)",
		percent(fixedBefore.shed, fixedBefore.requests), percent(tunedBefore.shed, tunedBefore.requests))
	report.add("last third: requests shed (%)",
		percent(fixedAfter.shed, fixedAfter.requests), percent(tunedAfter.shed, tunedAfter.requests))
	report.add("last third: requests the downstream shed", float64(fixedAfter.shed), float64(tunedAfter.shed))
	report.add("last third: requests served", fixedAfter.served(), tunedAfter.served())
	return report, nil
}
