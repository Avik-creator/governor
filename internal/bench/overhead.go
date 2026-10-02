package bench

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/Avik-creator/governor"
	"github.com/Avik-creator/governor/internal/config"
	"github.com/Avik-creator/governor/internal/core"
)

// OverheadParams sizes the overhead scenario.
type OverheadParams struct {
	// Calls is how many operations each measurement makes, and Parallel how many callers share them.
	Calls, Parallel int

	// DatabaseURL adds a second column measured with Postgres; empty leaves it out.
	DatabaseURL string
}

// DefaultOverhead makes 2,000 calls per measurement, and shares them between 16 callers when parallel.
var DefaultOverhead = OverheadParams{Calls: 2000, Parallel: 16}

// overhead measures the cost of governed calls against one governord.
func overhead(
	ctx context.Context, p OverheadParams, databaseURL string,
) (consume, lease, throughput float64, err error) {
	h, err := Start(Options{
		Root:        config.Caps{Limits: map[core.Class]int{class: p.Parallel}},
		DatabaseURL: databaseURL,
	})
	if err != nil {
		return 0, 0, 0, err
	}
	defer h.Stop()
	c, err := h.Dial(ctx, KeyA)
	if err != nil {
		return 0, 0, 0, err
	}
	defer c.Close()
	run, _, err := c.NewTask(ctx, governor.Spec{Name: "overhead"})
	if err != nil {
		return 0, 0, 0, err
	}
	nothing := func(context.Context) error { return nil }

	start := time.Now()
	for range p.Calls {
		if err := governor.Consume(run, "units", 1); err != nil {
			return 0, 0, 0, err
		}
	}
	consume = float64(time.Since(start).Microseconds()) / float64(p.Calls)

	start = time.Now()
	for range p.Calls {
		if err := governor.Do(run, class, nothing); err != nil {
			return 0, 0, 0, err
		}
	}
	lease = float64(time.Since(start).Microseconds()) / float64(p.Calls)

	// Many callers at once share commits, so throughput is more than one caller's rate.
	start = time.Now()
	var wg sync.WaitGroup
	for range p.Parallel {
		wg.Go(func() {
			for range p.Calls / p.Parallel {
				_ = governor.Consume(run, "units", 1)
			}
		})
	}
	wg.Wait()
	throughput = float64(p.Calls/p.Parallel*p.Parallel) / time.Since(start).Seconds()
	return consume, lease, throughput, nil
}

// Overhead measures what a governed call costs, in memory and with every change committed to Postgres.
func Overhead(ctx context.Context, p OverheadParams) (Report, error) {
	report := Report{
		Scenario: "Overhead",
		Note: fmt.Sprintf("%d calls over loopback gRPC. With Postgres, each call waits for its change to be "+
			"committed.", p.Calls),
		Columns: []string{"in memory", "with Postgres"},
	}
	consume, lease, throughput, err := overhead(ctx, p, "")
	if err != nil {
		return report, err
	}
	durable := [3]float64{math.NaN(), math.NaN(), math.NaN()}
	if p.DatabaseURL != "" {
		if err := wipe(ctx, p.DatabaseURL); err != nil {
			return report, err
		}
		if durable[0], durable[1], durable[2], err = overhead(ctx, p, p.DatabaseURL); err != nil {
			return report, err
		}
	}
	report.add("consume: time per call (µs)", consume, durable[0])
	report.add("acquire and release: time per pair (µs)", lease, durable[1])
	report.add(fmt.Sprintf("consume from %d callers: calls per second", p.Parallel), throughput, durable[2])
	return report, nil
}
