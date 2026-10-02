package bench

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Avik-creator/governor"
	"github.com/Avik-creator/governor/internal/config"
)

// RetryParams sizes the retry storm scenario.
type RetryParams struct {
	// Operations are logical calls, each wrapped in Layers retry loops of Attempts tries.
	Operations, Layers, Attempts int

	// Budget is the retry quota the whole run shares.
	Budget int64
}

// DefaultRetry wraps 20 operations in three layers of three attempts, with 20 retries to share.
var DefaultRetry = RetryParams{Operations: 20, Layers: 3, Attempts: 3, Budget: 20}

// retrying runs every operation through the nested retry loops and waits for all of them.
func retrying(p RetryParams, retry func(fn func() error) error, call func() error) {
	var layer func(depth int) error
	layer = func(depth int) error {
		if depth == p.Layers {
			return call()
		}
		return retry(func() error { return layer(depth + 1) })
	}
	var wg sync.WaitGroup
	for range p.Operations {
		wg.Go(func() { _ = layer(0) })
	}
	wg.Wait()
}

// Retry sends operations at a downstream that is down, through nested retry loops.
func Retry(ctx context.Context, p RetryParams) (Report, error) {
	report := Report{
		Scenario: "Retry storm",
		Note: fmt.Sprintf("The downstream is down. %d operations are each wrapped in %d retry loops of "+
			"%d attempts. The run has %d retries to share.", p.Operations, p.Layers, p.Attempts, p.Budget),
		Columns: withAndWithout,
	}
	client := plainClient()

	// Without Governor each layer retries on its own, so the attempts multiply.
	loud := NewDownstream(1000, 0)
	defer loud.Close()
	loud.SetFailing(true)
	retrying(p, func(fn func() error) error {
		var err error
		for range p.Attempts {
			if err = fn(); err == nil {
				return nil
			}
			time.Sleep(time.Millisecond)
		}
		return err
	}, func() error { return get(ctx, client, loud.URL()) })

	// With Governor every layer draws its retries from one budget.
	calm := NewDownstream(1000, 0)
	defer calm.Close()
	calm.SetFailing(true)
	h, err := Start(Options{Tenant: config.Caps{}})
	if err != nil {
		return report, err
	}
	defer h.Stop()
	c, err := h.Dial(ctx, KeyA)
	if err != nil {
		return report, err
	}
	defer c.Close()
	run, _, err := c.NewTask(ctx, governor.Spec{
		Name:   "retry-storm",
		Quotas: map[string]int64{governor.ResourceRetry: p.Budget},
	})
	if err != nil {
		return report, err
	}
	backoff := governor.WithBackoff(time.Millisecond, time.Millisecond)
	retrying(p, func(fn func() error) error {
		return governor.Retry(run, func(context.Context) error { return fn() },
			governor.WithMaxAttempts(p.Attempts), backoff)
	}, func() error { return get(ctx, client, calm.URL()) })

	a, b := loud.Counts().Requests, calm.Counts().Requests
	ops := float64(p.Operations)
	report.add("requests sent to the failing downstream", float64(a), float64(b))
	report.add("requests per operation", float64(a)/ops, float64(b)/ops)
	return report, nil
}
