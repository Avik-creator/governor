// Package adaptive tunes a concurrency limit from the latency and overload its leases report.
package adaptive

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/Avik-creator/governor/internal/core"
)

// decrease is what the limit is multiplied by when the downstream is struggling.
const decrease = 0.7

// Config describes one controller, which owns the limit of one class on one node.
type Config struct {
	// Node and Class name the limit; Node is normally the root.
	Node  core.NodeID
	Class core.Class

	// Initial is the limit the node has when the controller starts.
	Initial int

	// MinLimit and MaxLimit bound what the controller may set.
	MinLimit int
	MaxLimit int

	// Interval is how often the limit is reconsidered.
	Interval time.Duration

	// TargetP95 is the 95th percentile latency the downstream should stay under.
	TargetP95 time.Duration

	// MaxOverload is the share of releases that may report overload, from 0 to 1.
	MaxOverload float64

	// MinSamples is how many releases an interval needs before the limit moves.
	MinSamples int
}

// validate reports the first setting a controller could not run with.
func (c Config) validate() error {
	switch {
	case c.Class == "":
		return errors.New("class is empty")
	case c.MinLimit < 1 || c.MaxLimit < c.MinLimit:
		return errors.New("limits must satisfy 1 <= min_limit <= max_limit")
	case c.Initial < c.MinLimit || c.Initial > c.MaxLimit:
		return fmt.Errorf("initial limit %d is outside %d..%d", c.Initial, c.MinLimit, c.MaxLimit)
	case c.Interval <= 0 || c.TargetP95 <= 0:
		return errors.New("interval and target_p95 must be positive")
	case c.MaxOverload < 0 || c.MaxOverload > 1:
		return errors.New("max_overload must be between 0 and 1")
	case c.MinSamples < 1:
		return errors.New("min_samples must be at least 1")
	}
	return nil
}

// Limiter is the part of the engine a controller needs.
type Limiter interface {
	SetLimit(id core.NodeID, class core.Class, limit int) (uint64, error)
}

// Decision is the outcome of one interval, with the inputs that caused it.
type Decision struct {
	Samples  int
	P95      time.Duration
	Overload float64 // share of the samples that reported overload
	Old, New int
}

// Controller applies additive increase and multiplicative decrease to one limit.
type Controller struct {
	cfg        Config
	mu         sync.Mutex
	limit      int
	latencies  []time.Duration // reported since the last step
	overloaded int
}

// New returns a controller for the limit cfg describes.
func New(cfg Config) (*Controller, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("adaptive: class %q: %w", cfg.Class, err)
	}
	return &Controller{cfg: cfg, limit: cfg.Initial}, nil
}

// Class returns the class whose reports this controller wants.
func (c *Controller) Class() core.Class {
	return c.cfg.Class
}

// Observe records the report of one released lease; it is safe for concurrent use.
func (c *Controller) Observe(r core.Report) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.latencies = append(c.latencies, r.Latency)
	if r.Overloaded {
		c.overloaded++
	}
}

// Step ends the interval: it decides on the reports so far and applies the new limit.
func (c *Controller) Step(limiter Limiter) (Decision, error) {
	c.mu.Lock()
	latencies, overloaded := c.latencies, c.overloaded
	c.latencies, c.overloaded = nil, 0
	d := Decision{Samples: len(latencies), Old: c.limit, New: c.limit}
	c.mu.Unlock()

	// Too few samples say nothing about the downstream, so the limit stays.
	if d.Samples < c.cfg.MinSamples {
		return d, nil
	}
	slices.Sort(latencies)
	d.P95 = latencies[int(math.Ceil(0.95*float64(d.Samples)))-1]
	d.Overload = float64(overloaded) / float64(d.Samples)
	if d.P95 > c.cfg.TargetP95 || d.Overload > c.cfg.MaxOverload {
		d.New = max(c.cfg.MinLimit, int(float64(d.Old)*decrease))
	} else {
		d.New = min(c.cfg.MaxLimit, d.Old+1)
	}
	if d.New == d.Old {
		return d, nil
	}
	if _, err := limiter.SetLimit(c.cfg.Node, c.cfg.Class, d.New); err != nil {
		return d, fmt.Errorf("adaptive: set %s limit: %w", c.cfg.Class, err)
	}
	c.mu.Lock()
	c.limit = d.New
	c.mu.Unlock()
	return d, nil
}

// Run steps the controller every interval until ctx is done, logging each change.
func (c *Controller) Run(ctx context.Context, limiter Limiter) {
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		d, err := c.Step(limiter)
		switch {
		case err != nil:
			slog.ErrorContext(ctx, "governord: adaptive step failed", "class", c.cfg.Class, "err", err)
		case d.New != d.Old:
			// The log line is the record of why the limit moved.
			slog.InfoContext(ctx, "governord: limit changed", "class", c.cfg.Class, "old", d.Old, "new", d.New,
				"p95", d.P95, "overload", d.Overload, "samples", d.Samples)
		}
	}
}
