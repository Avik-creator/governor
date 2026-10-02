package adaptive

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Avik-creator/governor/internal/core"
)

// fakeLimiter records the limits a controller sets.
type fakeLimiter struct {
	mu   sync.Mutex
	sets []int
	err  error
}

func (f *fakeLimiter) SetLimit(_ core.NodeID, _ core.Class, limit int) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, f.err
	}
	f.sets = append(f.sets, limit)
	return 0, nil
}

func (f *fakeLimiter) last() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sets) == 0 {
		return 0, 0
	}
	return f.sets[len(f.sets)-1], len(f.sets)
}

func config() Config {
	return Config{
		Node: core.RootID, Class: "db", Initial: 10, MinLimit: 2, MaxLimit: 12,
		Interval: time.Millisecond, TargetP95: 100 * time.Millisecond, MaxOverload: 0.1, MinSamples: 20,
	}
}

// observe reports n releases with the given latency, the first `overloaded` of them overloaded.
func observe(c *Controller, n int, latency time.Duration, overloaded int) {
	for i := range n {
		c.Observe(core.Report{Latency: latency, Overloaded: i < overloaded})
	}
}

func TestStep(t *testing.T) {
	const fast, slow = 10 * time.Millisecond, 500 * time.Millisecond
	tests := []struct {
		name    string
		feed    func(*Controller)
		want    int
		wantP95 time.Duration
	}{
		{"no samples", func(*Controller) {}, 10, 0},
		{"too few samples", func(c *Controller) { observe(c, 19, slow, 19) }, 10, 0},
		{"healthy adds one", func(c *Controller) { observe(c, 20, fast, 0) }, 11, fast},
		{"overload at the threshold is healthy", func(c *Controller) { observe(c, 20, fast, 2) }, 11, fast},
		{"overload above the threshold cuts", func(c *Controller) { observe(c, 20, fast, 3) }, 7, fast},
		{"one slow sample in twenty is below p95", func(c *Controller) {
			observe(c, 19, fast, 0)
			observe(c, 1, slow, 0)
		}, 11, fast},
		{"two slow samples in twenty reach p95", func(c *Controller) {
			observe(c, 18, fast, 0)
			observe(c, 2, slow, 0)
		}, 7, slow},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lim := &fakeLimiter{}
			c, err := New(config())
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			tc.feed(c)
			d, err := c.Step(lim)
			if err != nil {
				t.Fatalf("Step: %v", err)
			}
			if d.Old != 10 || d.New != tc.want || d.P95 != tc.wantP95 {
				t.Errorf("decision = %+v, want 10 -> %d at p95 %v", d, tc.want, tc.wantP95)
			}
			if got, n := lim.last(); (tc.want != 10) != (n == 1) || (n == 1 && got != tc.want) {
				t.Errorf("limiter saw %d sets ending in %d, want the limit %d set only if it changed", n, got, tc.want)
			}
		})
	}
}

func TestStepStaysInsideBounds(t *testing.T) {
	lim := &fakeLimiter{}
	c, err := New(config())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Each interval's reports are used once, so an idle interval changes nothing.
	want := []int{11, 12, 12, 12}
	for i, w := range want {
		if i != 3 {
			observe(c, 20, time.Millisecond, 0)
		}
		if d, _ := c.Step(lim); d.New != w {
			t.Fatalf("step %d: limit = %d, want %d", i, d.New, w)
		}
	}
	// 12 -> 8 -> 5 -> 3 -> 2, and never below the minimum.
	for i, w := range []int{8, 5, 3, 2, 2} {
		observe(c, 20, time.Second, 0)
		if d, _ := c.Step(lim); d.New != w {
			t.Fatalf("slow step %d: limit = %d, want %d", i, d.New, w)
		}
	}
}

func TestStepReportsLimiterFailure(t *testing.T) {
	boom := errors.New("node has ended")
	lim := &fakeLimiter{err: boom}
	c, err := New(config())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	observe(c, 20, time.Millisecond, 0)
	if _, err := c.Step(lim); !errors.Is(err, boom) {
		t.Errorf("Step = %v, want the limiter's error", err)
	}
	// The limit was not applied, so the controller still believes the old one.
	observe(c, 20, time.Millisecond, 0)
	if d, _ := c.Step(lim); d.Old != 10 {
		t.Errorf("old limit after a failed set = %d, want 10", d.Old)
	}
}

func TestNewRejects(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config)
	}{
		{"empty class", func(c *Config) { c.Class = "" }},
		{"zero minimum", func(c *Config) { c.MinLimit = 0 }},
		{"maximum below minimum", func(c *Config) { c.MaxLimit = 1 }},
		{"initial above maximum", func(c *Config) { c.Initial = 13 }},
		{"initial below minimum", func(c *Config) { c.Initial = 1 }},
		{"zero interval", func(c *Config) { c.Interval = 0 }},
		{"zero target", func(c *Config) { c.TargetP95 = 0 }},
		{"overload above one", func(c *Config) { c.MaxOverload = 1.5 }},
		{"zero samples", func(c *Config) { c.MinSamples = 0 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config()
			tc.change(&cfg)
			if _, err := New(cfg); err == nil {
				t.Error("New accepted the config")
			}
		})
	}
}

func TestRunDrivesARealEngine(t *testing.T) {
	var controller *Controller
	engine, err := core.New(core.Config{
		Root:     core.Spec{Limits: map[core.Class]int{"db": 10}},
		Observer: func(_ core.Class, r core.Report) { controller.Observe(r) },
	})
	if err != nil {
		t.Fatalf("core.New: %v", err)
	}
	cfg := config()
	cfg.MinSamples = 1
	if controller, err = New(cfg); err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		controller.Run(ctx, engine)
		close(done)
	}()

	// A downstream that answers slowly must bring the limit down to its minimum.
	sid, _, _, err := engine.OpenSession(core.RootID, 0)
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		lease, _, err := engine.Acquire(ctx, sid, core.RootID, "db", core.AcquireOptions{})
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		if _, err := engine.Release(sid, lease, core.Report{Latency: time.Second}); err != nil {
			t.Fatalf("Release: %v", err)
		}
		controller.mu.Lock()
		limit := controller.limit
		controller.mu.Unlock()
		if limit == cfg.MinLimit {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("limit is still %d, want %d", limit, cfg.MinLimit)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	// The engine enforces the lowered limit: only MinLimit leases fit.
	for range cfg.MinLimit {
		if _, _, err := engine.Acquire(t.Context(), sid, core.RootID, "db", core.AcquireOptions{}); err != nil {
			t.Fatalf("Acquire within the limit: %v", err)
		}
	}
	short, stop := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer stop()
	_, _, err = engine.Acquire(short, sid, core.RootID, "db", core.AcquireOptions{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Acquire past the lowered limit = %v, want DeadlineExceeded", err)
	}
}
