package bench

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Avik-creator/governor"
	"github.com/Avik-creator/governor/internal/config"
	"github.com/Avik-creator/governor/internal/core"
)

// FairnessParams sizes the noisy neighbour scenario.
type FairnessParams struct {
	// Slots is the size of the shared pool, and Hold how long each job keeps a slot.
	Slots int
	Hold  time.Duration

	// NoisyJobs are queued by one tenant before the other queues QuietJobs.
	NoisyJobs, QuietJobs int
}

// DefaultFairness has one tenant queue 400 jobs just before another queues 10, on 4 slots.
var DefaultFairness = FairnessParams{Slots: 4, Hold: 5 * time.Millisecond, NoisyJobs: 400, QuietJobs: 10}

// class is the pool the fairness and crash scenarios compete for.
const class = "db"

// waits collects how long each job of one tenant waited for a slot.
type waits struct {
	mu       sync.Mutex
	queued   []time.Duration
	finished time.Duration // when the tenant's last job ended, since its first was submitted
}

// submit runs jobs through acquire, which must block until the job has a slot and return its release.
func (w *waits) submit(jobs int, hold time.Duration, do func(job func()) error) *sync.WaitGroup {
	var wg sync.WaitGroup
	start := time.Now()
	for range jobs {
		wg.Go(func() {
			submitted := time.Now()
			_ = do(func() {
				waited := time.Since(submitted)
				time.Sleep(hold)
				w.mu.Lock()
				w.queued = append(w.queued, waited)
				w.finished = max(w.finished, time.Since(start))
				w.mu.Unlock()
			})
		})
	}
	return &wg
}

// race queues the noisy tenant's jobs, then the quiet tenant's, and waits for all of them.
func race(p FairnessParams, noisy, quiet func(job func()) error) (noisyWaits, quietWaits *waits) {
	noisyWaits, quietWaits = &waits{}, &waits{}
	first := noisyWaits.submit(p.NoisyJobs, p.Hold, noisy)
	// The pause lets the noisy tenant's whole backlog queue up first.
	time.Sleep(20 * time.Millisecond)
	second := quietWaits.submit(p.QuietJobs, p.Hold, quiet)
	first.Wait()
	second.Wait()
	return noisyWaits, quietWaits
}

// Fairness has a noisy tenant flood a shared pool just before a quiet tenant needs it.
func Fairness(ctx context.Context, p FairnessParams) (Report, error) {
	report := Report{
		Scenario: "Noisy neighbour",
		Note: fmt.Sprintf("Tenant A queues %d jobs on a pool of %d slots; tenant B then queues %d. "+
			"Each job holds a slot for %v.", p.NoisyJobs, p.Slots, p.QuietJobs, p.Hold),
		Columns: withAndWithout,
	}

	// Without Governor the pool is one first-come, first-served semaphore.
	slots := make(chan struct{}, p.Slots)
	fifo := func(job func()) error {
		slots <- struct{}{}
		defer func() { <-slots }()
		job()
		return nil
	}
	plainNoisy, plainQuiet := race(p, fifo, fifo)

	// With Governor the pool is a root limit, and each tenant waits in its own queue.
	h, err := Start(Options{Root: config.Caps{Limits: map[core.Class]int{class: p.Slots}}})
	if err != nil {
		return report, err
	}
	defer h.Stop()
	tenant := func(key string) (func(job func()) error, func(), error) {
		c, err := h.Dial(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		run, _, err := c.NewTask(ctx, governor.Spec{Name: "jobs"})
		if err != nil {
			_ = c.Close()
			return nil, nil, err
		}
		do := func(job func()) error {
			return governor.Do(run, class, func(context.Context) error {
				job()
				return nil
			})
		}
		return do, func() { _ = c.Close() }, nil
	}
	noisy, closeNoisy, err := tenant(KeyA)
	if err != nil {
		return report, err
	}
	defer closeNoisy()
	quiet, closeQuiet, err := tenant(KeyB)
	if err != nil {
		return report, err
	}
	defer closeQuiet()
	fairNoisy, fairQuiet := race(p, noisy, quiet)

	report.add("tenant B: p95 wait for a slot (ms)", ms(p95(plainQuiet.queued)), ms(p95(fairQuiet.queued)))
	report.add("tenant B: all jobs done after (ms)", ms(plainQuiet.finished), ms(fairQuiet.finished))
	report.add("tenant A: all jobs done after (ms)", ms(plainNoisy.finished), ms(fairNoisy.finished))
	report.add("jobs completed", float64(len(plainNoisy.queued)+len(plainQuiet.queued)),
		float64(len(fairNoisy.queued)+len(fairQuiet.queued)))
	return report, nil
}
