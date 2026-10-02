package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Avik-creator/governor"
	"github.com/Avik-creator/governor/internal/config"
	"github.com/Avik-creator/governor/internal/core"
)

// FanoutParams sizes the runaway fan-out scenario.
type FanoutParams struct {
	// Each task spawns Branch subtasks, Depth levels deep, and makes Calls requests.
	Branch, Depth, Calls int

	// Capacity and Latency describe the downstream.
	Capacity int
	Latency  time.Duration

	// Budget is the http quota of the run; HTTPLimit and AgentsLimit are the shared pools.
	Budget      int64
	HTTPLimit   int
	AgentsLimit int
}

// DefaultFanout is a tree of 121 tasks that wants 363 requests from a service that can take 10 at once.
var DefaultFanout = FanoutParams{
	Branch: 3, Depth: 4, Calls: 3,
	Capacity: 10, Latency: 5 * time.Millisecond,
	Budget: 150, HTTPLimit: 8, AgentsLimit: 6,
}

// fanoutCounts is what one run of the tree did.
type fanoutCounts struct {
	tasks   atomic.Int64 // tasks that made all their requests
	refused atomic.Int64 // requests Governor refused before they were sent
}

// get sends one request and reports whether the downstream served it.
func get(ctx context.Context, client *http.Client, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	if err := resp.Body.Close(); err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("downstream answered %d", resp.StatusCode)
	}
	return nil
}

// Fanout runs a task that spawns subtasks recursively, each calling the downstream.
func Fanout(ctx context.Context, p FanoutParams) (Report, error) {
	report := Report{
		Scenario: "Runaway fan-out",
		Note: fmt.Sprintf("Each task spawns %d subtasks, %d levels deep, and makes %d requests. "+
			"The downstream can take %d at once. The run's budget is %d requests.",
			p.Branch, p.Depth, p.Calls, p.Capacity, p.Budget),
		Columns: withAndWithout,
	}

	// Without Governor every task is a goroutine and nothing holds the tree back.
	free := NewDownstream(p.Capacity, p.Latency)
	defer free.Close()
	var ungoverned fanoutCounts
	client := plainClient()
	var wg sync.WaitGroup
	var visit func(depth int)
	visit = func(depth int) {
		defer wg.Done()
		for range p.Calls {
			_ = get(ctx, client, free.URL())
		}
		ungoverned.tasks.Add(1)
		if depth < p.Depth {
			for range p.Branch {
				wg.Add(1)
				go visit(depth + 1)
			}
		}
	}
	wg.Add(1)
	go visit(0)
	wg.Wait()

	// With Governor the same tree runs as subtasks behind a quota and two pools.
	held := NewDownstream(p.Capacity, p.Latency)
	defer held.Close()
	h, err := Start(Options{Root: config.Caps{Limits: map[core.Class]int{
		governor.ClassHTTP: p.HTTPLimit, governor.ClassAgents: p.AgentsLimit,
	}}})
	if err != nil {
		return report, err
	}
	defer h.Stop()
	c, err := h.Dial(ctx, KeyA)
	if err != nil {
		return report, err
	}
	defer c.Close()
	run, task, err := c.NewTask(ctx, governor.Spec{
		Name:   "fan-out",
		Quotas: map[string]int64{governor.ResourceHTTP: p.Budget},
	})
	if err != nil {
		return report, err
	}
	defer task.Close()

	var governed fanoutCounts
	governedClient := &http.Client{Transport: governor.Transport(pooled())}
	g, _ := governor.NewGroup(run, governor.ClassAgents)
	var spawn func(depth int)
	spawn = func(depth int) {
		g.Go(governor.Spec{Name: fmt.Sprint("depth-", depth)}, func(ctx context.Context) error {
			for range p.Calls {
				// A task that is out of budget stops, and so does everything it would have spawned.
				if err := get(ctx, governedClient, held.URL()); errors.Is(err, governor.ErrDenied) {
					governed.refused.Add(1)
					return nil
				}
			}
			governed.tasks.Add(1)
			// Children join the same group, so this task's lease is free before they need one.
			if depth < p.Depth {
				for range p.Branch {
					spawn(depth + 1)
				}
			}
			return nil
		})
	}
	spawn(0)
	if err := g.Wait(); err != nil {
		return report, err
	}

	a, b := free.Counts(), held.Counts()
	report.add("tasks that ran", float64(ungoverned.tasks.Load()), float64(governed.tasks.Load()))
	report.add("requests reaching the downstream", float64(a.Requests), float64(b.Requests))
	report.add("requests the downstream shed (503)", float64(a.Shed), float64(b.Shed))
	report.add("peak concurrency at the downstream", float64(a.Peak), float64(b.Peak))
	report.add("requests refused by the budget", 0, float64(governed.refused.Load()))
	return report, nil
}
