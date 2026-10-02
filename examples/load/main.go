// Command load is a small workload for trying Governor: jobs that spend an HTTP budget and queue for database slots.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Avik-creator/governor"
)

const (
	// jobDeadline ends a job that has not spent its budget by then.
	jobDeadline = time.Minute

	// pause is the gap between jobs, and idle the gap after a job that was refused everything.
	pause = 100 * time.Millisecond
	idle  = 2 * time.Second

	// slowdown is how much longer the service takes once it is over capacity.
	slowdown = 4
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		log.Fatalf("load: %v", err)
	}
}

// run starts one job after another until ctx is done.
func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	workers := fs.Int("workers", 8, "requests one job makes at once")
	budget := fs.Int64("budget", 200, "http requests one job may make")
	capacity := fs.Int("capacity", 6, "requests the simulated service handles at once before it slows down")
	if err := fs.Parse(args); err != nil {
		return err
	}
	client, err := governor.Dial(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	svc := &service{capacity: int64(*capacity)}
	for n := 1; ctx.Err() == nil; n++ {
		done, err := job(ctx, client, svc, n, *workers, *budget)
		if err != nil && ctx.Err() == nil {
			return err
		}
		log.Printf("job-%d made %d requests", n, done)
		wait := pause
		// A job that could do nothing means the tenant's own budget is spent.
		if done == 0 {
			wait = idle
		}
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
	return nil
}

// job runs one task until its http budget is spent, and returns how many requests it made.
func job(ctx context.Context, client *governor.Client, svc *service, n, workers int, budget int64) (int64, error) {
	ctx, task, err := client.NewTask(ctx, governor.Spec{
		Name:     fmt.Sprintf("job-%d", n),
		Quotas:   map[string]int64{"http": budget},
		Deadline: time.Now().Add(jobDeadline),
	})
	if err != nil {
		return 0, err
	}
	defer task.Close()

	var (
		wg    sync.WaitGroup
		done  atomic.Int64
		once  sync.Once
		first error
	)
	for range workers {
		wg.Go(func() {
			for {
				err := request(ctx, svc)
				// A spent budget is how a job is meant to end.
				if errors.Is(err, governor.ErrDenied) {
					return
				}
				if err != nil {
					once.Do(func() { first = err })
					return
				}
				done.Add(1)
			}
		})
	}
	wg.Wait()
	return done.Load(), first
}

// request spends one http unit, waits for a database slot and calls the service while holding it.
func request(ctx context.Context, svc *service) error {
	if err := governor.Consume(ctx, "http", 1); err != nil {
		return err
	}
	lease, err := governor.Acquire(ctx, "db")
	if err != nil {
		return err
	}
	took, overloaded := svc.call(ctx)
	// The report is what an adaptive limit learns the service's capacity from.
	return lease.Release(governor.Report{Latency: took, Overloaded: overloaded})
}

// service stands in for a downstream that slows down when too many calls arrive at once.
type service struct {
	capacity int64
	inflight atomic.Int64
}

// call takes as long as the service would, and reports whether it was over capacity.
func (s *service) call(ctx context.Context) (time.Duration, bool) {
	overloaded := s.inflight.Add(1) > s.capacity
	defer s.inflight.Add(-1)
	took := 10*time.Millisecond + rand.N(10*time.Millisecond)
	if overloaded {
		took *= slowdown
	}
	select {
	case <-ctx.Done():
	case <-time.After(took):
	}
	return took, overloaded
}
