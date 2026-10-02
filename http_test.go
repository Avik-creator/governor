package governor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avikmukherjee/governor/internal/core"
)

// get sends a governed GET and returns the response with its body still open.
func get(ctx context.Context, client *http.Client, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func TestTransport(t *testing.T) {
	var (
		mu      sync.Mutex
		reports []core.Report
		hits    atomic.Int32
	)
	b := newBackend(t,
		core.Spec{Limits: map[core.Class]int{ClassHTTP: 1}},
		core.Spec{Quotas: map[core.Resource]int64{ResourceHTTP: 5}})
	b.observe(func(_ core.Class, r core.Report) {
		mu.Lock()
		defer mu.Unlock()
		reports = append(reports, r)
	})
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.URL.Path == "/busy" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer site.Close()
	ctx, _ := mustTask(t, b.mustDial(), Spec{})
	client := &http.Client{Transport: Transport(nil)}

	first, err := get(ctx, client, site.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	// The lease is held until the body is closed, so the one slot is taken.
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := get(short, client, site.URL); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("GET while the slot is held = %v, want DeadlineExceeded", err)
	}
	if body, _ := io.ReadAll(first.Body); string(body) != "ok" {
		t.Errorf("body = %q, want ok", body)
	}
	if err := first.Body.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	busy, err := get(ctx, client, site.URL+"/busy")
	if err != nil {
		t.Fatalf("GET /busy: %v", err)
	}
	_ = busy.Body.Close()
	mu.Lock()
	if len(reports) != 2 || reports[0].Overloaded || !reports[1].Overloaded || reports[0].Latency <= 0 {
		t.Errorf("reports = %+v, want a normal one and then an overloaded one", reports)
	}
	mu.Unlock()

	// A request that never got a reply gives its slot back at once.
	site.CloseClientConnections()
	down := httptest.NewServer(http.NotFoundHandler())
	down.Close()
	if _, err := get(ctx, client, down.URL); err == nil {
		t.Error("GET to a closed server succeeded")
	}
	last, err := get(ctx, client, site.URL)
	if err != nil {
		t.Fatalf("GET after a failed request: %v", err)
	}
	_ = last.Body.Close()

	// Five requests were charged, including the two that got no response; the sixth is denied.
	if got := b.usage(uint64(b.tenant), ResourceHTTP); got != 5 {
		t.Fatalf("tenant has used %d http, want 5", got)
	}
	before := hits.Load()
	if _, err := get(ctx, client, site.URL); !errors.Is(err, ErrDenied) {
		t.Errorf("GET past the quota = %v, want ErrDenied", err)
	}
	body := io.NopCloser(strings.NewReader("payload"))
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, site.URL, body)
	if _, err := client.Do(req); !errors.Is(err, ErrNoTask) {
		t.Errorf("POST without a task = %v, want ErrNoTask", err)
	}
	if hits.Load() != before {
		t.Error("a refused request reached the server")
	}
}

func TestRetry(t *testing.T) {
	boom := errors.New("upstream failed")
	fast := WithBackoff(time.Millisecond, time.Millisecond)
	// failing returns a function that fails its first n calls and counts all of them.
	failing := func(n int, calls *int) func(context.Context) error {
		return func(context.Context) error {
			*calls++
			if *calls <= n {
				return boom
			}
			return nil
		}
	}
	tests := []struct {
		name      string
		budget    int64
		run       func(ctx context.Context, calls *int) error
		wantCalls int
		wantUsed  int64
		wantErrs  []error
	}{
		{"succeeds first time for free", 3, func(ctx context.Context, calls *int) error {
			return Retry(ctx, failing(0, calls), fast)
		}, 1, 0, nil},
		{"succeeds after two retries", 3, func(ctx context.Context, calls *int) error {
			return Retry(ctx, failing(2, calls), fast)
		}, 3, 2, nil},
		{"stops when the budget is spent", 3, func(ctx context.Context, calls *int) error {
			return Retry(ctx, failing(100, calls), fast)
		}, 4, 3, []error{boom, ErrDenied}},
		{"stops at the attempt limit", 3, func(ctx context.Context, calls *int) error {
			return Retry(ctx, failing(100, calls), fast, WithMaxAttempts(2))
		}, 2, 1, []error{boom}},
		{"nested loops share one budget", 5, func(ctx context.Context, calls *int) error {
			return Retry(ctx, func(ctx context.Context) error {
				return Retry(ctx, func(ctx context.Context) error {
					return Retry(ctx, failing(100, calls), fast)
				}, fast)
			}, fast)
		}, 6, 5, []error{boom, ErrDenied}},
		{"does not retry a denial", 3, func(ctx context.Context, calls *int) error {
			return Retry(ctx, func(ctx context.Context) error {
				*calls++
				return Consume(ctx, "tokens", 1)
			}, fast)
		}, 1, 0, []error{ErrDenied}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newBackend(t, core.Spec{},
				core.Spec{Quotas: map[core.Resource]int64{ResourceRetry: tc.budget, "tokens": 0}})
			ctx, _ := mustTask(t, b.mustDial(), Spec{})
			calls := 0
			err := tc.run(ctx, &calls)
			if (err == nil) != (len(tc.wantErrs) == 0) {
				t.Fatalf("Retry = %v, want errors %v", err, tc.wantErrs)
			}
			for _, want := range tc.wantErrs {
				if !errors.Is(err, want) {
					t.Errorf("Retry = %v, want it to match %v", err, want)
				}
			}
			if calls != tc.wantCalls {
				t.Errorf("function ran %d times, want %d", calls, tc.wantCalls)
			}
			if got := b.usage(uint64(b.tenant), ResourceRetry); got != tc.wantUsed {
				t.Errorf("retry budget used = %d, want %d", got, tc.wantUsed)
			}
		})
	}

	t.Run("gives up when the context ends", func(t *testing.T) {
		b := newBackend(t, core.Spec{}, core.Spec{})
		ctx, _ := mustTask(t, b.mustDial(), Spec{})
		ctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		calls := 0
		err := Retry(ctx, failing(100, &calls), WithBackoff(time.Hour, time.Hour))
		if !errors.Is(err, boom) || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Retry = %v, want the last error and DeadlineExceeded", err)
		}
	})
}
