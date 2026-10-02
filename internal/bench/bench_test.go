package bench

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

// Columns of a report that compares a run without Governor to one with it.
const (
	without = 0
	with    = 1
)

func TestFanout(t *testing.T) {
	p := FanoutParams{
		Branch: 2, Depth: 3, Calls: 2,
		Capacity: 4, Latency: 2 * time.Millisecond,
		Budget: 12, HTTPLimit: 3, AgentsLimit: 3,
	}
	r, err := Fanout(t.Context(), p)
	if err != nil {
		t.Fatalf("Fanout: %v", err)
	}
	const requests = "requests reaching the downstream"
	// A tree of 15 tasks making 2 requests each wants 30 in all.
	if got := r.Value(requests, without); got != 30 {
		t.Errorf("ungoverned requests = %v, want 30", got)
	}
	if got := r.Value(requests, with); got != float64(p.Budget) {
		t.Errorf("governed requests = %v, want the budget of %d", got, p.Budget)
	}
	if got := r.Value("peak concurrency at the downstream", with); got > float64(p.HTTPLimit) {
		t.Errorf("governed peak concurrency = %v, want at most %d", got, p.HTTPLimit)
	}
	if got := r.Value("requests the downstream shed (503)", with); got != 0 {
		t.Errorf("governed run had %v requests shed, want 0", got)
	}
	if got := r.Value("requests refused by the budget", with); got == 0 {
		t.Error("the budget refused nothing")
	}
}

func TestFairness(t *testing.T) {
	r, err := Fairness(t.Context(), FairnessParams{Slots: 2, Hold: 2 * time.Millisecond, NoisyJobs: 80, QuietJobs: 5})
	if err != nil {
		t.Fatalf("Fairness: %v", err)
	}
	const wait = "tenant B: p95 wait for a slot (ms)"
	plain, fair := r.Value(wait, without), r.Value(wait, with)
	if fair >= plain/2 {
		t.Errorf("tenant B waited %.1fms with Governor and %.1fms without, want less than half", fair, plain)
	}
	if a, b := r.Value("jobs completed", without), r.Value("jobs completed", with); a != 85 || b != 85 {
		t.Errorf("jobs completed = %v and %v, want 85 in both runs", a, b)
	}
}

func TestRetry(t *testing.T) {
	p := RetryParams{Operations: 5, Layers: 3, Attempts: 3, Budget: 5}
	r, err := Retry(t.Context(), p)
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}
	const requests = "requests sent to the failing downstream"
	// Three layers of three attempts multiply to 27 requests per operation.
	if got := r.Value(requests, without); got != 135 {
		t.Errorf("ungoverned requests = %v, want 135", got)
	}
	// With a shared budget it is one free attempt per operation plus the retries.
	if got := r.Value(requests, with); got != float64(p.Operations)+float64(p.Budget) {
		t.Errorf("governed requests = %v, want %d", got, int64(p.Operations)+p.Budget)
	}
}

func TestAdaptive(t *testing.T) {
	r, err := Adaptive(t.Context(), AdaptiveParams{
		Workers: 20, Duration: 1500 * time.Millisecond, Before: 10, After: 3,
		Latency: 5 * time.Millisecond, Backoff: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Adaptive: %v", err)
	}
	const fixed, tuned = 0, 1
	const early = "before the drop: requests shed (%)"
	if a, b := r.Value(early, fixed), r.Value(early, tuned); a > 5 || b > 5 {
		t.Errorf("shed before the drop = %.1f%% and %.1f%%, want next to none", a, b)
	}
	const shed = "last third: requests the downstream shed"
	if a, b := r.Value(shed, fixed), r.Value(shed, tuned); b >= a/2 {
		t.Errorf("requests shed after the drop = %v fixed and %v adaptive, want adaptive under half", a, b)
	}
	const served = "last third: requests served"
	if a, b := r.Value(served, fixed), r.Value(served, tuned); b <= a {
		t.Errorf("requests served after the drop = %v fixed and %v adaptive, want adaptive to serve more", a, b)
	}
}

func TestCrash(t *testing.T) {
	p := CrashParams{Slots: 2, TTL: 300 * time.Millisecond}
	r, err := Crash(t.Context(), p)
	if err != nil {
		t.Fatalf("Crash: %v", err)
	}
	// The pool comes back when the dead worker's session lapses, not before and not much after.
	reclaimed := r.Value("pool usable by another worker after (ms)", 0)
	if reclaimed < 250 || reclaimed > 1500 {
		t.Errorf("pool reclaimed after %.0fms, want about the TTL of %v", reclaimed, p.TTL)
	}
	if got := r.Value("late releases by the dead worker that were refused", 0); got != float64(p.Slots) {
		t.Errorf("refused late releases = %v, want %d", got, p.Slots)
	}
	if got := r.Value("survivor's lease id is newer than every stale one (1 = yes)", 0); got != 1 {
		t.Error("the survivor's lease id is not newer than the stale ones")
	}
}

func TestOverhead(t *testing.T) {
	r, err := Overhead(t.Context(), OverheadParams{Calls: 200, Parallel: 4})
	if err != nil {
		t.Fatalf("Overhead: %v", err)
	}
	for _, row := range r.Rows {
		if row.Values[0] <= 0 {
			t.Errorf("%s = %v in memory, want a positive number", row.Metric, row.Values[0])
		}
		if !math.IsNaN(row.Values[1]) {
			t.Errorf("%s = %v with Postgres, want it left unmeasured without a database", row.Metric, row.Values[1])
		}
	}
}

func TestRestart(t *testing.T) {
	if _, err := Restart(t.Context(), RestartParams{Workers: 1, Duration: time.Second}); !errors.Is(err, ErrNoDatabase) {
		t.Errorf("Restart without a database = %v, want ErrNoDatabase", err)
	}
	dsn := os.Getenv("GOVERNOR_TEST_DSN")
	if dsn == "" {
		t.Skip("GOVERNOR_TEST_DSN is not set")
	}
	p := RestartParams{Workers: 2, Duration: 1500 * time.Millisecond, DatabaseURL: dsn}
	r, err := Restart(t.Context(), p)
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	acknowledged := r.Value("charges acknowledged in total", 0)
	recorded := r.Value("charges on record after the restart", 0)
	before := r.Value("charges acknowledged before the restart", 0)
	// A charge whose reply was lost in the shutdown may be on record without being acknowledged.
	if recorded < acknowledged || recorded > acknowledged+float64(p.Workers) {
		t.Errorf("record holds %v charges for %v acknowledged, want none lost", recorded, acknowledged)
	}
	if before == 0 || acknowledged <= before {
		t.Errorf("acknowledged %v before the restart and %v in total, want work on both sides", before, acknowledged)
	}
	if got := r.Value("session and lease still valid after the restart (1 = yes)", 0); got != 1 {
		t.Error("the session or its lease did not survive the restart")
	}
}

func TestReportWrite(t *testing.T) {
	r := Report{Scenario: "Example", Note: "A note.", Columns: withAndWithout}
	r.add("whole numbers", 27, 2)
	r.add("fractions and gaps", 1.5, math.NaN())
	var out strings.Builder
	if err := r.Write(&out); err != nil {
		t.Fatalf("Write: %v", err)
	}
	for _, want := range []string{"Example", "A note.", "without Governor", "27", "1.50", "n/a"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
	if !math.IsNaN(r.Value("no such metric", 0)) || !math.IsNaN(r.Value("whole numbers", 5)) {
		t.Error("Value of a missing metric or column is not NaN")
	}

	// A value that was not measured survives a trip through JSON.
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var back Report
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if back.Value("whole numbers", 0) != 27 || !math.IsNaN(back.Value("fractions and gaps", 1)) {
		t.Errorf("report after JSON = %+v", back)
	}
}
