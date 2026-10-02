// Command govbench runs workloads with and without Governor and reports what differed.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"

	"github.com/Avik-creator/governor/internal/bench"
)

// scenario is one benchmark and the name it is selected by.
type scenario struct {
	name string
	run  func(ctx context.Context, databaseURL string) (bench.Report, error)
}

// scenarios lists every benchmark in the order it is run.
var scenarios = []scenario{
	{"fanout", func(ctx context.Context, _ string) (bench.Report, error) {
		return bench.Fanout(ctx, bench.DefaultFanout)
	}},
	{"fairness", func(ctx context.Context, _ string) (bench.Report, error) {
		return bench.Fairness(ctx, bench.DefaultFairness)
	}},
	{"retry", func(ctx context.Context, _ string) (bench.Report, error) {
		return bench.Retry(ctx, bench.DefaultRetry)
	}},
	{"adaptive", func(ctx context.Context, _ string) (bench.Report, error) {
		return bench.Adaptive(ctx, bench.DefaultAdaptive)
	}},
	{"crash", func(ctx context.Context, _ string) (bench.Report, error) {
		return bench.Crash(ctx, bench.DefaultCrash)
	}},
	{"restart", func(ctx context.Context, databaseURL string) (bench.Report, error) {
		p := bench.DefaultRestart
		p.DatabaseURL = databaseURL
		return bench.Restart(ctx, p)
	}},
	{"overhead", func(ctx context.Context, databaseURL string) (bench.Report, error) {
		p := bench.DefaultOverhead
		p.DatabaseURL = databaseURL
		return bench.Overhead(ctx, p)
	}},
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, "govbench:", err)
		}
		os.Exit(1)
	}
}

// run executes the selected scenarios, printing each report and optionally saving them as JSON.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	var names []string
	for _, s := range scenarios {
		names = append(names, s.name)
	}
	flags := flag.NewFlagSet("govbench", flag.ContinueOnError)
	flags.SetOutput(stderr)
	only := flags.String("scenario", "all", "comma-separated scenarios to run: "+strings.Join(names, ", "))
	databaseURL := flags.String("database-url", "",
		"a throwaway Postgres for the restart and overhead scenarios; Governor's tables in it are emptied")
	jsonPath := flags.String("json", "", "also write the reports to this file as JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	selected := names
	if *only != "all" {
		selected = strings.Split(*only, ",")
		for _, name := range selected {
			if !slices.Contains(names, name) {
				return fmt.Errorf("unknown scenario %q; choose from %s", name, strings.Join(names, ", "))
			}
		}
	}
	// The daemon logs every start and stop, which would bury the reports.
	slog.SetDefault(slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelError})))

	var reports []bench.Report
	for _, s := range scenarios {
		if !slices.Contains(selected, s.name) {
			continue
		}
		report, err := s.run(ctx, *databaseURL)
		if errors.Is(err, bench.ErrNoDatabase) {
			fmt.Fprintf(stdout, "\n%s\nSkipped: it needs -database-url.\n", report.Scenario)
			continue
		}
		if err != nil {
			return fmt.Errorf("%s: %w", s.name, err)
		}
		if err := report.Write(stdout); err != nil {
			return err
		}
		reports = append(reports, report)
	}
	if *jsonPath == "" {
		return nil
	}
	data, err := json.MarshalIndent(reports, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(*jsonPath, append(data, '\n'), 0o644)
}
