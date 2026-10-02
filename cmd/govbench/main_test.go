package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Avik-creator/governor/internal/bench"
)

func TestRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	var out strings.Builder
	// The retry scenario is the quickest, and restart is skipped without a database.
	if err := run(t.Context(), []string{"-scenario", "retry,restart", "-json", path}, &out, io.Discard); err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, want := range []string{"Retry storm", "requests per operation", "Skipped: it needs -database-url."} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var reports []bench.Report
	if err := json.Unmarshal(data, &reports); err != nil {
		t.Fatalf("the JSON report does not parse: %v", err)
	}
	if len(reports) != 1 || reports[0].Scenario != "Retry storm" || len(reports[0].Rows) == 0 {
		t.Errorf("JSON reports = %+v, want the one scenario that ran", reports)
	}
}

func TestRunRejects(t *testing.T) {
	for _, args := range [][]string{{"-scenario", "nope"}, {"-unknown"}} {
		if err := run(t.Context(), args, io.Discard, io.Discard); err == nil {
			t.Errorf("run(%v) succeeded", args)
		}
	}
}
