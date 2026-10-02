// Package bench runs workloads with and without Governor against a simulated downstream.
package bench

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"slices"
	"text/tabwriter"
	"time"
)

// Columns most scenarios compare.
var withAndWithout = []string{"without Governor", "with Governor"}

// Report is the outcome of one scenario: one row per metric, one value per column.
type Report struct {
	Scenario string   `json:"scenario"`
	Note     string   `json:"note"`
	Columns  []string `json:"columns"`
	Rows     []Row    `json:"rows"`
}

// Row is one metric; a value that was not measured is NaN.
type Row struct {
	Metric string    `json:"metric"`
	Values []float64 `json:"values"`
}

// MarshalJSON writes a value that was not measured as null, since JSON has no NaN.
func (r Row) MarshalJSON() ([]byte, error) {
	values := make([]*float64, len(r.Values))
	for i, v := range r.Values {
		if !math.IsNaN(v) {
			values[i] = &v
		}
	}
	return json.Marshal(struct {
		Metric string     `json:"metric"`
		Values []*float64 `json:"values"`
	}{r.Metric, values})
}

// UnmarshalJSON reads a null value back as NaN.
func (r *Row) UnmarshalJSON(data []byte) error {
	var raw struct {
		Metric string     `json:"metric"`
		Values []*float64 `json:"values"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	r.Metric, r.Values = raw.Metric, make([]float64, len(raw.Values))
	for i, v := range raw.Values {
		r.Values[i] = math.NaN()
		if v != nil {
			r.Values[i] = *v
		}
	}
	return nil
}

// add appends a metric with one value per column.
func (r *Report) add(metric string, values ...float64) {
	r.Rows = append(r.Rows, Row{Metric: metric, Values: values})
}

// Value returns the value of a metric in a column, or NaN if there is none.
func (r Report) Value(metric string, column int) float64 {
	for _, row := range r.Rows {
		if row.Metric == metric && column < len(row.Values) {
			return row.Values[column]
		}
	}
	return math.NaN()
}

// Write prints the report as an aligned table.
func (r Report) Write(w io.Writer) error {
	fmt.Fprintf(w, "\n%s\n%s\n\n", r.Scenario, r.Note)
	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', tabwriter.AlignRight)
	fmt.Fprint(tw, "\t")
	for _, c := range r.Columns {
		fmt.Fprintf(tw, "%s\t", c)
	}
	fmt.Fprintln(tw)
	for _, row := range r.Rows {
		fmt.Fprintf(tw, "%s\t", row.Metric)
		for _, v := range row.Values {
			fmt.Fprintf(tw, "%s\t", format(v))
		}
		fmt.Fprintln(tw)
	}
	return tw.Flush()
}

// format prints whole numbers without decimals and marks what was not measured.
func format(v float64) string {
	switch {
	case math.IsNaN(v):
		return "n/a"
	case v == math.Trunc(v) && math.Abs(v) < 1e15:
		return fmt.Sprintf("%d", int64(v))
	default:
		return fmt.Sprintf("%.2f", v)
	}
}

// ms converts a duration to milliseconds for a report.
func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

// percent returns part as a percentage of whole, or zero for an empty whole.
func percent(part, whole int64) float64 {
	if whole == 0 {
		return 0
	}
	return 100 * float64(part) / float64(whole)
}

// p95 returns the 95th percentile of the durations, or zero if there are none.
func p95(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sorted := slices.Sorted(slices.Values(d))
	return sorted[int(math.Ceil(0.95*float64(len(sorted))))-1]
}
