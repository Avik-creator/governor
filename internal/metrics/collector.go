package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Avik-creator/governor/internal/core"
)

// Labels never carry a task's id or name, so the number of series grows only with the tenants.
var (
	nodesDesc    = prometheus.NewDesc("governor_nodes", "Nodes in the tree, ended ones included until they are removed.", nil, nil)
	sessionsDesc = prometheus.NewDesc("governor_sessions", "Open sessions.", nil, nil)
	waitingDesc  = prometheus.NewDesc("governor_waiting_acquires", "Acquires queued for a lease, by class.", []string{"class"}, nil)

	rootUsedDesc  = prometheus.NewDesc("governor_root_quota_used_total", "Units consumed in the whole tree, by resource.", []string{"resource"}, nil)
	rootQuotaDesc = prometheus.NewDesc("governor_root_quota_limit", "The root's cap on a resource.", []string{"resource"}, nil)
	rootHeldDesc  = prometheus.NewDesc("governor_root_leases_held", "Leases held in the whole tree, by class.", []string{"class"}, nil)
	rootLimitDesc = prometheus.NewDesc("governor_root_lease_limit", "The size of a shared pool: the root's cap on a class.", []string{"class"}, nil)

	tenantUsedDesc  = prometheus.NewDesc("governor_tenant_quota_used_total", "Units a tenant has consumed, by resource.", []string{"tenant", "resource"}, nil)
	tenantQuotaDesc = prometheus.NewDesc("governor_tenant_quota_limit", "A tenant's cap on a resource.", []string{"tenant", "resource"}, nil)
	tenantHeldDesc  = prometheus.NewDesc("governor_tenant_leases_held", "Leases a tenant holds, by class.", []string{"tenant", "class"}, nil)
	tenantLimitDesc = prometheus.NewDesc("governor_tenant_lease_limit", "A tenant's cap on a class.", []string{"tenant", "class"}, nil)
)

// collector turns one reading of the tree into metrics each time Prometheus scrapes.
type collector struct {
	src Source
}

// Describe lists the metrics the collector can produce.
func (collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		nodesDesc, sessionsDesc, waitingDesc,
		rootUsedDesc, rootQuotaDesc, rootHeldDesc, rootLimitDesc,
		tenantUsedDesc, tenantQuotaDesc, tenantHeldDesc, tenantLimitDesc,
	} {
		ch <- d
	}
}

// series is the labels of one sample: the tenant, and the resource or class.
type series [2]string

// add sums values under their labels, as two samples with the same labels would fail the scrape.
func add[K ~string, V int | int64](into map[series]float64, tenant string, values map[K]V) {
	for name, v := range values {
		into[series{tenant, string(name)}] += float64(v)
	}
}

// Collect reads the tree once and reports it.
func (c collector) Collect(ch chan<- prometheus.Metric) {
	s := c.src.Stats()
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	counter := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, labels...)
	}

	gauge(nodesDesc, float64(s.Nodes))
	gauge(sessionsDesc, float64(s.Sessions))
	for class, n := range s.Waiting {
		gauge(waitingDesc, float64(n), string(class))
	}
	// Usage never decreases, so it is a counter and its rate is the rate of consumption.
	for r, v := range s.Root.Used {
		counter(rootUsedDesc, float64(v), string(r))
	}
	for r, v := range s.Root.Quotas {
		gauge(rootQuotaDesc, float64(v), string(r))
	}
	for class, v := range s.Root.Held {
		gauge(rootHeldDesc, float64(v), string(class))
	}
	for class, v := range s.Root.Limits {
		gauge(rootLimitDesc, float64(v), string(class))
	}

	used, quota := map[series]float64{}, map[series]float64{}
	held, limit := map[series]float64{}, map[series]float64{}
	for _, t := range s.Tenants {
		name := t.Name
		if name == "" {
			name = fmt.Sprintf("#%d", t.ID)
		}
		add(used, name, t.Used)
		add(quota, name, t.Quotas)
		add(held, name, t.Held)
		add(limit, name, t.Limits)
	}
	for k, v := range used {
		counter(tenantUsedDesc, v, k[0], k[1])
	}
	for k, v := range quota {
		gauge(tenantQuotaDesc, v, k[0], k[1])
	}
	for k, v := range held {
		gauge(tenantHeldDesc, v, k[0], k[1])
	}
	for k, v := range limit {
		gauge(tenantLimitDesc, v, k[0], k[1])
	}
}

var _ prometheus.Collector = collector{}

var _ Source = (*core.Engine)(nil)
