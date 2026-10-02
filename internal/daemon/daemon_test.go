package daemon

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/Avik-creator/governor/internal/config"
	"github.com/Avik-creator/governor/internal/core"
	pb "github.com/Avik-creator/governor/internal/gen/governor/v1"
)

// startDaemon runs a governord that keeps nothing on disk and stops it when the test ends.
func startDaemon(t *testing.T, cfg *config.Config) *Daemon {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	d, err := Start(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		if err := d.Wait(); err != nil {
			t.Errorf("Wait: %v", err)
		}
	})
	return d
}

func testConfig(metricsListen string) *config.Config {
	return &config.Config{
		Listen:        "127.0.0.1:0",
		MetricsListen: metricsListen,
		ReapInterval:  config.DefaultReapInterval,
		DrainTimeout:  config.DefaultDrainTimeout,
		NodeRetention: config.DefaultNodeRetention,
		SnapshotEvery: config.DefaultSnapshotEvery,
		Root:          config.Caps{Limits: map[core.Class]int{"db": 4}},
		Tenants: []config.Tenant{
			{Name: "a", APIKey: "key-a", Caps: config.Caps{Quotas: map[core.Resource]int64{"http": 10}}},
		},
	}
}

// scrape fetches the metrics as Prometheus would.
func scrape(t *testing.T, addr string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/metrics", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("scrape: status %d, %v", resp.StatusCode, err)
	}
	return string(body)
}

func TestMetricsAreServed(t *testing.T) {
	d := startDaemon(t, testConfig("127.0.0.1:0"))
	conn, err := grpc.NewClient(d.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer conn.Close()
	client := pb.NewGovernorServiceClient(conn)
	as := func(secret string) context.Context {
		return metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer "+secret)
	}
	session, err := client.OpenSession(as("key-a"), &pb.OpenSessionRequest{})
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	// Two charges fit under the tenant's quota of 10, and the third is refused.
	for range 3 {
		charge := &pb.ConsumeRequest{NodeId: session.GetScopeId(), Resource: "http", Amount: 4}
		_, _ = client.Consume(as(session.GetSessionToken()), charge)
	}

	got := scrape(t, d.MetricsAddr())
	for _, line := range []string{
		`governor_grpc_requests_total{code="OK",method="Consume"} 2`,
		`governor_grpc_requests_total{code="ResourceExhausted",method="Consume"} 1`,
		`governor_grpc_request_duration_seconds_count{method="Consume"} 3`,
		`governor_events_total{kind="consumed"} 2`,
		`governor_tenant_quota_used_total{resource="http",tenant="a"} 8`,
		`governor_tenant_quota_limit{resource="http",tenant="a"} 10`,
		`governor_root_lease_limit{class="db"} 4`,
		`go_goroutines `,
	} {
		if !strings.Contains(got, line) {
			t.Errorf("the metrics lack %q", line)
		}
	}
	// The key that authenticates a call must never become a label.
	for _, secret := range []string{"key-a", session.GetSessionToken()} {
		if strings.Contains(got, secret) {
			t.Error("the metrics contain a key or a session token")
		}
	}
}

func TestMetricsAreOffByDefault(t *testing.T) {
	d := startDaemon(t, testConfig(""))
	if addr := d.MetricsAddr(); addr != "" {
		t.Errorf("MetricsAddr() = %q, want none", addr)
	}
}

func TestMetricsAddressInUse(t *testing.T) {
	first := startDaemon(t, testConfig("127.0.0.1:0"))
	if _, err := Start(t.Context(), testConfig(first.MetricsAddr())); err == nil {
		t.Fatal("Start on a metrics address in use succeeded")
	}
}
