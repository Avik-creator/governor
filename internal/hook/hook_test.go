package hook

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/Avik-creator/governor/internal/core"
	pb "github.com/Avik-creator/governor/internal/gen/governor/v1"
	"github.com/Avik-creator/governor/internal/server"
)

const testKey = "key-tenant"

// backend is a governord listening on a local port, with one tenant.
type backend struct {
	t      *testing.T
	engine *core.Engine
	clock  *core.ManualClock
	admin  core.SessionID
	tenant core.NodeID
	addr   string
	grpc   *grpc.Server
}

func newBackend(t *testing.T, tenant core.Spec) *backend {
	t.Helper()
	b := &backend{t: t, clock: core.NewManualClock(time.Now())}
	var err error
	if b.engine, err = core.New(core.Config{Clock: b.clock}); err != nil {
		t.Fatalf("core.New: %v", err)
	}
	setup, _, _, err := b.engine.OpenSession(core.RootID, 0)
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	if b.tenant, _, err = b.engine.CreateNode(setup, core.RootID, tenant); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	srv, err := server.New(t.Context(), server.Config{
		Engine:    b.engine,
		Committer: server.NopCommitter{},
		Tokens:    server.NopTokenStore{},
		Keys:      map[string]core.NodeID{testKey: b.tenant},
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if b.admin, _, _, err = b.engine.OpenSession(core.RootID, 0); err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	b.addr = lis.Addr().String()
	b.grpc = grpc.NewServer(grpc.UnaryInterceptor(srv.UnaryInterceptor), grpc.StreamInterceptor(srv.StreamInterceptor))
	pb.RegisterGovernorServiceServer(b.grpc, srv)
	go func() { _ = b.grpc.Serve(lis) }()
	t.Cleanup(func() {
		b.grpc.Stop()
		srv.Close()
	})
	return b
}

// config returns a hook configuration for this backend with the given budgets.
func (b *backend) config(toolCalls, agents, agentToolCalls int64) Config {
	return Config{
		Addr: b.addr, APIKey: testKey, Source: "claude",
		ToolCalls: toolCalls, Agents: agents, AgentToolCalls: agentToolCalls,
		TTL: time.Hour, Timeout: 5 * time.Second,
	}
}

// children lists the nodes under a parent by name.
func (b *backend) children(parent core.NodeID) map[string]core.Child {
	b.t.Helper()
	list, err := b.engine.Children(b.admin, parent)
	if err != nil {
		b.t.Fatalf("Children: %v", err)
	}
	out := make(map[string]core.Child, len(list))
	for _, c := range list {
		out[c.Name] = c
	}
	return out
}

// input builds the JSON a CLI sends on stdin, with fields this package must ignore.
func input(event, session, toolUse, agent string) *strings.Reader {
	extra := ""
	if agent != "" {
		extra = fmt.Sprintf(`, "agent_id": %q, "agent_type": "Explore"`, agent)
	}
	return strings.NewReader(fmt.Sprintf(`{
		"session_id": %q, "hook_event_name": %q, "tool_use_id": %q,
		"transcript_path": "/tmp/t.jsonl", "cwd": "/work", "permission_mode": "default",
		"tool_name": "Bash", "tool_input": {"command": "ls"}%s
	}`, session, event, toolUse, extra))
}

// wantBlocked fails unless err is a refusal whose reason mentions every fragment.
func wantBlocked(t *testing.T, err error, fragments ...string) {
	t.Helper()
	if _, ok := errors.AsType[*Blocked](err); !ok {
		t.Fatalf("Run = %v, want a *Blocked", err)
	}
	for _, f := range fragments {
		if !strings.Contains(err.Error(), f) {
			t.Errorf("reason %q does not mention %q", err, f)
		}
	}
}

func TestRunBudgetsToolCalls(t *testing.T) {
	b := newBackend(t, core.Spec{})
	cfg := b.config(3, 0, 0)
	for i := range 3 {
		if err := Run(t.Context(), cfg, input(preToolUse, "s1", fmt.Sprint("tool-", i), "")); err != nil {
			t.Fatalf("tool call %d: %v", i, err)
		}
	}
	// The same tool call reported again is not charged again.
	if err := Run(t.Context(), cfg, input(preToolUse, "s1", "tool-2", "")); err != nil {
		t.Errorf("repeat of a tool call: %v", err)
	}
	err := Run(t.Context(), cfg, input(preToolUse, "s1", "tool-3", ""))
	wantBlocked(t, err, "tool_calls", `"claude:s1"`, "3 of 3", "Do not retry")

	// Another session of the same CLI, and the same session id from another CLI, have their own budgets.
	if err := Run(t.Context(), cfg, input(preToolUse, "s2", "tool-0", "")); err != nil {
		t.Errorf("first tool call of another session: %v", err)
	}
	codex := cfg
	codex.Source = "codex"
	if err := Run(t.Context(), codex, input(preToolUse, "s1", "tool-0", "")); err != nil {
		t.Errorf("first tool call from another CLI: %v", err)
	}
	runs := b.children(b.tenant)
	if len(runs) != 3 || runs["claude:s1"].ID == 0 || runs["claude:s2"].ID == 0 || runs["codex:s1"].ID == 0 {
		t.Errorf("runs = %v, want claude:s1, claude:s2 and codex:s1", runs)
	}
}

func TestRunBudgetsSubagents(t *testing.T) {
	b := newBackend(t, core.Spec{})
	cfg := b.config(100, 2, 2)
	call := func(agent, toolUse string) error {
		return Run(t.Context(), cfg, input(preToolUse, "s1", toolUse, agent))
	}

	// Each subagent has its own tool call budget, and the run pays one agents unit for each.
	for _, agent := range []string{"a1", "a2"} {
		for i := range 2 {
			if err := call(agent, fmt.Sprint(agent, "-tool-", i)); err != nil {
				t.Fatalf("subagent %s tool call %d: %v", agent, i, err)
			}
		}
		wantBlocked(t, call(agent, agent+"-tool-2"), "tool_calls", `"agent:`+agent+`"`, "2 of 2")
	}

	// A third subagent is over the run's budget: it gets no node, however often it tries.
	for i := range 3 {
		wantBlocked(t, call("a3", fmt.Sprint("a3-tool-", i)), "agents", `"claude:s1"`, "2 of 2")
	}
	run := b.children(b.tenant)["claude:s1"]
	agents := b.children(run.ID)
	if len(agents) != 2 || agents["agent:a1"].ID == 0 || agents["agent:a2"].ID == 0 {
		t.Errorf("subagent nodes = %v, want only agent:a1 and agent:a2", agents)
	}
	// The main thread is still inside its own budget.
	if err := call("", "main-tool-0"); err != nil {
		t.Errorf("tool call on the main thread: %v", err)
	}
}

func TestRunConcurrentCalls(t *testing.T) {
	b := newBackend(t, core.Spec{})
	cfg := b.config(10, 0, 0)
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		allowed int
	)
	// Parallel tool calls each start their own hook process; they must share one run.
	for i := range 25 {
		wg.Go(func() {
			if err := Run(t.Context(), cfg, input(preToolUse, "s1", fmt.Sprint("tool-", i), "")); err == nil {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if allowed != 10 {
		t.Errorf("%d tool calls were allowed, want 10", allowed)
	}
	if runs := b.children(b.tenant); len(runs) != 1 {
		t.Errorf("%d runs were created, want 1", len(runs))
	}
}

func TestRunEnds(t *testing.T) {
	b := newBackend(t, core.Spec{})
	cfg := b.config(0, 0, 0)
	if err := Run(t.Context(), cfg, input(preToolUse, "s1", "tool-0", "")); err != nil {
		t.Fatalf("first tool call: %v", err)
	}

	// An operator's cancel sticks: the run is not replaced by a fresh one.
	run := b.children(b.tenant)["claude:s1"]
	if _, err := b.engine.Cancel(b.admin, run.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	wantBlocked(t, Run(t.Context(), cfg, input(preToolUse, "s1", "tool-1", "")), "this run has ended")
	wantBlocked(t, Run(t.Context(), cfg, input(preToolUse, "s1", "tool-2", "a1")), "this run has ended")
	if runs := b.children(b.tenant); len(runs) != 1 {
		t.Errorf("%d runs exist after a cancel, want 1", len(runs))
	}

	// The time limit counts from the run's first tool call.
	if err := Run(t.Context(), cfg, input(preToolUse, "s2", "tool-0", "")); err != nil {
		t.Fatalf("first tool call: %v", err)
	}
	b.clock.Advance(59 * time.Minute)
	if err := Run(t.Context(), cfg, input(preToolUse, "s2", "tool-1", "")); err != nil {
		t.Errorf("tool call inside the time limit: %v", err)
	}
	// The hook stamps the deadline from the wall clock, a moment after the engine's clock was set.
	b.clock.Advance(2 * time.Minute)
	wantBlocked(t, Run(t.Context(), cfg, input(preToolUse, "s2", "tool-2", "")), "this run has ended")
}

func TestRunHonoursTenantQuota(t *testing.T) {
	b := newBackend(t, core.Spec{Name: "team", Quotas: map[core.Resource]int64{ResourceToolCalls: 2}})
	cfg := b.config(0, 0, 0)
	// Two sessions with no cap of their own still share the tenant's.
	for i, session := range []string{"s1", "s2"} {
		if err := Run(t.Context(), cfg, input(preToolUse, session, "tool-0", "")); err != nil {
			t.Fatalf("tool call %d: %v", i, err)
		}
	}
	wantBlocked(t, Run(t.Context(), cfg, input(preToolUse, "s3", "tool-0", "")), "tool_calls", `"team"`, "2 of 2")
}

func TestRunIgnoresOtherEvents(t *testing.T) {
	// Nothing listens here, so reaching governord would be a refusal.
	cfg := Config{Addr: "127.0.0.1:1", APIKey: testKey, Source: "claude", Timeout: time.Second}
	for _, event := range []string{"PostToolUse", "SessionStart", "SessionEnd", "SubagentStop", "Stop"} {
		if err := Run(t.Context(), cfg, input(event, "s1", "tool-0", "")); err != nil {
			t.Errorf("%s: %v, want it ignored", event, err)
		}
	}
}

func TestRunFailsClosed(t *testing.T) {
	b := newBackend(t, core.Spec{})
	cfg := b.config(0, 0, 0)
	tests := []struct {
		name   string
		change func(*Config)
		stdin  *strings.Reader
		want   string
	}{
		{"malformed input", func(*Config) {}, strings.NewReader("{not json"), "not valid JSON"},
		{"no session id", func(*Config) {}, input(preToolUse, "", "tool-0", ""), "no session_id"},
		{"no API key", func(c *Config) { c.APIKey = "" }, input(preToolUse, "s1", "tool-0", ""), "no API key"},
		{"wrong API key", func(c *Config) { c.APIKey = "nope" }, input(preToolUse, "s1", "tool-0", ""), "does not accept"},
		{"unreadable TLS settings", func(c *Config) {
			c.Credentials = func() (credentials.TransportCredentials, error) { return nil, errors.New("no such file") }
		}, input(preToolUse, "s1", "tool-0", ""), "TLS settings"},
		{"governord unreachable", func(c *Config) { c.Addr, c.Timeout = "127.0.0.1:1", time.Second },
			input(preToolUse, "s1", "tool-0", ""), "gave no answer"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := cfg
			tc.change(&c)
			wantBlocked(t, Run(t.Context(), c, tc.stdin), tc.want)
		})
	}
	if runs := b.children(b.tenant); len(runs) != 0 {
		t.Errorf("refused calls created %d runs", len(runs))
	}

	// governord going away mid-run blocks the next tool call rather than letting it through.
	if err := Run(t.Context(), cfg, input(preToolUse, "s1", "tool-0", "")); err != nil {
		t.Fatalf("tool call: %v", err)
	}
	b.grpc.Stop()
	cfg.Timeout = time.Second
	wantBlocked(t, Run(t.Context(), cfg, input(preToolUse, "s1", "tool-1", "")), "gave no answer")
}
