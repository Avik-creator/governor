// Package hook governs an agent CLI, such as Claude Code or Codex, through its PreToolUse hook.
package hook

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/Avik-creator/governor/internal/gen/governor/v1"
)

// Quotas a run is charged against; configure tenant caps under the same names.
const (
	ResourceToolCalls = "tool_calls"
	ResourceAgents    = "agents"
)

// preToolUse is the only event that can refuse a tool in both Claude Code and Codex.
const preToolUse = "PreToolUse"

// Config is how one hook invocation reaches governord and what budget a run gets.
type Config struct {
	// Addr and APIKey reach governord as one tenant.
	Addr   string
	APIKey string

	// Source names the CLI, so runs of different tools never share a task.
	Source string

	// ToolCalls caps the tool calls of one run; zero sets no cap of its own.
	ToolCalls int64

	// Agents caps the subagents one run may use; zero sets no cap of its own.
	Agents int64

	// AgentToolCalls caps the tool calls of each subagent; zero sets no cap of its own.
	AgentToolCalls int64

	// TTL is how long after its first tool call a run is refused everything.
	TTL time.Duration

	// Timeout bounds the whole invocation, so the hook answers before the CLI gives up on it.
	Timeout time.Duration

	// Dial adds gRPC dial options, such as transport credentials.
	Dial []grpc.DialOption
}

// event is the part of the hook input this package reads; both CLIs send these fields.
type event struct {
	SessionID string `json:"session_id"`
	Name      string `json:"hook_event_name"`
	ToolUseID string `json:"tool_use_id"`
	AgentID   string `json:"agent_id"`
}

// Blocked is why a tool must not run; its text is written for the model to read.
type Blocked struct {
	reason string
}

func (b *Blocked) Error() string {
	return "Governor blocked this tool: " + b.reason
}

func blocked(format string, args ...any) error {
	return &Blocked{reason: fmt.Sprintf(format, args...)}
}

// Run handles one hook event read from stdin; a non-nil error means the tool must not run.
func Run(ctx context.Context, cfg Config, stdin io.Reader) error {
	var ev event
	if err := json.NewDecoder(stdin).Decode(&ev); err != nil {
		return blocked("the hook input is not valid JSON (%v).", err)
	}
	// Other events cannot refuse anything, so they need no call.
	if ev.Name != preToolUse {
		return nil
	}
	switch {
	case ev.SessionID == "":
		return blocked("the hook input has no session_id.")
	case cfg.APIKey == "":
		return blocked("no API key is set for the hook.")
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	dial := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, cfg.Dial...)
	conn, err := grpc.NewClient(cfg.Addr, dial...)
	if err != nil {
		return unreachable(cfg.Addr, err)
	}
	defer conn.Close()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+cfg.APIKey)
	if err := charge(ctx, pb.NewGovernorServiceClient(conn), cfg, ev); err != nil {
		return explain(cfg.Addr, err)
	}
	return nil
}

// charge finds or creates the run's node, and the subagent's if any, and charges one tool call.
func charge(ctx context.Context, rpc pb.GovernorServiceClient, cfg Config, ev event) error {
	// The run is found by name on every call, so the hook keeps no state between calls.
	run := &pb.Spec{
		Name:     cfg.Source + ":" + ev.SessionID,
		Quotas:   quotas(map[string]int64{ResourceToolCalls: cfg.ToolCalls, ResourceAgents: cfg.Agents}),
		Deadline: timestamppb.New(time.Now().Add(cfg.TTL)),
	}
	task, err := rpc.EnsureNode(ctx, &pb.EnsureNodeRequest{Spec: run})
	if err != nil {
		return err
	}
	node := task.GetNodeId()
	if ev.AgentID != "" {
		// A new subagent costs the run one agents unit, taken together with its node.
		subagent := quotas(map[string]int64{ResourceToolCalls: cfg.AgentToolCalls})
		agent, err := rpc.EnsureNode(ctx, &pb.EnsureNodeRequest{
			ParentId:       node,
			Spec:           &pb.Spec{Name: "agent:" + ev.AgentID, Quotas: subagent},
			ChargeResource: ResourceAgents,
			ChargeAmount:   1,
		})
		if err != nil {
			return err
		}
		node = agent.GetNodeId()
	}
	// The tool call's id makes a repeated hook for the same call free; the run's name keeps ids apart.
	requestID := ""
	if ev.ToolUseID != "" {
		requestID = run.GetName() + "/" + ev.ToolUseID
	}
	_, err = rpc.Consume(ctx, &pb.ConsumeRequest{
		RequestId: requestID,
		NodeId:    node,
		Resource:  ResourceToolCalls,
		Amount:    1,
	})
	return err
}

// quotas drops the caps of zero, which mean the node sets no cap of its own.
func quotas(caps map[string]int64) map[string]int64 {
	maps.DeleteFunc(caps, func(_ string, limit int64) bool { return limit <= 0 })
	return caps
}

// explain turns an RPC error into the reason the model is shown.
func explain(addr string, err error) error {
	for _, d := range status.Convert(err).Details() {
		detail, ok := d.(*pb.ErrorDetail)
		if !ok {
			continue
		}
		if den := detail.GetDenial(); den != nil {
			return blocked("the %s budget of %q is spent (%d of %d used). "+
				"Do not retry; stop and tell the user the budget is exhausted.",
				den.GetResource(), den.GetName(), den.GetUsed(), den.GetLimit())
		}
		switch detail.GetReason() {
		case pb.Reason_REASON_CLOSED:
			return blocked("this run has ended: it was cancelled or its time limit passed. " +
				"Do not retry; stop and tell the user.")
		case pb.Reason_REASON_SESSION_EXPIRED, pb.Reason_REASON_BAD_API_KEY:
			return blocked("governord does not accept the hook's API key.")
		}
		// Any other verdict is a refusal too, shown as governord worded it.
		return blocked("governord refused the call (%s).", status.Convert(err).Message())
	}
	return unreachable(addr, err)
}

// unreachable is the reason when governord gave no verdict; tools stay blocked, which fails closed.
func unreachable(addr string, err error) error {
	return blocked("governord at %s gave no answer (%v). Tools stay blocked until it does.",
		addr, status.Convert(err).Message())
}
