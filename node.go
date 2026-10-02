package governor

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/Avik-creator/governor/internal/gen/governor/v1"
)

// Unlimited, given as a quota or a limit to set, removes the cap instead.
const Unlimited = -1

// State is the lifecycle state of a node.
type State string

// Node states. Every state other than StateActive is final.
const (
	StateActive           State = "active"
	StateDone             State = "done"
	StateCancelled        State = "cancelled"
	StateDeadlineExceeded State = "deadline_exceeded"
)

// states maps the wire states to their names here.
var states = map[pb.State]State{
	pb.State_STATE_ACTIVE:            StateActive,
	pb.State_STATE_DONE:              StateDone,
	pb.State_STATE_CANCELLED:         StateCancelled,
	pb.State_STATE_DEADLINE_EXCEEDED: StateDeadlineExceeded,
}

// Defaults is what a node gives each new child, whatever the child's own spec asks for.
type Defaults struct {
	// Quotas replace the child's caps for the resources named here.
	Quotas map[string]int64

	// Children become the child's own defaults, for the nodes created under it.
	Children *Defaults
}

func (d *Defaults) proto() *pb.Defaults {
	if d == nil {
		return nil
	}
	return &pb.Defaults{Quotas: d.Quotas, Children: d.Children.proto()}
}

func defaultsFromProto(p *pb.Defaults) *Defaults {
	if p == nil {
		return nil
	}
	return &Defaults{Quotas: p.GetQuotas(), Children: defaultsFromProto(p.GetChildren())}
}

// Node describes one node of the tree: a tenant, a task or a subtask.
type Node struct {
	ID     uint64
	Parent uint64 // zero for the root
	Name   string
	State  State

	// Deadline is the effective deadline, and EndedAt is zero while the node is active.
	Deadline time.Time
	EndedAt  time.Time

	// Quotas and Limits are the node's own caps; Used and Held count its whole subtree.
	Quotas map[string]int64
	Used   map[string]int64
	Limits map[string]int64
	Held   map[string]int64

	// Defaults is what each new child starts with, and Children how many children there are.
	Defaults *Defaults
	Children int
}

func nodeFromProto(p *pb.Node) Node {
	return Node{
		ID:       p.GetId(),
		Parent:   p.GetParentId(),
		Name:     p.GetName(),
		State:    states[p.GetState()],
		Deadline: timeFromProto(p.GetDeadline()),
		EndedAt:  timeFromProto(p.GetEndedAt()),
		Quotas:   p.GetQuotas(),
		Used:     p.GetUsed(),
		Limits:   p.GetLimits(),
		Held:     p.GetHeld(),
		Defaults: defaultsFromProto(p.GetDefaults()),
		Children: int(p.GetChildCount()),
	}
}

// timeFromProto converts a wire time; unset is the zero time.
func timeFromProto(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

// limitToProto converts a cap to set; Unlimited becomes unset, which removes the cap.
func limitToProto(limit int64) *int64 {
	if limit == Unlimited {
		return nil
	}
	return proto.Int64(limit)
}

// Node describes a node and its children, ordered by id; id zero is the client's own scope.
func (c *Client) Node(ctx context.Context, id uint64) (Node, []Node, error) {
	resp, err := call(ctx, c, func(ctx context.Context) (*pb.GetNodeResponse, error) {
		return c.rpc.GetNode(ctx, &pb.GetNodeRequest{NodeId: id})
	})
	if err != nil {
		return Node{}, nil, err
	}
	children := make([]Node, len(resp.GetChildren()))
	for i, child := range resp.GetChildren() {
		children[i] = nodeFromProto(child)
	}
	return nodeFromProto(resp.GetNode()), children, nil
}

// SetQuota changes a node's cap for a resource; Unlimited removes it.
func (c *Client) SetQuota(ctx context.Context, id uint64, resource string, limit int64) error {
	req := &pb.SetQuotaRequest{NodeId: id, Resource: resource, Limit: limitToProto(limit)}
	_, err := call(ctx, c, func(ctx context.Context) (*pb.SetQuotaResponse, error) {
		return c.rpc.SetQuota(ctx, req)
	})
	return err
}

// SetLimit changes a node's cap for a class; Unlimited removes it.
func (c *Client) SetLimit(ctx context.Context, id uint64, class string, limit int64) error {
	req := &pb.SetLimitRequest{NodeId: id, Class: class, Limit: limitToProto(limit)}
	_, err := call(ctx, c, func(ctx context.Context) (*pb.SetLimitResponse, error) {
		return c.rpc.SetLimit(ctx, req)
	})
	return err
}

// SetDefaults changes what each new child of a node starts with; nil gives them nothing.
func (c *Client) SetDefaults(ctx context.Context, id uint64, d *Defaults) error {
	req := &pb.SetDefaultsRequest{NodeId: id, Defaults: d.proto()}
	_, err := call(ctx, c, func(ctx context.Context) (*pb.SetDefaultsResponse, error) {
		return c.rpc.SetDefaults(ctx, req)
	})
	return err
}

// CancelNode ends a node and everything under it as cancelled.
func (c *Client) CancelNode(ctx context.Context, id uint64) error {
	_, err := call(ctx, c, func(ctx context.Context) (*pb.CancelNodeResponse, error) {
		return c.rpc.CancelNode(ctx, &pb.CancelNodeRequest{NodeId: id})
	})
	return err
}
