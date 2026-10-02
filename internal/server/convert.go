package server

import (
	"fmt"
	"math"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Avik-creator/governor/internal/core"
	pb "github.com/Avik-creator/governor/internal/gen/governor/v1"
)

// toInt converts a wire integer, rejecting values an int cannot hold.
func toInt(field string, v int64) (int, error) {
	if v < math.MinInt || v > math.MaxInt {
		return 0, fmt.Errorf("%w: %s is out of range", core.ErrInvalid, field)
	}
	return int(v), nil
}

// toDuration converts a wire duration; unset means zero.
func toDuration(field string, d *durationpb.Duration) (time.Duration, error) {
	if d == nil {
		return 0, nil
	}
	if err := d.CheckValid(); err != nil {
		return 0, fmt.Errorf("%w: %s: %v", core.ErrInvalid, field, err)
	}
	return d.AsDuration(), nil
}

// toTimestamp converts a time for the wire; the zero time is left unset.
func toTimestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// specFromProto converts a wire Spec; a nil Spec has no limits of its own.
func specFromProto(p *pb.Spec) (core.Spec, error) {
	spec := core.Spec{Name: p.GetName()}
	var err error
	if spec.Weight, err = toInt("weight", p.GetWeight()); err != nil {
		return core.Spec{}, err
	}
	if spec.Priority, err = toInt("priority", p.GetPriority()); err != nil {
		return core.Spec{}, err
	}
	if d := p.GetDeadline(); d != nil {
		if err := d.CheckValid(); err != nil {
			return core.Spec{}, fmt.Errorf("%w: deadline: %v", core.ErrInvalid, err)
		}
		spec.Deadline = d.AsTime()
	}
	if len(p.GetQuotas()) > 0 {
		spec.Quotas = make(map[core.Resource]int64, len(p.GetQuotas()))
		for r, limit := range p.GetQuotas() {
			spec.Quotas[core.Resource(r)] = limit
		}
	}
	if len(p.GetLimits()) > 0 {
		spec.Limits = make(map[core.Class]int, len(p.GetLimits()))
		for c, limit := range p.GetLimits() {
			if spec.Limits[core.Class(c)], err = toInt("limit "+c, limit); err != nil {
				return core.Spec{}, err
			}
		}
	}
	return spec, nil
}

// defaultsFromProto converts wire defaults; nil stays nil, which clears a node's defaults.
func defaultsFromProto(p *pb.Defaults) *core.Defaults {
	if p == nil {
		return nil
	}
	d := &core.Defaults{Children: defaultsFromProto(p.GetChildren())}
	if len(p.GetQuotas()) > 0 {
		d.Quotas = make(map[core.Resource]int64, len(p.GetQuotas()))
		for r, limit := range p.GetQuotas() {
			d.Quotas[core.Resource(r)] = limit
		}
	}
	return d
}

func defaultsToProto(d *core.Defaults) *pb.Defaults {
	if d == nil {
		return nil
	}
	return &pb.Defaults{Quotas: toWire(d.Quotas), Children: defaultsToProto(d.Children)}
}

// toWire converts a map of caps or counts to its wire form.
func toWire[K ~string, V ~int | ~int64](m map[K]V) map[string]int64 {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[string(k)] = int64(v)
	}
	return out
}

func nodeToProto(n core.NodeInfo) *pb.Node {
	return &pb.Node{
		Id:         uint64(n.ID),
		ParentId:   uint64(n.Parent),
		Name:       n.Name,
		State:      stateToProto(n.State),
		Deadline:   toTimestamp(n.Deadline),
		EndedAt:    toTimestamp(n.EndedAt),
		Quotas:     toWire(n.Quotas),
		Used:       toWire(n.Used),
		Limits:     toWire(n.Limits),
		Held:       toWire(n.Held),
		Defaults:   defaultsToProto(n.Defaults),
		ChildCount: int64(n.Children),
	}
}

func stateToProto(state core.State) pb.State {
	switch state {
	case core.StateActive:
		return pb.State_STATE_ACTIVE
	case core.StateDone:
		return pb.State_STATE_DONE
	case core.StateCancelled:
		return pb.State_STATE_CANCELLED
	case core.StateDeadlineExceeded:
		return pb.State_STATE_DEADLINE_EXCEEDED
	default:
		return pb.State_STATE_UNSPECIFIED
	}
}

func denialToProto(d *core.DeniedError) *pb.Denial {
	return &pb.Denial{
		NodeId:          uint64(d.Node),
		Name:            d.Name,
		Resource:        string(d.Resource),
		Used:            d.Used,
		Limit:           d.Limit,
		Requested:       d.Requested,
		TopConsumerId:   uint64(d.TopConsumer),
		TopConsumerName: d.TopConsumerName,
		TopConsumerUsed: d.TopConsumerUsed,
	}
}
