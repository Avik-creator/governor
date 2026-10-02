package core

import "fmt"

// maxRequests caps how many request ids a session remembers; the oldest are forgotten first.
const maxRequests = 1024

// outcome is what a request that carried an id did, so that a retry can be answered without doing it again.
type outcome struct {
	kind     EventKind
	node     NodeID // the parent for a creation, the node charged or leased otherwise
	resource Resource
	amount   int64
	class    Class
	result   uint64 // the node that was created or the lease that was granted
}

// recall returns the result of an earlier request with this id, or an error if the id was used for something else.
func (s *session) recall(request string, want outcome) (uint64, bool, error) {
	got, ok := s.requests[request]
	if request == "" || !ok {
		return 0, false, nil
	}
	want.result = got.result
	if got != want {
		return 0, false, fmt.Errorf("%w: request id %q was used for another request", ErrInvalid, request)
	}
	return got.result, true, nil
}

// remember records what a request did; a request without an id is not remembered.
func (s *session) remember(request string, did outcome) {
	if request == "" {
		return
	}
	if s.requests == nil {
		s.requests = make(map[string]outcome)
	}
	s.requests[request] = did
	s.order = append(s.order, request)
	if len(s.order) > maxRequests {
		delete(s.requests, s.order[0])
		s.order = s.order[1:]
	}
}

// CreateNodeOnce is CreateNode for a request that may be repeated: the same id creates one node.
func (e *Engine) CreateNodeOnce(sid SessionID, request string, parent NodeID, spec Spec) (NodeID, uint64, error) {
	if err := spec.validate(); err != nil {
		return 0, 0, err
	}
	spec = spec.clone()
	e.mu.Lock()
	defer e.mu.Unlock()
	s, err := e.liveSession(sid)
	if err != nil {
		return 0, 0, err
	}
	did := outcome{kind: EventNodeCreated, node: parent}
	if id, ok, err := s.recall(request, did); ok || err != nil {
		return NodeID(id), e.seq, err
	}
	_, p, err := e.activeTarget(sid, parent)
	if err != nil {
		return 0, 0, err
	}
	if p.depth >= MaxDepth {
		return 0, 0, fmt.Errorf("%w: tree is deeper than %d", ErrInvalid, MaxDepth)
	}
	n := e.addNode(NodeID(e.nextID()), p, spec)
	e.emit(Event{Kind: EventNodeCreated, Node: n.id, Parent: p.id, Spec: &spec, Session: sid, Request: request})
	did.result = uint64(n.id)
	s.remember(request, did)
	return n.id, e.seq, nil
}

// ConsumeOnce is Consume for a request that may be repeated: the same id is charged once.
func (e *Engine) ConsumeOnce(sid SessionID, request string, id NodeID, r Resource, amount int64) (uint64, error) {
	if r == "" || amount <= 0 {
		return 0, fmt.Errorf("%w: bad resource or amount", ErrInvalid)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	s, err := e.liveSession(sid)
	if err != nil {
		return 0, err
	}
	did := outcome{kind: EventConsumed, node: id, resource: r, amount: amount}
	if _, ok, err := s.recall(request, did); ok || err != nil {
		return e.seq, err
	}
	_, n, err := e.activeTarget(sid, id)
	if err != nil {
		return 0, err
	}
	if err := n.check(r, amount); err != nil {
		return 0, err
	}
	n.charge(r, amount)
	e.emit(Event{Kind: EventConsumed, Node: n.id, Session: sid, Resource: r, Amount: amount, Request: request})
	s.remember(request, did)
	return e.seq, nil
}
