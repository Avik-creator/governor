package tui

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/Avik-creator/governor"
)

// fakeClient is a tree held in memory that records the changes made to it.
type fakeClient struct {
	nodes map[uint64]governor.Node
	scope uint64
	calls []string // every change, in order, as text
	err   error    // returned by every call when set
	deny  error    // returned by every change when set, while reads still work
}

// refused returns the error a change must fail with, if any.
func (c *fakeClient) refused() error {
	if c.err != nil {
		return c.err
	}
	return c.deny
}

func (c *fakeClient) Node(_ context.Context, id uint64) (governor.Node, []governor.Node, error) {
	if c.err != nil {
		return governor.Node{}, nil, c.err
	}
	if id == 0 {
		id = c.scope
	}
	node, ok := c.nodes[id]
	if !ok {
		return governor.Node{}, nil, governor.ErrNotFound
	}
	var children []governor.Node
	for _, n := range c.nodes {
		if n.Parent == id {
			children = append(children, n)
		}
	}
	return node, children, nil
}

// set records a change to one cap and applies it to the tree.
func (c *fakeClient) set(kind string, caps func(*governor.Node) *map[string]int64, id uint64, name string, limit int64) error {
	if err := c.refused(); err != nil {
		return err
	}
	c.calls = append(c.calls, fmt.Sprintf("%s(%d, %s, %d)", kind, id, name, limit))
	node := c.nodes[id]
	m := maps.Clone(*caps(&node))
	if m == nil {
		m = map[string]int64{}
	}
	if m[name] = limit; limit == governor.Unlimited {
		delete(m, name)
	}
	*caps(&node) = m
	c.nodes[id] = node
	return nil
}

func (c *fakeClient) SetQuota(_ context.Context, id uint64, resource string, limit int64) error {
	return c.set("SetQuota", func(n *governor.Node) *map[string]int64 { return &n.Quotas }, id, resource, limit)
}

func (c *fakeClient) SetLimit(_ context.Context, id uint64, class string, limit int64) error {
	return c.set("SetLimit", func(n *governor.Node) *map[string]int64 { return &n.Limits }, id, class, limit)
}

func (c *fakeClient) SetDefaults(_ context.Context, id uint64, d *governor.Defaults) error {
	if err := c.refused(); err != nil {
		return err
	}
	c.calls = append(c.calls, fmt.Sprintf("SetDefaults(%d)", id))
	node := c.nodes[id]
	node.Defaults = d
	c.nodes[id] = node
	return nil
}

func (c *fakeClient) CancelNode(_ context.Context, id uint64) error {
	if err := c.refused(); err != nil {
		return err
	}
	c.calls = append(c.calls, fmt.Sprintf("CancelNode(%d)", id))
	node := c.nodes[id]
	node.State = governor.StateCancelled
	c.nodes[id] = node
	return nil
}

var testNow = time.Date(2026, 10, 2, 14, 0, 0, 0, time.UTC)

// newFake returns a tenant with an older finished run, a newer active run and one subagent.
func newFake() *fakeClient {
	nodes := []governor.Node{
		{ID: 2, Parent: 1, Name: "codex", State: governor.StateActive, Children: 2,
			Used:     map[string]int64{"tool_calls": 537, "agents": 1},
			Defaults: &governor.Defaults{Quotas: map[string]int64{"tool_calls": 500}}},
		{ID: 3, Parent: 2, Name: "codex:old", State: governor.StateDone,
			Quotas: map[string]int64{"tool_calls": 500}, Used: map[string]int64{"tool_calls": 500}},
		{ID: 4, Parent: 2, Name: "codex:new", State: governor.StateActive, Children: 1, Deadline: testNow.Add(90 * time.Minute),
			Quotas: map[string]int64{"tool_calls": 500, "agents": 10}, Used: map[string]int64{"tool_calls": 37, "agents": 1},
			Limits: map[string]int64{"db": 4}, Held: map[string]int64{"db": 2}},
		{ID: 5, Parent: 4, Name: "agent:7f", State: governor.StateActive,
			Quotas: map[string]int64{"tool_calls": 100}, Used: map[string]int64{"tool_calls": 12}},
	}
	c := &fakeClient{nodes: make(map[uint64]governor.Node), scope: 2}
	for _, n := range nodes {
		c.nodes[n.ID] = n
	}
	return c
}

// screen drives a model the way the program would, running each command it returns at once.
type screen struct {
	t *testing.T
	m model
}

func newScreen(t *testing.T, c Client) *screen {
	t.Helper()
	m := newModel(t.Context(), c, "127.0.0.1:7600")
	m.now = func() time.Time { return testNow }
	m.width = 200
	s := &screen{t: t, m: m}
	s.run(m.load())
	return s
}

// run executes a command and feeds its message back, as the program's loop does.
func (s *screen) run(cmd tea.Cmd) {
	s.t.Helper()
	if cmd == nil {
		return
	}
	next, following := s.m.Update(cmd())
	s.m = next.(model)
	s.run(following)
}

// keys maps the names of the special keys to what the terminal sends for them.
var keys = map[string]tea.KeyType{
	"enter": tea.KeyEnter, "esc": tea.KeyEsc, "up": tea.KeyUp, "down": tea.KeyDown, "backspace": tea.KeyBackspace,
}

// press sends keys: the name of a special key, or text typed one character at a time.
func (s *screen) press(inputs ...string) {
	s.t.Helper()
	for _, input := range inputs {
		msgs := []tea.KeyMsg{{Type: keys[input]}}
		if _, special := keys[input]; !special {
			msgs = msgs[:0]
			for _, r := range input {
				msgs = append(msgs, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
			}
		}
		for _, msg := range msgs {
			next, cmd := s.m.Update(msg)
			s.m = next.(model)
			s.run(cmd)
		}
	}
}

// wantView checks that the screen shows every fragment.
func (s *screen) wantView(fragments ...string) {
	s.t.Helper()
	view := s.m.View()
	for _, f := range fragments {
		if !strings.Contains(view, f) {
			s.t.Errorf("the screen does not show %q:\n%s", f, view)
		}
	}
}

func (s *screen) wantCalls(c *fakeClient, want ...string) {
	s.t.Helper()
	if strings.Join(c.calls, "; ") != strings.Join(want, "; ") {
		s.t.Errorf("calls = %v, want %v", c.calls, want)
	}
}

func TestListShowsChildren(t *testing.T) {
	s := newScreen(t, newFake())
	s.wantView(
		"127.0.0.1:7600", "updated 14:00:00",
		"codex", "tool_calls 537/∞", "agents 1/∞",
		"NAME", "STATE", "ENDS IN", "CHILDREN", "db held",
		"37/500", "500/500", "1/10", "2/4", "1h30m", "done",
		"New children start with: tool_calls 500",
	)
	// The active run comes first although it is the newer one, and the cursor starts on it.
	view := s.m.View()
	if strings.Index(view, "codex:new") > strings.Index(view, "codex:old") {
		t.Errorf("the finished run is listed before the active one:\n%s", view)
	}
	if got := s.m.selected(); got == nil || got.Name != "codex:new" {
		t.Errorf("selected = %+v, want the active run", got)
	}
}

func TestOpenAndBack(t *testing.T) {
	c := newFake()
	s := newScreen(t, c)
	s.press("enter")
	s.wantView("codex › codex:new", "agent:7f", "12/100")

	// Going back puts the cursor on the node that was left, not on the first row.
	s.press("up", "esc")
	if got := s.m.selected(); got == nil || got.Name != "codex:new" {
		t.Errorf("selected after going back = %+v, want the run that was open", got)
	}
	s.press("esc")
	if len(s.m.path) != 1 {
		t.Errorf("went above the client's scope: path = %v", s.m.path)
	}

	// A node removed while it is open leaves the screen on its parent.
	s.press("enter")
	delete(c.nodes, 4)
	s.press("r")
	if len(s.m.path) != 1 || s.m.current().Name != "codex" || s.m.err != nil {
		t.Errorf("after the open node was removed: path = %v, err = %v", s.m.path, s.m.err)
	}
}

func TestEditCaps(t *testing.T) {
	c := newFake()
	s := newScreen(t, c)
	s.press("e")
	s.wantView("Caps of codex:new", "agents", "(1 used)", "tool_calls", "(37 used)", "db", "(2 held)", "+ add")

	// The form lists agents, tool_calls, an add line, db and an add line.
	s.press("backspace", "backspace", "25")
	s.press("down", "backspace", "backspace", "backspace")
	s.wantView("no cap", "changed")
	// A new limit is named on the add line of the limits section, then given a value.
	s.press("down", "down", "down", "enter", "http!", "enter", "8")
	s.wantView("http")
	s.press("enter")

	s.wantCalls(c, "SetQuota(4, agents, 25)", "SetQuota(4, tool_calls, -1)", "SetLimit(4, http, 8)")
	if s.m.form != nil {
		t.Error("the form is still open after saving")
	}
	s.wantView("1/25", "37/∞", "http held", "0/8")
}

func TestEditCapsDiscardAndFailure(t *testing.T) {
	c := newFake()
	s := newScreen(t, c)
	s.press("e", "9", "esc")
	if s.m.form != nil || len(c.calls) != 0 {
		t.Errorf("esc left the form open or made calls: %v", c.calls)
	}

	// A failed save keeps the form open with what was typed, and the reason outlasts the next read.
	s.press("e", "9")
	c.deny = governor.ErrForbidden
	s.press("enter")
	if s.m.form == nil {
		t.Fatal("the form closed although saving failed")
	}
	s.wantView("error: agents: governor: forbidden", "109")
	c.deny = nil
	s.press("enter")
	s.wantCalls(c, "SetQuota(4, agents, 109)")
	if strings.Contains(s.m.View(), "error:") {
		t.Errorf("the error is still shown after the save succeeded:\n%s", s.m.View())
	}

	// A refused cancel is reported too, and the question goes away.
	c.deny = governor.ErrForbidden
	s.press("c", "y")
	s.wantView("error: governor: forbidden")
	if s.m.confirm != nil {
		t.Error("still asking after the cancel was refused")
	}
}

func TestEditDefaults(t *testing.T) {
	c := newFake()
	s := newScreen(t, c)
	s.press("d")
	s.wantView("What codex gives each new child", "tool_calls", "500")

	// Change the run's budget, add a second quota, and give each subagent a budget.
	s.press("backspace", "backspace", "backspace", "200")
	s.press("down", "enter", "agents", "enter", "5")
	s.press("down", "down", "enter", "tool_calls", "enter", "50", "enter")
	s.wantCalls(c, "SetDefaults(2)")
	got := c.nodes[2].Defaults
	if got == nil || !maps.Equal(got.Quotas, map[string]int64{"tool_calls": 200, "agents": 5}) ||
		got.Children == nil || !maps.Equal(got.Children.Quotas, map[string]int64{"tool_calls": 50}) {
		t.Errorf("defaults = %+v", got)
	}
	s.wantView("New children start with: agents 5, tool_calls 200", "their children: tool_calls 50")

	// Emptying every field clears the defaults.
	s.press("d", "backspace", "down", "backspace", "backspace", "backspace")
	s.press("down", "down", "backspace", "backspace", "enter")
	if c.nodes[2].Defaults != nil {
		t.Errorf("defaults = %+v after every field was emptied, want none", c.nodes[2].Defaults)
	}
	s.wantView("nothing set")
}

func TestNewFieldNames(t *testing.T) {
	s := newScreen(t, newFake())
	s.press("d", "down", "enter")
	// An empty name and a name already in the section are not accepted.
	s.press("enter")
	if !s.m.form.naming {
		t.Error("an empty name was accepted")
	}
	s.press("tool_calls", "enter")
	if !s.m.form.naming || len(s.m.form.fields) != 1 {
		t.Errorf("a name already in the section was accepted: %+v", s.m.form.fields)
	}
	s.press("esc")
	if s.m.form == nil || s.m.form.naming {
		t.Error("esc while naming should only stop the naming")
	}
}

func TestCancelAsksFirst(t *testing.T) {
	c := newFake()
	s := newScreen(t, c)
	s.press("c")
	s.wantView("Cancel codex:new and everything under it?")
	s.press("n")
	s.wantCalls(c)
	s.press("c", "y")
	s.wantCalls(c, "CancelNode(4)")
	s.wantView("cancelled")

	// A node that has ended cannot be cancelled again.
	s.press("c")
	if s.m.confirm != nil {
		t.Error("asked to cancel a node that has already ended")
	}
}

func TestFailuresAreShown(t *testing.T) {
	c := newFake()
	s := newScreen(t, c)
	c.err = errors.New("connection refused")
	s.press("r")
	// What was read before stays on screen under the error.
	s.wantView("error: connection refused", "codex:new")
	c.err = nil
	s.press("r")
	if strings.Contains(s.m.View(), "error:") {
		t.Errorf("the error is still shown after a read succeeded:\n%s", s.m.View())
	}

	// An answer about a node the screen has since left is ignored.
	next, _ := s.m.Update(loadedMsg{id: 99, node: governor.Node{ID: 99, Name: "stale"}})
	if next.(model).current().Name != "codex" {
		t.Error("a stale answer replaced the node on screen")
	}
}

func TestLongListsScroll(t *testing.T) {
	c := newFake()
	for id := uint64(100); id < 160; id++ {
		c.nodes[id] = governor.Node{ID: id, Parent: 2, Name: fmt.Sprint("run-", id), State: governor.StateDone}
	}
	s := newScreen(t, c)
	next, _ := s.m.Update(tea.WindowSizeMsg{Width: 60, Height: 20})
	s.m = next.(model)
	for range 40 {
		s.press("down")
	}
	view := s.m.View()
	if lines := strings.Count(view, "\n") + 1; lines > 20 {
		t.Errorf("the screen is %d lines tall in a terminal of 20:\n%s", lines, view)
	}
	if got := s.m.selected(); got == nil || !strings.Contains(view, got.Name) {
		t.Errorf("the row under the cursor is not on screen:\n%s", view)
	}
	for line := range strings.SplitSeq(view, "\n") {
		if w := len([]rune(line)); w > 60 {
			t.Errorf("a line is %d wide in a terminal of 60: %q", w, line)
		}
	}
}
