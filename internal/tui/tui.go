// Package tui is the terminal screen of "governor ui": it shows the tree and changes its caps.
package tui

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/Avik-creator/governor"
)

const (
	// refreshEvery is how often the node on screen is read again.
	refreshEvery = time.Second

	// callTimeout bounds one call to governord, so a dead server shows as an error.
	callTimeout = 3 * time.Second
)

// Client is what the screen needs from governord; *governor.Client is one.
type Client interface {
	Node(ctx context.Context, id uint64) (governor.Node, []governor.Node, error)
	SetQuota(ctx context.Context, id uint64, resource string, limit int64) error
	SetLimit(ctx context.Context, id uint64, class string, limit int64) error
	SetDefaults(ctx context.Context, id uint64, d *governor.Defaults) error
	CancelNode(ctx context.Context, id uint64) error
}

// Run shows the screen until the user quits or ctx is done; addr is only displayed.
func Run(ctx context.Context, client Client, addr string) error {
	_, err := tea.NewProgram(newModel(ctx, client, addr), tea.WithAltScreen(), tea.WithContext(ctx)).Run()
	// Quitting because the context ended is not a failure of the screen.
	if errors.Is(err, tea.ErrProgramKilled) && ctx.Err() != nil {
		return nil
	}
	return err
}

// loadedMsg carries the answer to one read of the node with the given id.
type loadedMsg struct {
	id       uint64
	node     governor.Node
	children []governor.Node
	err      error
}

// doneMsg reports how a change the user asked for went.
type doneMsg struct {
	err error
}

// tickMsg says it is time to read the node again.
type tickMsg struct{}

// model is the whole state of the screen.
type model struct {
	ctx    context.Context
	client Client
	addr   string
	now    func() time.Time

	path     []governor.Node // from the client's scope down to the node on screen
	want     uint64          // id of the node on screen; zero until the scope is known
	children []governor.Node // active ones first, then newest first
	cursor   int
	loaded   time.Time // when the node was last read; zero before the first answer
	err      error     // why the last read failed, shown until a read succeeds
	failed   error     // why the last change failed, shown until the next key

	form    *form          // set while caps or defaults are being edited
	confirm *governor.Node // set while asking whether to cancel this node

	width, height int
}

func newModel(ctx context.Context, client Client, addr string) model {
	return model{ctx: ctx, client: client, addr: addr, now: time.Now, width: 100, height: 30}
}

// Init reads the client's scope and starts the refresh timer.
func (m model) Init() tea.Cmd {
	return tea.Batch(m.load(), tick())
}

func tick() tea.Cmd {
	return tea.Tick(refreshEvery, func(time.Time) tea.Msg { return tickMsg{} })
}

// load reads the node on screen and its children.
func (m model) load() tea.Cmd {
	id := m.want
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(m.ctx, callTimeout)
		defer cancel()
		node, children, err := m.client.Node(ctx, id)
		return loadedMsg{id: id, node: node, children: children, err: err}
	}
}

// do runs a change the user asked for and reports how it went.
func (m model) do(change func(context.Context) error) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(m.ctx, callTimeout)
		defer cancel()
		return doneMsg{err: change(ctx)}
	}
}

// current returns the node on screen, or nil before the first answer.
func (m model) current() *governor.Node {
	if len(m.path) == 0 {
		return nil
	}
	return &m.path[len(m.path)-1]
}

// selected returns the child under the cursor, or nil if there are no children.
func (m model) selected() *governor.Node {
	if m.cursor < 0 || m.cursor >= len(m.children) {
		return nil
	}
	return &m.children[m.cursor]
}

// Update applies one message to the screen.
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
	case tickMsg:
		return m, tea.Batch(m.load(), tick())
	case loadedMsg:
		return m.loadedNode(msg)
	case doneMsg:
		m.failed, m.confirm = msg.err, nil
		// A form whose change failed stays open, so what was typed is not lost.
		if msg.err == nil {
			m.form = nil
		}
		return m, m.load()
	case tea.KeyMsg:
		m.failed = nil
		switch {
		case msg.String() == "ctrl+c":
			return m, tea.Quit
		case m.form != nil:
			return m.formKey(msg)
		case m.confirm != nil:
			return m.confirmKey(msg)
		default:
			return m.listKey(msg)
		}
	}
	return m, nil
}

// newestActiveFirst orders children with the active ones first, and the newest first among equals.
func newestActiveFirst(a, b governor.Node) int {
	if active := a.State == governor.StateActive; active != (b.State == governor.StateActive) {
		if active {
			return -1
		}
		return 1
	}
	return cmp.Compare(b.ID, a.ID)
}

// loadedNode shows a fresh reading, unless the screen has moved on to another node since.
func (m model) loadedNode(msg loadedMsg) (tea.Model, tea.Cmd) {
	if msg.id != m.want {
		return m, nil
	}
	if msg.err != nil {
		// A node that was removed while on screen leaves nothing to show but its parent.
		if errors.Is(msg.err, governor.ErrNotFound) && len(m.path) > 1 {
			return m.back()
		}
		m.err = msg.err
		return m, nil
	}
	under := m.selected()
	slices.SortFunc(msg.children, newestActiveFirst)
	// The path is replaced rather than changed, as earlier models share its array.
	m.path = append(slices.Clone(m.path[:max(0, len(m.path)-1)]), msg.node)
	m.want, m.children, m.loaded, m.err = msg.node.ID, msg.children, m.now(), nil
	// The cursor stays on the node it was on, wherever the new order puts it.
	if under != nil {
		if i := slices.IndexFunc(m.children, func(n governor.Node) bool { return n.ID == under.ID }); i >= 0 {
			m.cursor = i
		}
	}
	m.cursor = max(0, min(m.cursor, len(m.children)-1))
	return m, nil
}

// back shows the parent of the node on screen, with the cursor on the node just left.
func (m model) back() (tea.Model, tea.Cmd) {
	if len(m.path) < 2 {
		return m, nil
	}
	left := m.path[len(m.path)-1]
	m.path = m.path[:len(m.path)-1]
	m.want, m.children, m.cursor = m.path[len(m.path)-1].ID, []governor.Node{left}, 0
	return m, m.load()
}

// listKey handles a key pressed while the list of children is shown.
func (m model) listKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	child := m.selected()
	switch msg.String() {
	case "q":
		return m, tea.Quit
	case "up", "k":
		m.cursor = max(0, m.cursor-1)
	case "down", "j":
		m.cursor = max(0, min(m.cursor+1, len(m.children)-1))
	case "r":
		return m, m.load()
	case "esc", "backspace", "left", "h":
		return m.back()
	case "enter", "right", "l":
		if child != nil {
			m.path = append(slices.Clone(m.path), *child)
			m.want, m.children, m.cursor = child.ID, nil, 0
			return m, m.load()
		}
	case "e":
		if child != nil {
			m.form = capsForm(*child)
		}
	case "d":
		if node := m.current(); node != nil {
			m.form = defaultsForm(*node)
		}
	case "c":
		if child != nil && child.State == governor.StateActive {
			m.confirm = child
		}
	}
	return m, nil
}

// confirmKey handles the answer to "cancel this node?".
func (m model) confirmKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if msg.String() != "y" {
		m.confirm = nil
		return m, nil
	}
	id := m.confirm.ID
	return m, m.do(func(ctx context.Context) error { return m.client.CancelNode(ctx, id) })
}

// formKey handles a key pressed while a form is open.
func (m model) formKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// The form is edited as a copy, so an earlier model is never changed behind its back.
	f := m.form.clone()
	m.form = f
	save, closed := f.key(msg)
	switch {
	case closed:
		m.form = nil
	case save:
		return m, m.do(func(ctx context.Context) error { return f.save(ctx, m.client) })
	}
	return m, nil
}
