package tui

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/Avik-creator/governor"
)

const (
	// maxName is the widest a node's name is shown before it is cut short.
	maxName = 44

	// chrome is how many lines of the list screen are not rows of the table.
	chrome = 10

	// nearlySpent is the share of a cap from which its usage is shown as a warning.
	nearlySpent = 0.8

	noCap = "∞"
)

// Colours come from the terminal's own palette, so they suit light and dark themes.
var (
	bold    = lipgloss.NewStyle().Bold(true)
	dim     = lipgloss.NewStyle().Faint(true)
	cursor  = lipgloss.NewStyle().Reverse(true)
	good    = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	warning = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	bad     = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
)

// label names a node for a person: its name, or its id if it has none.
func label(n governor.Node) string {
	if n.Name != "" {
		return n.Name
	}
	if n.Parent == 0 {
		return "root"
	}
	return fmt.Sprintf("#%d", n.ID)
}

// cut shortens s to at most width characters, marking that it was cut.
func cut(s string, width int) string {
	if r := []rune(s); len(r) > width {
		return string(r[:max(0, width-1)]) + "…"
	}
	return s
}

// stateText is how a state is written on screen.
func stateText(s governor.State) string {
	if s == governor.StateDeadlineExceeded {
		return "timed out"
	}
	return string(s)
}

func stateStyle(s governor.State) lipgloss.Style {
	switch s {
	case governor.StateActive:
		return good
	case governor.StateDeadlineExceeded:
		return bad
	default:
		return dim
	}
}

// usage writes "used/cap" and picks the style that says how close to the cap it is.
func usage(used int64, limit int64, capped bool) (string, lipgloss.Style) {
	if !capped {
		return fmt.Sprintf("%d/%s", used, noCap), lipgloss.NewStyle()
	}
	text := fmt.Sprintf("%d/%d", used, limit)
	switch {
	case used >= limit:
		return text, bad
	case float64(used) >= nearlySpent*float64(limit):
		return text, warning
	default:
		return text, lipgloss.NewStyle()
	}
}

// remaining writes how long is left until a deadline, to the two largest units.
func remaining(d time.Duration) string {
	switch {
	case d <= 0:
		return "now"
	case d >= 48*time.Hour:
		return fmt.Sprintf("%dd%dh", d/(24*time.Hour), d%(24*time.Hour)/time.Hour)
	case d >= time.Hour:
		return fmt.Sprintf("%dh%dm", d/time.Hour, d%time.Hour/time.Minute)
	case d >= time.Minute:
		return fmt.Sprintf("%dm%ds", d/time.Minute, d%time.Minute/time.Second)
	default:
		return fmt.Sprintf("%ds", d/time.Second)
	}
}

// names returns, sorted, every key that some node has a cap or a count for.
func names(nodes []governor.Node, of func(governor.Node) (caps, counts map[string]int64)) []string {
	seen := make(map[string]bool)
	for _, n := range nodes {
		caps, counts := of(n)
		for name := range caps {
			seen[name] = true
		}
		for name := range counts {
			seen[name] = true
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

func quotasOf(n governor.Node) (caps, counts map[string]int64) { return n.Quotas, n.Used }
func limitsOf(n governor.Node) (caps, counts map[string]int64) { return n.Limits, n.Held }

// summary writes every cap and count of one node on a line, such as "tool_calls 37/500".
func summary(n governor.Node) string {
	var parts []string
	for _, name := range names([]governor.Node{n}, quotasOf) {
		limit, capped := n.Quotas[name]
		text, style := usage(n.Used[name], limit, capped)
		parts = append(parts, name+" "+style.Render(text))
	}
	for _, name := range names([]governor.Node{n}, limitsOf) {
		limit, capped := n.Limits[name]
		text, style := usage(n.Held[name], limit, capped)
		parts = append(parts, name+" "+style.Render(text)+" held")
	}
	return strings.Join(parts, dim.Render(" · "))
}

// defaultsLine writes what a node gives its new children on one line.
func defaultsLine(d *governor.Defaults) string {
	list := func(quotas map[string]int64) string {
		var parts []string
		for _, name := range slices.Sorted(maps.Keys(quotas)) {
			parts = append(parts, fmt.Sprintf("%s %d", name, quotas[name]))
		}
		return strings.Join(parts, ", ")
	}
	if d == nil || (len(d.Quotas) == 0 && (d.Children == nil || len(d.Children.Quotas) == 0)) {
		return "nothing set (press d)"
	}
	line := list(d.Quotas)
	if d.Children != nil && len(d.Children.Quotas) > 0 {
		if line != "" {
			line += dim.Render(" · ")
		}
		line += "their children: " + list(d.Children.Quotas)
	}
	return line
}

// View draws the screen.
func (m model) View() string {
	var b strings.Builder
	updated := "connecting…"
	if !m.loaded.IsZero() {
		updated = "updated " + m.loaded.Format(time.TimeOnly)
	}
	title := bold.Render("Governor") + "  " + m.addr
	gap := max(2, m.width-lipgloss.Width(title)-lipgloss.Width(updated))
	b.WriteString(title + strings.Repeat(" ", gap) + dim.Render(updated) + "\n")

	switch {
	case m.form != nil:
		b.WriteString(m.form.view())
	default:
		b.WriteString(m.listView())
	}
	for _, err := range []error{m.failed, m.err} {
		if err != nil {
			b.WriteString("\n" + bad.Render("error: "+err.Error()))
		}
	}
	// Lines wider than the terminal are cut rather than wrapped, so the layout holds.
	return lipgloss.NewStyle().MaxWidth(m.width).Render(b.String())
}

// listView draws the node on screen, the table of its children and the keys.
func (m model) listView() string {
	var b strings.Builder
	rule := dim.Render(strings.Repeat("─", m.width)) + "\n"
	node := m.current()
	if node == nil {
		return rule
	}
	crumbs := make([]string, len(m.path))
	for i, n := range m.path {
		crumbs[i] = label(n)
	}
	b.WriteString(bold.Render(strings.Join(crumbs, " › ")))
	if s := summary(*node); s != "" {
		b.WriteString("   " + s)
	}
	b.WriteString("\n" + rule)

	b.WriteString(m.table())
	b.WriteString(rule)
	b.WriteString("New children start with: " + defaultsLine(node.Defaults) + "\n")
	if m.confirm != nil {
		b.WriteString(warning.Render("Cancel "+label(*m.confirm)+" and everything under it?") + "  y yes · any other key no")
	} else {
		b.WriteString(dim.Render("↑↓ move · enter open · esc back · e edit caps · d edit defaults · c cancel · r refresh · q quit"))
	}
	return b.String()
}

// column is one column of the table: its heading and the cell of each row.
type column struct {
	head  string
	cells []string
	style []lipgloss.Style
	right bool // whether the column is aligned to the right
}

func (c *column) add(text string, style lipgloss.Style) {
	c.cells = append(c.cells, text)
	c.style = append(c.style, style)
}

// width is how wide the column must be to hold its heading and every cell.
func (c *column) width() int {
	w := lipgloss.Width(c.head)
	for _, cell := range c.cells {
		w = max(w, lipgloss.Width(cell))
	}
	return w
}

// cell pads the text of one row, or of the heading if row is negative, to the column's width.
func (c *column) cell(row int) string {
	text, style := c.head, bold
	if row >= 0 {
		text, style = c.cells[row], c.style[row]
	}
	pad := strings.Repeat(" ", c.width()-lipgloss.Width(text))
	if c.right {
		return pad + style.Render(text)
	}
	return style.Render(text) + pad
}

// columns builds the table of the children: one column per quota and per class any of them has.
func (m model) columns() []*column {
	plain := lipgloss.NewStyle()
	name, state := &column{head: "NAME"}, &column{head: "STATE"}
	ends, kids := &column{head: "ENDS IN", right: true}, &column{head: "CHILDREN", right: true}
	for _, n := range m.children {
		name.add(cut(label(n), maxName), plain)
		state.add(stateText(n.State), stateStyle(n.State))
		left := "-"
		if n.State == governor.StateActive && !n.Deadline.IsZero() {
			left = remaining(n.Deadline.Sub(m.now()))
		}
		ends.add(left, plain)
		kids.add(fmt.Sprint(n.Children), plain)
	}
	cols := []*column{name, state, ends, kids}
	for _, resource := range names(m.children, quotasOf) {
		c := &column{head: resource, right: true}
		for _, n := range m.children {
			limit, capped := n.Quotas[resource]
			c.add(usage(n.Used[resource], limit, capped))
		}
		cols = append(cols, c)
	}
	for _, class := range names(m.children, limitsOf) {
		c := &column{head: class + " held", right: true}
		for _, n := range m.children {
			limit, capped := n.Limits[class]
			c.add(usage(n.Held[class], limit, capped))
		}
		cols = append(cols, c)
	}
	return cols
}

// table draws the children that fit on screen, keeping the cursor in view.
func (m model) table() string {
	if len(m.children) == 0 {
		return dim.Render("  nothing here yet") + "\n"
	}
	cols := m.columns()
	line := func(row int) string {
		cells := make([]string, len(cols))
		for i, c := range cols {
			cells[i] = c.cell(row)
		}
		return strings.Join(cells, "  ")
	}
	var b strings.Builder
	b.WriteString("  " + line(-1) + "\n")
	visible := max(1, m.height-chrome)
	first := max(0, min(m.cursor-visible/2, len(m.children)-visible))
	for row := first; row < min(first+visible, len(m.children)); row++ {
		if row == m.cursor {
			b.WriteString(cursor.Render("▸") + " " + line(row) + "\n")
		} else {
			b.WriteString("  " + line(row) + "\n")
		}
	}
	if hidden := len(m.children) - visible; hidden > 0 {
		b.WriteString(dim.Render(fmt.Sprintf("  %d of %d shown", visible, len(m.children))) + "\n")
	}
	return b.String()
}

// view draws the form.
func (f *form) view() string {
	var b strings.Builder
	b.WriteString(bold.Render(f.title) + "\n")
	width := 0
	for _, fd := range f.fields {
		width = max(width, len(fd.name))
	}
	section := -1
	for i, r := range f.rows() {
		if r.section != section {
			section = r.section
			b.WriteString("\n" + f.sections[section] + "\n")
		}
		mark := "  "
		if i == f.cursor {
			mark = cursor.Render("▸") + " "
		}
		switch {
		case r.field >= 0:
			fd := f.fields[r.field]
			value := fd.value
			if value == "" {
				value = dim.Render("no cap")
			}
			if fd.value != fd.was {
				value = warning.Render(value) + dim.Render("  changed")
			}
			note := ""
			if fd.note != "" {
				note = dim.Render("  (" + fd.note + ")")
			}
			b.WriteString(fmt.Sprintf("%s%-*s  %s%s\n", mark, width, fd.name, value, note))
		case f.naming && i == f.cursor:
			b.WriteString(mark + "name: " + f.name + cursor.Render(" ") + "\n")
		default:
			b.WriteString(mark + dim.Render("+ add") + "\n")
		}
	}
	b.WriteString("\n")
	switch {
	case f.naming:
		b.WriteString(dim.Render("type the name · enter add · esc back"))
	default:
		b.WriteString(dim.Render("↑↓ move · type digits · backspace erase (empty = no cap) · enter save · esc discard"))
	}
	return b.String()
}
