package tui

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/Avik-creator/governor"
)

// maxDigits keeps a typed cap well inside an int64.
const maxDigits = 15

// nameChars are the characters a resource or class name may be typed with.
const nameChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.:"

// The two sections of a caps form, which are also those of a defaults form.
const (
	sectionQuotas = iota // in a defaults form: the quotas of each new child
	sectionLimits        // in a defaults form: the quotas of each child of those
)

// field is one cap in a form; an empty value means no cap.
type field struct {
	section int
	name    string
	value   string // digits as typed
	was     string // the value when the form opened
	note    string // shown beside the value, such as what is used
}

// limit returns the cap a field holds.
func (fd field) limit() int64 {
	if fd.value == "" {
		return governor.Unlimited
	}
	// The field only ever holds a short run of digits, so this cannot fail.
	n, _ := strconv.ParseInt(fd.value, 10, 64)
	return n
}

// form edits either the caps of a node or the defaults it gives its children.
type form struct {
	title    string
	node     uint64
	defaults bool      // whether the form edits defaults rather than caps
	sections [2]string // the heading of each section
	fields   []field   // ordered by section
	cursor   int       // index into rows
	naming   bool      // whether the name of a new field is being typed
	name     string    // the name typed so far
}

// row is one line the cursor can rest on: a field, or the "add" line that ends each section.
type row struct {
	section int
	field   int // index into fields, or -1 for the "add" line
}

func (f *form) clone() *form {
	c := *f
	c.fields = slices.Clone(f.fields)
	return &c
}

// rows lists the lines of the form in the order they are shown.
func (f *form) rows() []row {
	var rows []row
	for s := range f.sections {
		for i, fd := range f.fields {
			if fd.section == s {
				rows = append(rows, row{section: s, field: i})
			}
		}
		rows = append(rows, row{section: s, field: -1})
	}
	return rows
}

// fieldsFor makes one field per name in caps or counts, sorted; counted names what counts holds.
func fieldsFor(section int, caps, counts map[string]int64, counted string) []field {
	names := slices.Collect(maps.Keys(caps))
	for name := range counts {
		if _, ok := caps[name]; !ok {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	fields := make([]field, 0, len(names))
	for _, name := range names {
		fd := field{section: section, name: name}
		if limit, ok := caps[name]; ok {
			fd.value = strconv.FormatInt(limit, 10)
		}
		if counted != "" {
			fd.note = fmt.Sprintf("%d %s", counts[name], counted)
		}
		fd.was = fd.value
		fields = append(fields, fd)
	}
	return fields
}

// capsForm opens a form on the quotas and limits of a node.
func capsForm(n governor.Node) *form {
	return &form{
		title:    "Caps of " + label(n),
		node:     n.ID,
		sections: [2]string{"Quotas: how much may be used in total", "Limits: how many may be held at once"},
		fields: append(
			fieldsFor(sectionQuotas, n.Quotas, n.Used, "used"),
			fieldsFor(sectionLimits, n.Limits, n.Held, "held")...),
	}
}

// defaultsForm opens a form on what a node gives its new children.
func defaultsForm(n governor.Node) *form {
	f := &form{
		title:    "What " + label(n) + " gives each new child",
		node:     n.ID,
		defaults: true,
		sections: [2]string{"Quotas each new child starts with", "Quotas each child of those starts with"},
	}
	if d := n.Defaults; d != nil {
		f.fields = fieldsFor(sectionQuotas, d.Quotas, nil, "")
		if d.Children != nil {
			f.fields = append(f.fields, fieldsFor(sectionLimits, d.Children.Quotas, nil, "")...)
		}
	}
	return f
}

// key applies a key to the form and says whether to save it or to close it unsaved.
func (f *form) key(msg tea.KeyMsg) (save, closed bool) {
	if f.naming {
		f.nameKey(msg)
		return false, false
	}
	rows := f.rows()
	at := rows[f.cursor]
	switch key := msg.String(); key {
	case "esc":
		return false, true
	case "up", "shift+tab":
		f.cursor = max(0, f.cursor-1)
	case "down", "tab":
		f.cursor = min(f.cursor+1, len(rows)-1)
	case "ctrl+s":
		return true, false
	case "enter":
		if at.field >= 0 {
			return true, false
		}
		f.naming, f.name = true, ""
	case "backspace":
		if at.field >= 0 {
			v := &f.fields[at.field].value
			*v = (*v)[:max(0, len(*v)-1)]
		}
	default:
		// A digit is typed into the field under the cursor.
		if at.field >= 0 && len(key) == 1 && key[0] >= '0' && key[0] <= '9' {
			if v := &f.fields[at.field].value; len(*v) < maxDigits {
				*v += key
			}
		}
	}
	return false, false
}

// nameKey applies a key while the name of a new field is being typed.
func (f *form) nameKey(msg tea.KeyMsg) {
	section := f.rows()[f.cursor].section
	switch key := msg.String(); key {
	case "esc":
		f.naming = false
	case "backspace":
		f.name = f.name[:max(0, len(f.name)-1)]
	case "enter":
		taken := slices.ContainsFunc(f.fields, func(fd field) bool { return fd.section == section && fd.name == f.name })
		if f.name == "" || taken {
			return
		}
		// The new field goes at the end of its section, which is the line the cursor is on.
		at := 0
		for at < len(f.fields) && f.fields[at].section <= section {
			at++
		}
		f.fields = slices.Insert(f.fields, at, field{section: section, name: f.name})
		f.naming = false
	default:
		if len(key) == 1 && strings.Contains(nameChars, key) && len(f.name) < 64 {
			f.name += key
		}
	}
}

// save sends what the form changed to governord.
func (f *form) save(ctx context.Context, client Client) error {
	if f.defaults {
		return client.SetDefaults(ctx, f.node, f.asDefaults())
	}
	for _, fd := range f.fields {
		if fd.value == fd.was {
			continue
		}
		set := client.SetQuota
		if fd.section == sectionLimits {
			set = client.SetLimit
		}
		if err := set(ctx, f.node, fd.name, fd.limit()); err != nil {
			return fmt.Errorf("%s: %w", fd.name, err)
		}
	}
	return nil
}

// asDefaults builds the defaults the form describes; nil if every field is empty.
func (f *form) asDefaults() *governor.Defaults {
	quotas := [2]map[string]int64{{}, {}}
	for _, fd := range f.fields {
		if fd.value != "" {
			quotas[fd.section][fd.name] = fd.limit()
		}
	}
	var d *governor.Defaults
	if len(quotas[sectionLimits]) > 0 {
		d = &governor.Defaults{Children: &governor.Defaults{Quotas: quotas[sectionLimits]}}
	}
	if len(quotas[sectionQuotas]) > 0 {
		if d == nil {
			d = &governor.Defaults{}
		}
		d.Quotas = quotas[sectionQuotas]
	}
	return d
}
