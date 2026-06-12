package main

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/ochaton/s3du/radix"
)

// runTUI launches the bubbletea browser over an already-built radix.Tree.
func runTUI(tree *radix.Tree, region string) error {
	_, err := tea.NewProgram(initialModel(tree, region), tea.WithAltScreen()).Run()
	return err
}

// tuiModel drives the radix-tree browser. It holds no derived state across
// renders that ListDirectory cannot cheaply recompute, so navigation is just
// a stack of prefix strings.
type tuiModel struct {
	tree    *radix.Tree
	region  string
	width   int
	height  int
	prefix  string        // current directory prefix ("" for root)
	stack   []navFrame    // ancestry from root to prefix exclusive
	entries []radix.Entry // listing of prefix; recomputed on navigation
	cursor  int           // selected entry index
	err     error         // last navigation error, if any
}

// navFrame remembers the cursor position at each ancestor so going back
// restores the user's place.
type navFrame struct {
	prefix string
	cursor int
}

func initialModel(tree *radix.Tree, region string) *tuiModel {
	m := &tuiModel{tree: tree, region: region}
	m.reload()
	return m
}

// reload fetches the current prefix's listing into m.entries.
func (m *tuiModel) reload() {
	entries, err := m.tree.ListDirectory(m.prefix)
	if err != nil {
		m.err = err
		m.entries = nil
		return
	}
	m.err = nil
	m.entries = entries
	if m.cursor >= len(entries) {
		m.cursor = max(0, len(entries)-1)
	}
}

func (m *tuiModel) Init() tea.Cmd { return nil }

func (m *tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < len(m.entries)-1 {
				m.cursor++
			}
		case "home", "g":
			m.cursor = 0
		case "end", "G":
			m.cursor = max(0, len(m.entries) - 1)
		case "pgup":
			m.cursor = max(0, m.cursor - m.pageStep())
		case "pgdown":
			m.cursor = m.cursor + m.pageStep()
			if m.cursor >= len(m.entries) {
				m.cursor = max(0, len(m.entries) - 1)
			}
		case "enter", "right", "l":
			m.descend()
		case "backspace", "left", "h":
			m.ascend()
		}
	}
	return m, nil
}

func (m *tuiModel) pageStep() int {
	if m.height < 6 {
		return 1
	}
	return m.height - 4
}

func (m *tuiModel) descend() {
	if m.cursor >= len(m.entries) {
		return
	}
	e := m.entries[m.cursor]
	if !e.IsDir {
		return
	}
	m.stack = append(m.stack, navFrame{prefix: m.prefix, cursor: m.cursor})
	m.prefix = m.prefix + e.Name
	m.cursor = 0
	m.reload()
}

func (m *tuiModel) ascend() {
	if len(m.stack) == 0 {
		return
	}
	last := m.stack[len(m.stack)-1]
	m.stack = m.stack[:len(m.stack)-1]
	m.prefix = last.prefix
	m.cursor = last.cursor
	m.reload()
}

// Styling. The fixed-color choices were unreadable on some terminals (the
// "blue" ANSI slot renders as dark purple on common macOS schemes). Use
// terminal-native effects (bold, reverse, faint) which respect the user's
// scheme.
var (
	headerStyle    = lipgloss.NewStyle().Bold(true)
	selectedStyle  = lipgloss.NewStyle().Reverse(true)
	dirStyle       = lipgloss.NewStyle().Bold(true)
	dimStyle       = lipgloss.NewStyle().Faint(true)
	errStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("9")) // bright red
	footerHelpHint = "↑/↓ move · Enter/l descend · Backspace/h up · q quit"
)

func (m *tuiModel) View() string {
	var b strings.Builder
	prefix := m.prefix
	if prefix == "" {
		prefix = "/"
	}
	b.WriteString(headerStyle.Render(fmt.Sprintf("s3du · %s · %s", m.region, prefix)))
	b.WriteString("\n\n")

	if m.err != nil {
		b.WriteString(errStyle.Render("error: " + m.err.Error()))
		b.WriteString("\n")
		return b.String()
	}
	if len(m.entries) == 0 {
		b.WriteString(dimStyle.Render("(empty directory)"))
		b.WriteString("\n\n")
		b.WriteString(dimStyle.Render(footerHelpHint))
		return b.String()
	}

	// Layout: pad name column to width; rest is fixed-width metrics (the
	// extra class column adds ~21 chars to the non-name area).
	nameWidth := max(m.width-60, 20)

	// Column header row helps readers map values to fields.
	header := fmt.Sprintf("%-*s  %-19s  %10s  %10s  %10s",
		nameWidth, "name", "class", "objects", "bytes", "$/mo")
	b.WriteString(dimStyle.Render(header))
	b.WriteString("\n")

	// Body rows.
	startRow, endRow := visibleRange(m.cursor, m.height-6, len(m.entries))
	for i := startRow; i < endRow; i++ {
		e := m.entries[i]
		line := renderRow(e, m.region, nameWidth)
		if i == m.cursor {
			line = selectedStyle.Render(line)
		} else if e.IsDir {
			line = dirStyle.Render(line)
		}
		b.WriteString(line)
		b.WriteString("\n")
	}

	// Status: totals + per-class breakdown across all entries in this listing.
	totalObj, totalBytes, totalCost := dirTotals(m.entries, m.region)
	b.WriteString("\n")
	b.WriteString(dimStyle.Render(fmt.Sprintf("total: %d objects · %s · %s/mo",
		totalObj, humanBytes(totalBytes), humanDollars(totalCost))))
	b.WriteString("\n")
	if cb := classBreakdown(m.entries); cb != "" {
		b.WriteString(dimStyle.Render("classes: " + cb))
		b.WriteString("\n")
	}
	b.WriteString(dimStyle.Render(footerHelpHint))
	return b.String()
}

// visibleRange returns the inclusive-exclusive index range that fits in
// rows display rows while keeping cursor visible.
func visibleRange(cursor, rows, total int) (int, int) {
	if rows < 1 {
		rows = 1
	}
	if total <= rows {
		return 0, total
	}
	half := rows / 2
	start := max(cursor-half, 0)
	end := start + rows
	if end > total {
		end = total
		start = end - rows
	}
	return start, end
}

// renderRow formats one entry as four columns: name, class info, size, cost.
// Directories report the dominant class (and a "+N" marker if more than one
// storage class is present in the subtree). Files render their own class.
// Empty-name file entries are S3 directory markers (zero-byte objects whose
// key equals the current prefix); they get a "." placeholder so the table
// stays aligned.
func renderRow(e radix.Entry, region string, nameWidth int) string {
	name := e.Name
	if !e.IsDir && name == "" {
		name = "."
	}
	if len(name) > nameWidth {
		name = name[:nameWidth-1] + "…"
	}
	if e.IsDir {
		bytes := byteSum(e.Aggregate.Bytes)
		cost := dirCost(e.Aggregate.Bytes, region)
		return fmt.Sprintf("%-*s  %-19s  %10d  %10s  %10s",
			nameWidth, name,
			dominantClassLabel(e.Aggregate.Bytes),
			e.Aggregate.Objects,
			humanBytes(bytes),
			humanDollars(cost),
		)
	}
	cost := monthlyStorageCost(e.Size, e.Class.String(), region)
	return fmt.Sprintf("%-*s  %-19s  %10s  %10s  %10s",
		nameWidth, name,
		e.Class.String(),
		"",
		humanBytes(e.Size),
		humanDollars(cost),
	)
}

// dominantClassLabel returns a compact label for a directory's class mix:
// just the largest-by-bytes class when there is only one populated bucket,
// or "<class> +N" when there are multiple.
func dominantClassLabel(b radix.ClassBytes) string {
	if len(b) == 0 {
		return ""
	}
	top := b[0]
	for _, kv := range b[1:] {
		if kv.Size > top.Size {
			top = kv
		}
	}
	if len(b) == 1 {
		return top.Class.String()
	}
	return fmt.Sprintf("%s +%d", top.Class.String(), len(b)-1)
}

// dirCost sums the monthly storage cost for an aggregate's ClassBytes.
func dirCost(b radix.ClassBytes, region string) float64 {
	var total float64
	for _, kv := range b {
		total += monthlyStorageCost(kv.Size, kv.Class.String(), region)
	}
	return total
}

// classBreakdown sums objects and bytes per storage class across every entry
// in the listing (directories and files alike) and returns a single-line
// compact summary like "GLACIER_IR: 49998 objs / 106.8 GiB · STANDARD: 2".
func classBreakdown(entries []radix.Entry) string {
	totals := map[radix.StorageClass]struct {
		objs  int64
		bytes int64
	}{}
	for _, e := range entries {
		if e.IsDir {
			// Aggregate.Bytes lists the per-class byte totals across the
			// subtree; the per-class object counts are not tracked
			// separately so we attribute every dir's Objects to its
			// dominant class. Not perfect but readable.
			top := dominantClassFor(e.Aggregate.Bytes)
			t := totals[top]
			t.objs += e.Aggregate.Objects
			for _, kv := range e.Aggregate.Bytes {
				bucket := totals[kv.Class]
				bucket.bytes += kv.Size
				if kv.Class == top {
					bucket.objs = t.objs
				}
				totals[kv.Class] = bucket
			}
			continue
		}
		b := totals[e.Class]
		b.objs++
		b.bytes += e.Size
		totals[e.Class] = b
	}
	if len(totals) == 0 {
		return ""
	}
	// Order classes by enum value for stable rendering.
	classes := make([]radix.StorageClass, 0, len(totals))
	for c := range totals {
		classes = append(classes, c)
	}
	slices.SortFunc(classes, func(a, b radix.StorageClass) int { return cmp.Compare(a, b) })
	parts := make([]string, 0, len(classes))
	for _, c := range classes {
		t := totals[c]
		parts = append(parts, fmt.Sprintf("%s: %d / %s", c.String(), t.objs, humanBytes(t.bytes)))
	}
	return strings.Join(parts, " · ")
}

// dominantClassFor returns the class with the largest Size in b. Used as a
// crude attribution of a dir's Objects count among its classes for the
// status-line summary.
func dominantClassFor(b radix.ClassBytes) radix.StorageClass {
	if len(b) == 0 {
		return radix.ClassUnknown
	}
	top := b[0]
	for _, kv := range b[1:] {
		if kv.Size > top.Size {
			top = kv
		}
	}
	return top.Class
}

// dirTotals collapses a listing's entries into totals for the status line.
func dirTotals(entries []radix.Entry, region string) (int64, int64, float64) {
	var (
		objs  int64
		bytes int64
		cost  float64
	)
	for _, e := range entries {
		if e.IsDir {
			objs += e.Aggregate.Objects
			bytes += byteSum(e.Aggregate.Bytes)
			cost += dirCost(e.Aggregate.Bytes, region)
			continue
		}
		objs++
		bytes += e.Size
		cost += monthlyStorageCost(e.Size, e.Class.String(), region)
	}
	return objs, bytes, cost
}
