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

// sortMode picks the column the listing is ordered by.
type sortMode uint8

const (
	sortBySize    sortMode = iota // default — descending
	sortByName                    // ascending by name
	sortByObjects                 // descending by recursive object count
)

// tuiModel drives the radix-tree browser. It holds no derived state across
// renders that ListDirectory cannot cheaply recompute, so navigation is just
// a stack of prefix strings.
type tuiModel struct {
	tree       *radix.Tree
	region     string
	width      int
	height     int
	prefix     string        // current directory prefix ("" for root)
	stack      []navFrame    // ancestry from root to prefix exclusive
	entries    []radix.Entry // listing of prefix; recomputed on navigation
	cursor     int           // selected entry index
	maxBytes   int64         // largest single-entry byte count in this listing — drives the percent bar
	err        error         // last navigation error, if any
	sortMode   sortMode      // current sort column
	sortAsc    bool          // false = descending (default for size/objects), true = ascending
	dirsFirst  bool          // when true, all directories sort ahead of files regardless of column
	showHelp   bool          // when true, View renders the help modal instead of the listing
}

// navFrame remembers the cursor position at each ancestor so going back
// restores the user's place.
type navFrame struct {
	prefix string
	cursor int
}

func initialModel(tree *radix.Tree, region string) *tuiModel {
	m := &tuiModel{
		tree:      tree,
		region:    region,
		sortMode:  sortBySize,
		sortAsc:   false, // size descending
		dirsFirst: true,
	}
	m.reload()
	return m
}

// reload fetches the current prefix's listing into m.entries and re-applies
// the current sort + dirs-first preferences.
func (m *tuiModel) reload() {
	entries, err := m.tree.ListDirectory(m.prefix)
	if err != nil {
		m.err = err
		m.entries = nil
		m.maxBytes = 0
		return
	}
	m.err = nil
	m.entries = entries
	m.sortEntries()
	m.recomputeMaxBytes()
	if m.cursor >= len(entries) {
		m.cursor = max(0, len(entries)-1)
	}
}

// recomputeMaxBytes updates the cached maximum byte count over the current
// listing — used to scale the percentage bar in renderRow.
func (m *tuiModel) recomputeMaxBytes() {
	var max int64
	for _, e := range m.entries {
		b := entryBytes(e)
		if b > max {
			max = b
		}
	}
	m.maxBytes = max
}

// sortEntries orders m.entries by the active sortMode (and direction),
// optionally pinning directories above files. Stable — ties keep their
// arrival order (which from radix is ascending lex).
func (m *tuiModel) sortEntries() {
	asc := m.sortAsc
	sortFn := func(a, b radix.Entry) int {
		if m.dirsFirst && a.IsDir != b.IsDir {
			if a.IsDir {
				return -1
			}
			return 1
		}
		switch m.sortMode {
		case sortByName:
			if a.Name < b.Name {
				if asc {
					return -1
				}
				return 1
			}
			if a.Name > b.Name {
				if asc {
					return 1
				}
				return -1
			}
			return 0
		case sortByObjects:
			ao, bo := entryObjects(a), entryObjects(b)
			if ao == bo {
				return 0
			}
			if ao < bo {
				if asc {
					return -1
				}
				return 1
			}
			if asc {
				return 1
			}
			return -1
		default: // sortBySize
			ab, bb := entryBytes(a), entryBytes(b)
			if ab == bb {
				return 0
			}
			if ab < bb {
				if asc {
					return -1
				}
				return 1
			}
			if asc {
				return 1
			}
			return -1
		}
	}
	slices.SortStableFunc(m.entries, sortFn)
}

func entryBytes(e radix.Entry) int64 {
	if e.IsDir {
		return e.Aggregate.Bytes.Total()
	}
	return e.Size
}

func entryObjects(e radix.Entry) int64 {
	if e.IsDir {
		return e.Aggregate.Objects
	}
	return 1
}

func (m *tuiModel) Init() tea.Cmd { return nil }

func (m *tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tea.KeyMsg:
		key := msg.String()
		// The help modal swallows every key (any keystroke dismisses it,
		// except q/ctrl+c which always quits the program). This matches
		// the standard ncdu / less behaviour.
		if m.showHelp {
			switch key {
			case "q", "ctrl+c":
				return m, tea.Quit
			default:
				m.showHelp = false
			}
			return m, nil
		}
		switch key {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "?":
			m.showHelp = true
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
			m.cursor = max(0, len(m.entries)-1)
		case "pgup":
			m.cursor = max(0, m.cursor-m.pageStep())
		case "pgdown":
			m.cursor = m.cursor + m.pageStep()
			if m.cursor >= len(m.entries) {
				m.cursor = max(0, len(m.entries)-1)
			}
		case "enter", "right", "l":
			m.descend()
		case "backspace", "left", "h":
			m.ascend()
		case "s":
			m.cycleSort(sortBySize, false)
		case "n":
			m.cycleSort(sortByName, true)
		case "C":
			m.cycleSort(sortByObjects, false)
		case "t":
			m.dirsFirst = !m.dirsFirst
			m.sortEntries()
		}
	}
	return m, nil
}

// cycleSort sets the active sort column. If the column is already active,
// flip the direction. Otherwise switch to the column with its preferred
// default direction (descending for size/objects, ascending for name).
func (m *tuiModel) cycleSort(mode sortMode, preferAsc bool) {
	if m.sortMode == mode {
		m.sortAsc = !m.sortAsc
	} else {
		m.sortMode = mode
		m.sortAsc = preferAsc
	}
	m.sortEntries()
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
	barFillStyle   = lipgloss.NewStyle().Bold(true)
	barEmptyStyle  = lipgloss.NewStyle().Faint(true)
	helpBoxStyle   = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(1, 2)
	footerHelpHint = "↑/↓ move · Enter descend · Backspace up · s size · n name · C count · t dirs · ? help · q quit"
)

// percentBarWidth is the fixed character width of the per-row size bar.
// Picked to match ncdu's default — wide enough to convey proportion at a
// glance, narrow enough not to crowd long key names.
const percentBarWidth = 12

func (m *tuiModel) View() string {
	if m.showHelp {
		return m.helpView()
	}
	var b strings.Builder
	prefix := m.prefix
	if prefix == "" {
		prefix = "/"
	}
	b.WriteString(headerStyle.Render(fmt.Sprintf("s3du · %s · %s", m.region, prefix)))
	b.WriteString("  ")
	b.WriteString(dimStyle.Render(m.sortIndicator()))
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

	// Layout: bytes + bar + name + class + objects + $/mo. Name takes the
	// remaining width. The bar widget is fixed at percentBarWidth.
	const fixedCols = 10 + 2 + percentBarWidth + 2 + 19 + 2 + 10 + 2 + 10 + 2 // bytes, bar, class, objects, $/mo
	nameWidth := max(m.width-fixedCols, 20)

	header := fmt.Sprintf("%10s  %-*s  %-*s  %-19s  %10s  %10s",
		"bytes", percentBarWidth, "% size", nameWidth, "name", "class", "objects", "$/mo")
	b.WriteString(dimStyle.Render(header))
	b.WriteString("\n")

	startRow, endRow := visibleRange(m.cursor, m.height-6, len(m.entries))
	for i := startRow; i < endRow; i++ {
		e := m.entries[i]
		line := m.renderRow(e, nameWidth)
		switch {
		case i == m.cursor:
			line = selectedStyle.Render(line)
		case e.IsDir:
			line = dirStyle.Render(line)
		}
		b.WriteString(line)
		b.WriteString("\n")
	}

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

// sortIndicator returns a short tag like "[size↓ dirs first]" that the
// header strip renders so the user can tell what they're looking at.
func (m *tuiModel) sortIndicator() string {
	col := "size"
	switch m.sortMode {
	case sortByName:
		col = "name"
	case sortByObjects:
		col = "count"
	}
	arrow := "↓"
	if m.sortAsc {
		arrow = "↑"
	}
	flags := ""
	if m.dirsFirst {
		flags = " · dirs first"
	}
	return fmt.Sprintf("[%s%s%s]", col, arrow, flags)
}

// renderRow renders one entry with size, percent bar, name, class, objects,
// and cost. The bar is sized to entryBytes(e) / m.maxBytes (which is the
// largest single-entry size in the current listing, NOT the directory
// total — matches how ncdu visualises a single directory).
func (m *tuiModel) renderRow(e radix.Entry, nameWidth int) string {
	name := e.Name
	if !e.IsDir && name == "" {
		name = "."
	}
	if len(name) > nameWidth {
		name = name[:nameWidth-1] + "…"
	}

	bytes := entryBytes(e)
	bar := renderBar(bytes, m.maxBytes, percentBarWidth)

	if e.IsDir {
		cost := dirCost(e.Aggregate.Bytes, m.region)
		return fmt.Sprintf("%10s  %s  %-*s  %-19s  %10d  %10s",
			humanBytes(bytes),
			bar,
			nameWidth, name,
			dominantClassLabel(e.Aggregate.Bytes),
			e.Aggregate.Objects,
			humanDollars(cost),
		)
	}
	cost := monthlyStorageCost(e.Size, e.Class.String(), m.region)
	return fmt.Sprintf("%10s  %s  %-*s  %-19s  %10s  %10s",
		humanBytes(bytes),
		bar,
		nameWidth, name,
		e.Class.String(),
		"",
		humanDollars(cost),
	)
}

// renderBar draws a `width`-character solid-block bar proportional to
// value/max. Empty cells stay faint dots to keep the column visible even
// for sub-1% entries.
func renderBar(value, max int64, width int) string {
	if max <= 0 || value <= 0 {
		return barEmptyStyle.Render(strings.Repeat("·", width))
	}
	filled := min(int(value*int64(width)/max), width)
	// Sub-1% entries that round to zero still get one block so the user
	// sees that there is something to count.
	if filled == 0 && value > 0 {
		filled = 1
	}
	return barFillStyle.Render(strings.Repeat("█", filled)) +
		barEmptyStyle.Render(strings.Repeat("·", width-filled))
}

// helpView renders the modal help screen as a centered rounded-border box.
// Listed bindings mirror the ncdu cheat sheet adapted to s3du's vocabulary.
func (m *tuiModel) helpView() string {
	body := strings.Join([]string{
		headerStyle.Render("s3du — keybindings"),
		"",
		"Navigation",
		"  ↑/k        previous entry",
		"  ↓/j        next entry",
		"  g / Home   first entry",
		"  G / End    last entry",
		"  PgUp/PgDn  page up / page down",
		"  Enter / l  descend into selected dir",
		"  Bksp / h   ascend to parent dir",
		"",
		"Sort",
		"  s          by size (toggle direction)",
		"  n          by name (toggle direction)",
		"  C          by object count (toggle direction)",
		"  t          toggle directories-before-files",
		"",
		"Misc",
		"  ?          show / hide this help",
		"  q / Ctrl-C quit",
	}, "\n")
	return helpBoxStyle.Render(body)
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
			bytes += e.Aggregate.Bytes.Total()
			cost += dirCost(e.Aggregate.Bytes, region)
			continue
		}
		objs++
		bytes += e.Size
		cost += monthlyStorageCost(e.Size, e.Class.String(), region)
	}
	return objs, bytes, cost
}
