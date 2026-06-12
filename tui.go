package main

import (
	"fmt"
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
	prefix  string             // current directory prefix ("" for root)
	stack   []navFrame         // ancestry from root to prefix exclusive
	entries []radix.Entry      // listing of prefix; recomputed on navigation
	cursor  int                // selected entry index
	err     error              // last navigation error, if any
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
		m.cursor = max0(len(entries) - 1)
	}
}

func max0(n int) int {
	if n < 0 {
		return 0
	}
	return n
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
			m.cursor = max0(len(m.entries) - 1)
		case "pgup":
			m.cursor = max0(m.cursor - m.pageStep())
		case "pgdown":
			m.cursor = m.cursor + m.pageStep()
			if m.cursor >= len(m.entries) {
				m.cursor = max0(len(m.entries) - 1)
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

// Styling.
var (
	headerStyle    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("12"))
	selectedStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("0")).Background(lipgloss.Color("12"))
	dirStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("12"))
	dimStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	errStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
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

	// Layout: pad name column to width; rest is fixed-width metrics.
	nameWidth := m.width - 50
	if nameWidth < 20 {
		nameWidth = 20
	}

	// Body rows.
	startRow, endRow := visibleRange(m.cursor, m.height-5, len(m.entries))
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

	// Status line: total + cost for the current directory.
	totalObj, totalBytes, totalCost := dirTotals(m.entries, m.region)
	b.WriteString("\n")
	b.WriteString(dimStyle.Render(fmt.Sprintf("total: %d objects · %s · %s/mo",
		totalObj, humanBytes(totalBytes), humanDollars(totalCost))))
	b.WriteString("\n")
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
	start := cursor - half
	if start < 0 {
		start = 0
	}
	end := start + rows
	if end > total {
		end = total
		start = end - rows
	}
	return start, end
}

func renderRow(e radix.Entry, region string, nameWidth int) string {
	name := e.Name
	if e.IsDir {
		// indicate dir with trailing slash (already in radix.Entry for dirs).
	} else {
		// nothing to add; file rows render their class/size.
	}
	if len(name) > nameWidth {
		name = name[:nameWidth-1] + "…"
	}
	if e.IsDir {
		bytes := byteSum(e.Aggregate.Bytes)
		cost := dirCost(e.Aggregate.Bytes, region)
		return fmt.Sprintf("%-*s  %10d  %10s  %10s",
			nameWidth, name,
			e.Aggregate.Objects,
			humanBytes(bytes),
			humanDollars(cost),
		)
	}
	cost := monthlyStorageCost(e.Size, e.Class.String(), region)
	return fmt.Sprintf("%-*s  %10s  %10s  %10s",
		nameWidth, name,
		e.Class.String(),
		humanBytes(e.Size),
		humanDollars(cost),
	)
}

// dirCost sums the monthly storage cost for an aggregate's ClassBytes.
func dirCost(b radix.ClassBytes, region string) float64 {
	var total float64
	for _, kv := range b {
		total += monthlyStorageCost(kv.Size, kv.Class.String(), region)
	}
	return total
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
