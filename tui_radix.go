package main

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/ochaton/s3du/internal/progress"
	"github.com/ochaton/s3du/radix"
)

// runRadixTUI launches the raw-radix browser. Unlike runTUI (which navigates
// by '/'-delimited prefixes), this mode walks the actual radix tree structure
// node by node so the operator can see edges, child counts, dir-markers, and
// aggregates at each step.
func runRadixTUI(tree *radix.Tree, region string) error {
	_, err := tea.NewProgram(initialRadixModel(tree, region), tea.WithAltScreen()).Run()
	return err
}

type radixFrame struct {
	view   radix.NodeView
	cursor int
}

type radixModel struct {
	tree     *radix.Tree
	region   string
	width    int
	height   int
	current  radix.NodeView   // the internal we're inside
	children []radix.NodeView // its children, in stored order
	cursor   int
	stack    []radixFrame
}

func initialRadixModel(tree *radix.Tree, region string) *radixModel {
	root := tree.RootView()
	return &radixModel{
		tree:     tree,
		region:   region,
		current:  root,
		children: tree.ChildrenOf(root.CID),
	}
}

func (m *radixModel) Init() tea.Cmd { return nil }

func (m *radixModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
			if m.cursor < len(m.children)-1 {
				m.cursor++
			}
		case "home", "g":
			m.cursor = 0
		case "end", "G":
			m.cursor = max(0, len(m.children)-1)
		case "pgup":
			m.cursor = max(0, m.cursor-m.pageStep())
		case "pgdown":
			m.cursor = m.cursor + m.pageStep()
			if m.cursor >= len(m.children) {
				m.cursor = max(0, len(m.children)-1)
			}
		case "enter", "right", "l":
			m.descend()
		case "backspace", "left", "h":
			m.ascend()
		}
	}
	return m, nil
}

func (m *radixModel) pageStep() int {
	if m.height < 6 {
		return 1
	}
	return m.height - 6
}

func (m *radixModel) descend() {
	if m.cursor >= len(m.children) {
		return
	}
	sel := m.children[m.cursor]
	if sel.Kind != radix.NodeInternal {
		return
	}
	m.stack = append(m.stack, radixFrame{view: m.current, cursor: m.cursor})
	m.current = sel
	m.children = m.tree.ChildrenOf(sel.CID)
	m.cursor = 0
}

func (m *radixModel) ascend() {
	if len(m.stack) == 0 {
		return
	}
	last := m.stack[len(m.stack)-1]
	m.stack = m.stack[:len(m.stack)-1]
	m.current = last.view
	m.children = m.tree.ChildrenOf(last.view.CID)
	m.cursor = last.cursor
}

var (
	rxHeaderStyle = lipgloss.NewStyle().Bold(true)
	rxSelStyle    = lipgloss.NewStyle().Reverse(true)
	rxIntStyle    = lipgloss.NewStyle().Bold(true)
	rxLeafStyle   = lipgloss.NewStyle()
	rxDimStyle    = lipgloss.NewStyle().Faint(true)
	rxHelpHint    = "↑/↓ move · Enter/l descend · Backspace/h up · q quit"
)

func (m *radixModel) View() string {
	var b strings.Builder

	// Header: breadcrumb path from root to current node.
	path := m.breadcrumb()
	b.WriteString(rxHeaderStyle.Render(fmt.Sprintf("s3du radix · %s", path)))
	b.WriteString("\n\n")

	// Current-node block.
	b.WriteString(renderNodeBlock(m.current))
	b.WriteString("\n")

	if len(m.children) == 0 {
		b.WriteString(rxDimStyle.Render("(no children — leaf node or empty internal)"))
		b.WriteString("\n\n")
		b.WriteString(rxDimStyle.Render(rxHelpHint))
		return b.String()
	}

	// Children table.
	nameWidth := max(m.width-72, 16)
	header := fmt.Sprintf("%5s  %10s  %4s  %-*s  %-19s  %10s  %10s",
		"idx", "cid", "kind", nameWidth, "edge", "class/agg", "objects", "size")
	b.WriteString(rxDimStyle.Render(header))
	b.WriteString("\n")

	startRow, endRow := visibleRange(m.cursor, m.height-10, len(m.children))
	for i := startRow; i < endRow; i++ {
		row := renderRadixRow(i, m.children[i], nameWidth)
		switch {
		case i == m.cursor:
			row = rxSelStyle.Render(row)
		case m.children[i].Kind == radix.NodeInternal:
			row = rxIntStyle.Render(row)
		default:
			row = rxLeafStyle.Render(row)
		}
		b.WriteString(row)
		b.WriteString("\n")
	}

	b.WriteString("\n")
	b.WriteString(rxDimStyle.Render(rxHelpHint))
	return b.String()
}

func (m *radixModel) breadcrumb() string {
	parts := make([]string, 0, len(m.stack)+1)
	for _, f := range m.stack {
		parts = append(parts, edgeLabel(f.view.Edge))
	}
	parts = append(parts, edgeLabel(m.current.Edge))
	return strings.Join(parts, " ▸ ")
}

func edgeLabel(e string) string {
	if e == "" {
		return "<root>"
	}
	return truncate(fmt.Sprintf("%q", e), 24)
}

// renderNodeBlock prints a few lines summarising the current node (the one
// the user is "inside"). Reads CID, kind, edge length, agg, dir-marker.
func renderNodeBlock(v radix.NodeView) string {
	var b strings.Builder
	tagged := ""
	if isLeafCID(v.CID) {
		tagged = " (tagged)"
	}
	b.WriteString(rxDimStyle.Render(fmt.Sprintf(
		"current: cid=0x%08x%s · kind=%s · edge=%q (len %d)",
		v.CID, tagged, v.Kind.String(), v.Edge, len(v.Edge),
	)))
	b.WriteString("\n")
	if v.Kind == radix.NodeInternal {
		b.WriteString(rxDimStyle.Render(fmt.Sprintf(
			"        children=%d · agg.objects=%d · agg.bytes=%s · classes=%d",
			v.NumChildren,
			v.Aggregate.Objects,
			progress.HumanBytes(v.Aggregate.Bytes.Total()),
			len(v.Aggregate.Bytes),
		)))
		b.WriteString("\n")
		if v.HasDirMarker {
			b.WriteString(rxDimStyle.Render(fmt.Sprintf(
				"        dir-marker: class=%s size=%d",
				v.DirMarker.Class.String(), v.DirMarker.Size,
			)))
			b.WriteString("\n")
		}
	} else {
		b.WriteString(rxDimStyle.Render(fmt.Sprintf(
			"        class=%s · size=%d (%s)",
			v.Class.String(), v.Size, progress.HumanBytes(v.Size),
		)))
		b.WriteString("\n")
	}
	return b.String()
}

func renderRadixRow(idx int, v radix.NodeView, nameWidth int) string {
	// Quote BEFORE truncating: the %q escaping can change a string's
	// rendered width, so applying truncate to the quoted form keeps the
	// column aligned. truncate is rune-aware (not byte-aware) so multi-
	// byte UTF-8 edges never split a code point.
	edge := truncate(fmt.Sprintf("%q", v.Edge), nameWidth)
	if v.Kind == radix.NodeLeaf {
		return fmt.Sprintf("%5d  0x%08x  %-4s  %-*s  %-19s  %10s  %10s",
			idx, v.CID, "leaf",
			nameWidth, edge,
			v.Class.String(),
			"",
			progress.HumanBytes(v.Size),
		)
	}
	label := dominantClassLabel(v.Aggregate.Bytes)
	if v.HasDirMarker {
		label = "*" + label
	}
	return fmt.Sprintf("%5d  0x%08x  %-4s  %-*s  %-19s  %10d  %10s",
		idx, v.CID, "int",
		nameWidth, edge,
		label,
		v.Aggregate.Objects,
		progress.HumanBytes(v.Aggregate.Bytes.Total()),
	)
}

// isLeafCID mirrors the package-internal leaf-tag check; the top bit of the
// CID indicates a leaf-arena reference.
func isLeafCID(cid uint32) bool { return cid&(1<<31) != 0 }
