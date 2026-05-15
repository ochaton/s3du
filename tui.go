package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// styles
var (
	styleHeader   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("33"))
	styleSelected = lipgloss.NewStyle().Reverse(true)
	styleDim      = lipgloss.NewStyle().Faint(true)
	styleSep      = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	styleFooter   = lipgloss.NewStyle().Faint(true)
	styleCost     = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
	styleDir      = lipgloss.NewStyle().Foreground(lipgloss.Color("39"))
)

type DirEntry struct {
	name        string // display name (basename for files, last segment for dirs)
	fullPrefix  string // full prefix for dirs (used for navigation); full key for files (unused)
	isDir       bool
	count       int64
	size        int64
	monthlyCost float64
}

type tuiModel struct {
	bucket       string
	region       string
	treeIndex    map[string]DirSection
	listRequests int64

	objIndex map[string]ObjectSection
	objFile  *os.File

	currentPrefix string
	cursor        int
	entries       []DirEntry
	cursorHistory map[string]int

	showHelp bool
	width    int
	height   int
}

func newTUIModel(bucket, region string, listRequests int64, treeIndex map[string]DirSection, objIndex map[string]ObjectSection, objFile *os.File) tuiModel {
	m := tuiModel{
		bucket:        bucket,
		region:        region,
		treeIndex:     treeIndex,
		listRequests:  listRequests,
		objIndex:      objIndex,
		objFile:       objFile,
		currentPrefix: "",
		cursorHistory: make(map[string]int),
		width:         80,
		height:        24,
	}
	m.entries = m.computeEntries(m.currentPrefix)
	return m
}

// computeEntries builds the display list for currentPrefix.
// Directories come from treeIndex (in memory). Files loaded lazily from objects.bin.
func (m *tuiModel) computeEntries(currentPrefix string) []DirEntry {
	entries := make([]DirEntry, 0, 32)

	if currentPrefix != "" {
		entries = append(entries, DirEntry{name: "..", isDir: true})
	}

	for k, sec := range m.treeIndex {
		if !strings.HasPrefix(k, currentPrefix) {
			continue
		}
		rel := k[len(currentPrefix):]
		if rel == "" {
			continue
		}
		// Direct child: rel is "something/" with exactly one slash.
		if strings.Count(rel, "/") != 1 {
			continue
		}
		entries = append(entries, DirEntry{
			name:        lastSegment(k),
			fullPrefix:  k,
			isDir:       true,
			count:       sec.Count,
			size:        sec.Size,
			monthlyCost: sec.monthlyCost(m.region),
		})
	}

	// Load files for this prefix from disk (only those directly here).
	files := m.loadFiles(currentPrefix)
	for _, fe := range files {
		cost := monthlyStorageCost(fe.SizeBytes, fe.StorageClass, m.region)
		entries = append(entries, DirEntry{
			name:        fe.Name,
			isDir:       false,
			count:       1,
			size:        fe.SizeBytes,
			monthlyCost: cost,
		})
	}

	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].name == ".." {
			return true
		}
		if entries[j].name == ".." {
			return false
		}
		if entries[i].isDir != entries[j].isDir {
			return entries[i].isDir
		}
		return entries[i].size > entries[j].size
	})

	return entries
}

func (m *tuiModel) loadFiles(prefix string) []FileEntry {
	if m.objFile == nil || m.objIndex == nil {
		return nil
	}
	files, _ := ReadFilesForPrefix(m.objFile, m.objIndex, prefix)
	return files
}

func (m tuiModel) Init() tea.Cmd {
	return nil
}

func (m tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit

		case "?":
			m.showHelp = !m.showHelp
			return m, nil

		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}

		case "down", "j":
			if m.cursor < len(m.entries)-1 {
				m.cursor++
			}

		case "pgup", "ctrl+b":
			m.cursor -= m.pageSize()
			if m.cursor < 0 {
				m.cursor = 0
			}

		case "pgdn", "ctrl+f":
			m.cursor += m.pageSize()
			if m.cursor >= len(m.entries) {
				m.cursor = len(m.entries) - 1
			}

		case "enter", "right", "l":
			if m.cursor < len(m.entries) {
				sel := m.entries[m.cursor]
				if sel.name == ".." {
					m = m.goUp()
				} else if sel.isDir {
					m.cursorHistory[m.currentPrefix] = m.cursor
					m.currentPrefix = sel.fullPrefix
					m.entries = m.computeEntries(m.currentPrefix)
					m.cursor = m.cursorHistory[m.currentPrefix]
					if m.cursor >= len(m.entries) {
						m.cursor = 0
					}
				}
			}

		case "backspace", "left", "h":
			if m.currentPrefix != "" {
				m = m.goUp()
			}
		}
	}
	return m, nil
}

func (m tuiModel) pageSize() int {
	ps := m.height - 6
	if ps < 1 {
		ps = 1
	}
	return ps
}

func (m tuiModel) goUp() tuiModel {
	parent := parentPrefix(m.currentPrefix)
	m.cursorHistory[m.currentPrefix] = m.cursor
	prevCursor := m.cursorHistory[parent]
	m.currentPrefix = parent
	m.entries = m.computeEntries(m.currentPrefix)
	m.cursor = prevCursor
	if m.cursor >= len(m.entries) {
		m.cursor = 0
	}
	return m
}

func parentPrefix(prefix string) string {
	if prefix == "" {
		return ""
	}
	trimmed := strings.TrimSuffix(prefix, "/")
	idx := strings.LastIndex(trimmed, "/")
	if idx < 0 {
		return ""
	}
	return trimmed[:idx+1]
}

func (m tuiModel) View() string {
	if m.showHelp {
		return m.helpView()
	}

	var sb strings.Builder

	path := "s3://" + m.bucket + "/" + m.currentPrefix
	header := styleHeader.Render(fmt.Sprintf(" s3du  %-*s  %s", m.width-20, path, m.region))
	sb.WriteString(header + "\n")
	sb.WriteString(styleSep.Render(strings.Repeat("─", m.width)) + "\n")

	sizeW := 10
	objW := 9
	costW := 10
	nameW := m.width - sizeW - objW - costW - 6
	if nameW < 10 {
		nameW = 10
	}

	colHdr := fmt.Sprintf("  %*s  %*s  %*s  %-s",
		sizeW, "Size",
		objW, "Objects",
		costW, "$/month",
		"Name",
	)
	sb.WriteString(styleDim.Render(colHdr) + "\n")

	listHeight := m.height - 6
	if listHeight < 1 {
		listHeight = 1
	}

	start := 0
	if m.cursor >= listHeight {
		start = m.cursor - listHeight + 1
	}
	end := start + listHeight
	if end > len(m.entries) {
		end = len(m.entries)
	}

	for i := start; i < end; i++ {
		e := m.entries[i]
		var line string
		if e.name == ".." {
			line = fmt.Sprintf("  %*s  %*s  %*s  %s",
				sizeW, "", objW, "", costW, "",
				styleDir.Render(".."),
			)
		} else {
			var nameStr string
			if e.isDir {
				nameStr = styleDir.Render(e.name)
			} else {
				nameStr = e.name
			}
			if len(nameStr) > nameW {
				nameStr = nameStr[:nameW-1] + "…"
			}
			costStr := fmt.Sprintf("$%.4f", e.monthlyCost)
			line = fmt.Sprintf("  %*s  %*d  %*s  %s",
				sizeW, humanSize(e.size),
				objW, e.count,
				costW, costStr,
				nameStr,
			)
		}
		if i == m.cursor {
			line = styleSelected.Render(line)
		}
		sb.WriteString(line + "\n")
	}

	for i := end - start; i < listHeight; i++ {
		sb.WriteString("\n")
	}

	sb.WriteString(styleSep.Render(strings.Repeat("─", m.width)) + "\n")

	var totalCount, totalSize int64
	var totalCost float64
	for _, e := range m.entries {
		if e.name != ".." {
			totalCount += e.count
			totalSize += e.size
			totalCost += e.monthlyCost
		}
	}
	listCost := computeCost(m.listRequests, m.region)
	footer1 := fmt.Sprintf(" Total: %s  %d objects  Monthly: %s",
		humanSize(totalSize), totalCount, styleCost.Render(fmt.Sprintf("$%.4f", totalCost)))
	footer2 := styleFooter.Render(fmt.Sprintf(" LIST run: %d requests  $%.6f  [↑↓/jk] move  [PgUp/Dn] page  [↵/→/l] enter  [←/h/bksp] back  [q]uit  [?]help",
		m.listRequests, listCost))
	sb.WriteString(footer1 + "\n")
	sb.WriteString(footer2)

	return sb.String()
}

func (m tuiModel) helpView() string {
	help := []string{
		"",
		"  s3du — keyboard shortcuts",
		"",
		"  ↑ / k            move up",
		"  ↓ / j            move down",
		"  PgUp / Ctrl+B    page up",
		"  PgDn / Ctrl+F    page down",
		"  Enter / → / l    enter directory",
		"  Backspace / ← / h  go up",
		"  q / Ctrl-C       quit",
		"  ?                toggle this help",
		"",
		"  Press any key to close",
	}
	return strings.Join(help, "\n")
}

func lastSegment(prefix string) string {
	trimmed := strings.TrimSuffix(prefix, "/")
	idx := strings.LastIndex(trimmed, "/")
	if idx < 0 {
		return prefix
	}
	return trimmed[idx+1:] + "/"
}

func runTUI(bucket, region string, listRequests int64, treeIndex map[string]DirSection, objIndex map[string]ObjectSection, objFile *os.File) error {
	m := newTUIModel(bucket, region, listRequests, treeIndex, objIndex, objFile)
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}
