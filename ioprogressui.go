package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/mattn/go-isatty"
)

// ioProgressModel is the bubbletea-style "we are streaming bytes" dashboard
// used for snapshot load (where total is known) and save (where total is
// 0 — we report rate and elapsed only).
type ioProgressModel struct {
	label    string
	counter  *atomic.Int64
	total    int64 // 0 = unknown
	start    time.Time
	interval time.Duration
	bar      progress.Model
	spinner  spinner.Model
	width    int
	prev     int64
	prevAt   time.Time
	rate     float64 // EWMA bytes/sec
}

func newIOProgressModel(label string, counter *atomic.Int64, total int64, interval time.Duration) ioProgressModel {
	sp := spinner.New()
	sp.Spinner = spinner.Dot
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#6e6e6e", Dark: "#a9a9a9"})

	bar := progress.New(progress.WithDefaultGradient(), progress.WithoutPercentage())
	bar.Width = 40

	now := time.Now()
	return ioProgressModel{
		label:    label,
		counter:  counter,
		total:    total,
		start:    now,
		interval: interval,
		bar:      bar,
		spinner:  sp,
		prevAt:   now,
	}
}

type ioTickMsg time.Time

func (m ioProgressModel) tickCmd() tea.Cmd {
	return tea.Tick(m.interval, func(t time.Time) tea.Msg { return ioTickMsg(t) })
}

func (m ioProgressModel) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, m.tickCmd())
}

func (m ioProgressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		// Leave space for labels and counters: ~50 chars off the side.
		w := msg.Width - 50
		w = min(max(w, 20), 80)
		m.bar.Width = w
		return m, nil
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case ioTickMsg:
		cur := m.counter.Load()
		now := time.Time(msg)
		if dt := now.Sub(m.prevAt); dt > 0 {
			inst := float64(cur-m.prev) / dt.Seconds()
			switch {
			case m.rate == 0:
				m.rate = inst
			default:
				// Light EWMA so the ETA isn't jumpy on uneven I/O.
				m.rate = m.rate*0.7 + inst*0.3
			}
		}
		m.prev = cur
		m.prevAt = now
		return m, m.tickCmd()
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			return m, tea.Quit
		}
	}
	return m, nil
}

func (m ioProgressModel) View() string {
	cur := m.counter.Load()
	elapsed := time.Since(m.start).Round(time.Second)

	var ratio float64
	if m.total > 0 {
		ratio = float64(cur) / float64(m.total)
		ratio = min(ratio, 1)
	}

	pctStr := "—"
	etaStr := "—"
	if m.total > 0 {
		pctStr = fmt.Sprintf("%5.1f%%", ratio*100)
		if m.rate > 0 && cur < m.total {
			remaining := float64(m.total-cur) / m.rate
			etaStr = time.Duration(remaining * float64(time.Second)).Round(time.Second).String()
		}
	}

	header := m.spinner.View() + "  " + progHeaderStyle.Render(m.label)
	bar := ""
	if m.total > 0 {
		bar = m.bar.ViewAs(ratio) + "  " + progValueStyle.Render(pctStr) + "\n"
	}

	total := "—"
	if m.total > 0 {
		total = humanBytes(m.total)
	}
	row := func(k, v string) string {
		return progLabelStyle.Render(k) + progValueStyle.Render(v)
	}
	body := header + "\n\n" + bar +
		row("bytes", fmt.Sprintf("%s / %s", humanBytes(cur), total)) + "\n" +
		row("rate", humanRate(m.rate)) + "\n" +
		row("elapsed", elapsed.String()) + "\n" +
		row("ETA", etaStr)
	return progBoxStyle.Render(body)
}

// runIOProgress executes work in a goroutine while a bubbletea dashboard
// tracks counter (and total, when non-zero) on stderr. Falls back to the
// plain "\r-line" reporter when stderr is not a TTY so log redirection
// doesn't get an ANSI rendering. Returns whatever work returned.
func runIOProgress(label string, counter *atomic.Int64, total int64, interval time.Duration, work func() error) error {
	if !isatty.IsTerminal(os.Stderr.Fd()) {
		done := make(chan struct{})
		errCh := make(chan error, 1)
		go func() { errCh <- work(); close(done) }()
		reportIO(os.Stderr, label, counter, done, interval)
		return <-errCh
	}

	model := newIOProgressModel(label, counter, total, interval)
	prog := tea.NewProgram(model, tea.WithOutput(os.Stderr))

	errCh := make(chan error, 1)
	go func() {
		errCh <- work()
		prog.Quit()
	}()
	_, _ = prog.Run()
	return <-errCh
}
