package progress

import (
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/mattn/go-isatty"
)

// ioModel is the bubbletea "we are streaming bytes" dashboard used for
// snapshot load (where total is known) and save (where total is 0 — we
// report rate and elapsed only).
type ioModel struct {
	label    string
	counter  *atomic.Int64
	total    int64
	start    time.Time
	interval time.Duration
	bar      progress.Model
	spinner  spinner.Model
	width    int
	prev     int64
	prevAt   time.Time
	rate     float64
}

func newIOModel(label string, counter *atomic.Int64, total int64, interval time.Duration) ioModel {
	sp := spinner.New()
	sp.Spinner = spinner.Dot
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#6e6e6e", Dark: "#a9a9a9"})

	bar := progress.New(progress.WithDefaultGradient(), progress.WithoutPercentage())
	bar.Width = 40

	now := time.Now()
	return ioModel{
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

func (m ioModel) tickCmd() tea.Cmd {
	return tea.Tick(m.interval, func(t time.Time) tea.Msg { return ioTickMsg(t) })
}

func (m ioModel) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, m.tickCmd())
}

func (m ioModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		w := min(max(msg.Width-50, 20), 80)
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

func (m ioModel) View() string {
	cur := m.counter.Load()
	elapsed := time.Since(m.start).Round(time.Second)

	var ratio float64
	if m.total > 0 {
		ratio = min(float64(cur)/float64(m.total), 1)
	}

	pctStr, etaStr := "—", "—"
	if m.total > 0 {
		pctStr = fmt.Sprintf("%5.1f%%", ratio*100)
		if m.rate > 0 && cur < m.total {
			remaining := float64(m.total-cur) / m.rate
			etaStr = time.Duration(remaining * float64(time.Second)).Round(time.Second).String()
		}
	}

	header := m.spinner.View() + "  " + headerStyle.Render(m.label)
	bar := ""
	if m.total > 0 {
		bar = m.bar.ViewAs(ratio) + "  " + valueStyle.Render(pctStr) + "\n"
	}

	total := "—"
	if m.total > 0 {
		total = HumanBytes(m.total)
	}
	row := func(k, v string) string {
		return labelStyle.Render(k) + valueStyle.Render(v)
	}
	body := header + "\n\n" + bar +
		row("bytes", fmt.Sprintf("%s / %s", HumanBytes(cur), total)) + "\n" +
		row("rate", HumanRate(m.rate)) + "\n" +
		row("elapsed", elapsed.String()) + "\n" +
		row("ETA", etaStr)
	return boxStyle.Render(body)
}

// RunIO executes work in a goroutine while a bubbletea dashboard tracks
// counter (and total, when non-zero) on stderr. Falls back to ReportIO's
// plain "\r-line" reporter when stderr is not a TTY so log redirection
// doesn't get an ANSI rendering. Returns whatever work returned.
func RunIO(label string, counter *atomic.Int64, total int64, interval time.Duration, work func() error) error {
	if !isatty.IsTerminal(os.Stderr.Fd()) {
		done := make(chan struct{})
		errCh := make(chan error, 1)
		go func() { errCh <- work(); close(done) }()
		ReportIO(os.Stderr, label, counter, done, interval)
		return <-errCh
	}

	model := newIOModel(label, counter, total, interval)
	prog := tea.NewProgram(model, tea.WithOutput(os.Stderr))

	errCh := make(chan error, 1)
	go func() {
		errCh <- work()
		prog.Quit()
	}()
	_, _ = prog.Run()
	return <-errCh
}

// ReportIO ticks every interval and prints a one-line "\r"-anchored
// progress indicator with the cumulative bytes through the counter and
// the rate observed between consecutive ticks. On done close it prints
// the final summary and returns. Used as the non-TTY fallback by RunIO.
func ReportIO(w io.Writer, label string, counter *atomic.Int64, done <-chan struct{}, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	start := time.Now()
	var prev int64
	for {
		select {
		case <-done:
			cur := counter.Load()
			elapsed := time.Since(start)
			rate := 0.0
			if elapsed > 0 {
				rate = float64(cur) / elapsed.Seconds()
			}
			fmt.Fprintf(w, "\r%s: %s in %s (avg %s)\033[K\n",
				label, HumanBytes(cur), elapsed.Round(time.Millisecond), HumanRate(rate))
			return
		case <-tick.C:
			cur := counter.Load()
			delta := cur - prev
			prev = cur
			rate := float64(delta) / interval.Seconds()
			fmt.Fprintf(w, "\r%s: %s  %s\033[K",
				label, HumanBytes(cur), HumanRate(rate))
		}
	}
}
