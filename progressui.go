package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/mattn/go-isatty"
)

// runProgressUI renders a live scanner dashboard to stderr until done is
// closed. The bubbletea program runs inline (no alt-screen) and clears its
// rendered area on quit, so subsequent stderr writes flow normally below.
//
// A side goroutine keeps the EWMA fresh on its own cadence (sampleInterval)
// independent of the UI tick rate, so a -progress-ms tweak does not skew the
// effective-parallelism math.
//
// On non-TTY stderr (e.g. log redirection) the function falls back to a
// plain printf-per-tick loop — bubbletea's TUI output would otherwise turn
// into ANSI noise inside the log file.
func runProgressUI(p *Progress, done <-chan struct{}, sampleInterval, uiInterval time.Duration) {
	stopSampler := startSampler(p, sampleInterval)
	defer stopSampler()

	if !isatty.IsTerminal(os.Stderr.Fd()) {
		runPlainReporter(os.Stderr, p, done, uiInterval)
		return
	}

	tp := tea.NewProgram(newProgressModel(p, uiInterval), tea.WithOutput(os.Stderr))
	go func() {
		<-done
		tp.Quit()
	}()
	if _, err := tp.Run(); err != nil {
		slog.Warn("progress UI exited with error", "err", err)
	}
}

// startSampler runs Progress.sampleInflight on a fixed cadence and returns
// a stopper that joins the goroutine.
func startSampler(p *Progress, interval time.Duration) func() {
	stop := make(chan struct{})
	doneAck := make(chan struct{})
	go func() {
		defer close(doneAck)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				p.sampleInflight()
				return
			case <-t.C:
				p.sampleInflight()
			}
		}
	}()
	return func() {
		close(stop)
		<-doneAck
	}
}

// runPlainReporter is the redirected-output fallback: one line per tick,
// terminated with \r so a TTY still updates in place while a log file gets
// each line separately. Kept simple to stay readable when grep'd later.
func runPlainReporter(w io.Writer, p *Progress, done <-chan struct{}, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-done:
			printPlain(w, p.Snapshot(), true)
			return
		case <-tick.C:
			printPlain(w, p.Snapshot(), false)
		}
	}
}

func printPlain(w io.Writer, s ProgressSnapshot, final bool) {
	end := "\r"
	if final {
		end = "\n"
	}
	fmt.Fprintf(w,
		"lists=%-7d (%-8s) inflight=%2d/%-2d (%3d%%) eff=%5.1f/%-2d (%3d%%) queue=%-5d obj/list=%-6.1f objects=%-9d (%-9s) bytes=%-9s list$=%-7s storage$/mo=%-7s%s",
		s.ListRequests, humanRate(s.RequestsPerSec),
		s.Inflight, s.MaxWorkers, percent(float64(s.Inflight), s.MaxWorkers),
		s.InflightEWMA, s.MaxWorkers, percent(s.InflightEWMA, s.MaxWorkers),
		s.QueueDepth,
		s.ObjectsPerList,
		s.ObjectsSeen, humanRate(s.ObjectsPerSec),
		humanBytes(s.TotalBytes()),
		humanDollars(s.ListCost()),
		humanDollars(s.MonthlyStorageCost()),
		end,
	)
}

// progressModel is the bubbletea model that renders the live dashboard.
type progressModel struct {
	p           *Progress
	width       int
	uiInterval  time.Duration
	spinner     spinner.Model
	inflightBar progress.Model
	effBar      progress.Model
}

func newProgressModel(p *Progress, uiInterval time.Duration) progressModel {
	sp := spinner.New()
	sp.Spinner = spinner.Dot
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#6e6e6e", Dark: "#a9a9a9"})

	bar := func() progress.Model {
		b := progress.New(progress.WithDefaultGradient(), progress.WithoutPercentage())
		b.Width = 40
		return b
	}
	return progressModel{
		p:           p,
		uiInterval:  uiInterval,
		spinner:     sp,
		inflightBar: bar(),
		effBar:      bar(),
	}
}

// tickMsg drives the View refresh independent of spinner.
type tickMsg time.Time

func (m progressModel) tickCmd() tea.Cmd {
	return tea.Tick(m.uiInterval, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m progressModel) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, m.tickCmd())
}

func (m progressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case tickMsg:
		return m, m.tickCmd()
	case tea.WindowSizeMsg:
		m.width = msg.Width
		// Reserve label + counters + padding; the rest is bar width.
		w := msg.Width - 32
		if w < 16 {
			w = 16
		}
		m.inflightBar.Width = w
		m.effBar.Width = w
		return m, nil
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			return m, tea.Quit
		}
	}
	return m, nil
}

var (
	progLabelStyle = lipgloss.NewStyle().Faint(true).Width(10)
	progValueStyle = lipgloss.NewStyle().Bold(true)
	progBoxStyle   = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			Padding(0, 1)
	progHeaderStyle = lipgloss.NewStyle().Bold(true)
)

func (m progressModel) View() string {
	s := m.p.Snapshot()
	elapsed := formatElapsed(s.Elapsed)
	header := progHeaderStyle.Render(fmt.Sprintf("s3du · %s · %s", s.Region, elapsed))

	row := func(label, value string) string {
		return progLabelStyle.Render(label) + progValueStyle.Render(value)
	}

	maxWorkers := s.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = 1
	}
	inflightRatio := clamp01(float64(s.Inflight) / float64(maxWorkers))
	effRatio := clamp01(s.InflightEWMA / float64(maxWorkers))

	body := lipgloss.JoinVertical(lipgloss.Left,
		m.spinner.View()+"  scanning",
		"",
		header,
		"",
		row("lists", fmt.Sprintf("%d  (%s)", s.ListRequests, humanRate(s.RequestsPerSec))),
		row("queue", fmt.Sprintf("%d", s.QueueDepth)),
		row("obj/list", fmt.Sprintf("%.1f", s.ObjectsPerList)),
		row("objects", fmt.Sprintf("%d  (%s)", s.ObjectsSeen, humanRate(s.ObjectsPerSec))),
		row("bytes", humanBytes(s.TotalBytes())),
		row("list$", humanDollars(s.ListCost())),
		row("storage$", humanDollars(s.MonthlyStorageCost())+" / mo"),
		"",
		fmt.Sprintf("%s%s  %d/%d (%d%%)",
			progLabelStyle.Render("inflight"),
			m.inflightBar.ViewAs(inflightRatio),
			s.Inflight, s.MaxWorkers, percent(float64(s.Inflight), s.MaxWorkers)),
		fmt.Sprintf("%s%s  %.1f/%d (%d%%)",
			progLabelStyle.Render("eff 30s"),
			m.effBar.ViewAs(effRatio),
			s.InflightEWMA, s.MaxWorkers, percent(s.InflightEWMA, s.MaxWorkers)),
	)
	return progBoxStyle.Render(body)
}

// formatElapsed renders a duration as mm:ss for the panel header.
func formatElapsed(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	total := int(d.Seconds())
	return fmt.Sprintf("%02d:%02d", total/60, total%60)
}

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}
