package progress

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

// RunScanUI renders a live scanner dashboard to stderr until done is closed.
// The bubbletea program runs inline (no alt-screen) and clears its rendered
// area on quit so subsequent stderr writes flow normally below.
//
// A side goroutine keeps the EWMA fresh on its own cadence (sampleInterval)
// independent of the UI tick rate so a -progress-ms tweak doesn't skew the
// effective-parallelism math.
//
// On non-TTY stderr (log redirection) the function falls back to a plain
// printf-per-tick loop — bubbletea TUI output would otherwise turn into
// ANSI noise inside the log file.
func RunScanUI(p *Progress, done <-chan struct{}, sampleInterval, uiInterval time.Duration) {
	stopSampler := StartSampler(p, sampleInterval)
	defer stopSampler()

	if !isatty.IsTerminal(os.Stderr.Fd()) {
		RunPlainReporter(os.Stderr, p, done, uiInterval)
		return
	}

	tp := tea.NewProgram(newScanModel(p, uiInterval), tea.WithOutput(os.Stderr))
	go func() {
		<-done
		tp.Quit()
	}()
	if _, err := tp.Run(); err != nil {
		slog.Warn("progress UI exited with error", "err", err)
	}
}

// StartSampler runs Progress.SampleInflight on a fixed cadence and returns
// a stopper that joins the sampler goroutine.
func StartSampler(p *Progress, interval time.Duration) func() {
	stop := make(chan struct{})
	doneAck := make(chan struct{})
	go func() {
		defer close(doneAck)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				p.SampleInflight()
				return
			case <-t.C:
				p.SampleInflight()
			}
		}
	}()
	return func() {
		close(stop)
		<-doneAck
	}
}

// RunPlainReporter is the redirected-output fallback: one line per tick,
// terminated with \r so a TTY still updates in place while a log file gets
// each line separately.
func RunPlainReporter(w io.Writer, p *Progress, done <-chan struct{}, interval time.Duration) {
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

func printPlain(w io.Writer, s Snapshot, final bool) {
	end := "\r"
	if final {
		end = "\n"
	}
	fmt.Fprintf(w,
		"lists=%-7d (%-8s) inflight=%2d/%-2d (%3d%%) eff=%5.1f/%-2d (%3d%%) queue=%-5d obj/list=%-6.1f objects=%-9d (%-9s) bytes=%-9s list$=%-7s storage$/mo=%-7s%s",
		s.ListRequests, HumanRate(s.RequestsPerSec),
		s.Inflight, s.MaxWorkers, Percent(float64(s.Inflight), s.MaxWorkers),
		s.InflightEWMA, s.MaxWorkers, Percent(s.InflightEWMA, s.MaxWorkers),
		s.QueueDepth,
		s.ObjectsPerList,
		s.ObjectsSeen, HumanRate(s.ObjectsPerSec),
		HumanBytes(s.TotalBytes()),
		HumanDollars(s.ListCost()),
		HumanDollars(s.MonthlyStorageCost()),
		end,
	)
}

type scanModel struct {
	p           *Progress
	width       int
	uiInterval  time.Duration
	spinner     spinner.Model
	inflightBar progress.Model
	effBar      progress.Model
}

func newScanModel(p *Progress, uiInterval time.Duration) scanModel {
	sp := spinner.New()
	sp.Spinner = spinner.Dot
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#6e6e6e", Dark: "#a9a9a9"})

	bar := func() progress.Model {
		b := progress.New(progress.WithDefaultGradient(), progress.WithoutPercentage())
		b.Width = 40
		return b
	}
	return scanModel{
		p:           p,
		uiInterval:  uiInterval,
		spinner:     sp,
		inflightBar: bar(),
		effBar:      bar(),
	}
}

type tickMsg time.Time

func (m scanModel) tickCmd() tea.Cmd {
	return tea.Tick(m.uiInterval, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m scanModel) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, m.tickCmd())
}

func (m scanModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case tickMsg:
		return m, m.tickCmd()
	case tea.WindowSizeMsg:
		m.width = msg.Width
		w := max(msg.Width-32, 16)
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
	labelStyle  = lipgloss.NewStyle().Faint(true).Width(10)
	valueStyle  = lipgloss.NewStyle().Bold(true)
	boxStyle    = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1)
	headerStyle = lipgloss.NewStyle().Bold(true)
)

func (m scanModel) View() string {
	s := m.p.Snapshot()
	elapsed := formatElapsed(s.Elapsed)
	header := headerStyle.Render(fmt.Sprintf("s3du · %s · %s", s.Region, elapsed))

	row := func(label, value string) string {
		return labelStyle.Render(label) + valueStyle.Render(value)
	}

	maxWorkers := max(s.MaxWorkers, 1)
	inflightRatio := clamp01(float64(s.Inflight) / float64(maxWorkers))
	effRatio := clamp01(s.InflightEWMA / float64(maxWorkers))

	body := lipgloss.JoinVertical(lipgloss.Left,
		m.spinner.View()+"  scanning",
		"",
		header,
		"",
		row("lists", fmt.Sprintf("%d  (%s)", s.ListRequests, HumanRate(s.RequestsPerSec))),
		row("queue", fmt.Sprintf("%d", s.QueueDepth)),
		row("obj/list", fmt.Sprintf("%.1f", s.ObjectsPerList)),
		row("objects", fmt.Sprintf("%d  (%s)", s.ObjectsSeen, HumanRate(s.ObjectsPerSec))),
		row("bytes", HumanBytes(s.TotalBytes())),
		row("list$", HumanDollars(s.ListCost())),
		row("storage$", HumanDollars(s.MonthlyStorageCost())+" / mo"),
		"",
		fmt.Sprintf("%s%s  %d/%d (%d%%)",
			labelStyle.Render("inflight"),
			m.inflightBar.ViewAs(inflightRatio),
			s.Inflight, s.MaxWorkers, Percent(float64(s.Inflight), s.MaxWorkers)),
		fmt.Sprintf("%s%s  %.1f/%d (%d%%)",
			labelStyle.Render("eff 30s"),
			m.effBar.ViewAs(effRatio),
			s.InflightEWMA, s.MaxWorkers, Percent(s.InflightEWMA, s.MaxWorkers)),
	)
	return boxStyle.Render(body)
}

func formatElapsed(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	total := int(d.Seconds())
	return fmt.Sprintf("%02d:%02d", total/60, total%60)
}

func clamp01(x float64) float64 {
	switch {
	case x < 0:
		return 0
	case x > 1:
		return 1
	}
	return x
}
